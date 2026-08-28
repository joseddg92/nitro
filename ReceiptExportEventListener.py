"""EventListener fed by a patched nitro node's shared-memory receipt exporter
(`execution.receipt-export`) — the fastest source available, because the node publishes a block's
receipts the moment it commits them, with no JSON-RPC in the path at all.

How it differs from the other listeners:
  * no subscription, no request/response, no JSON. The node writes a compact binary record per block
    into a shared-memory ring and bumps an eventfd;
  * we mmap that ring and let the HOT LOOP watch the eventfd with ``add_reader``. asyncio wakes on
    the fd directly, so a block is decoded on the loop thread microseconds after it is committed —
    no reader thread, no polling, and no hand-off before the buy path (see utils.hot_loop).

Wire format (must match execution/gethexec/receiptexporter/protocol.go), little-endian:

  Shared-memory header (4096-byte page), then the data ring:
    off 0   u64 magic "NITRORB1" | off 8  u32 version(2) | off 12 u32 header_size(4096)
    off 16  u64 capacity (power of two) | off 24 u64 dropped | off 32 u64 msg_count
    off 64  u64 head (consumer cursor — ours to advance) | off 128 u64 tail (producer cursor)
    off 4096    data[capacity]

  Records: [u32 payload_len][payload]. head/tail are monotonic; index = cursor % capacity, so a
  record may wrap. The producer advances `tail` only after the whole record is written, so an
  observed tail means the record is complete.

  Payload = one block:
    Block:   block_number u64, block_hash[32], parent_hash[32], timestamp u64,
             tx_count u32, receipt_count u32, receipts[]
    Receipt: tx_hash[32], status u8, tx_type u8, contract_address[20], cumulative_gas_used u64,
             gas_used u64, gas_used_for_l1 u64, effective_gas_price[32] (big-endian), log_count u32, logs[]
    Log:     address[20], topic_count u8, topics[topic_count][32], data_len u32, data[data_len]

  Handshake on the socket: magic(8) version(4) header_size(4) capacity(8), with two SCM_RIGHTS fds
  in order: [0] shared memory, [1] eventfd.

Two fields the binary format does NOT carry and that we therefore derive, exactly as the chain
numbers them: `transactionIndex` is the receipt's position in the block, and `logIndex` is a running
count over ALL logs of the block (not per-transaction). That makes these events dedup correctly in
EventListenerAgregator against the same log arriving over WSS/IPC.
"""
import array
import mmap
import os
import socket
import struct
import time
from typing import Dict, List, Optional, Set, Tuple

from EventListeners.EventListener import EventListener
from EventListeners.EventListenerRegistration import EventListenerRegistration
from loggers import APP_LOG
from utils.TimedRecords import TIME_TAG_WS_RECEIVED
from utils.hot_loop import HOT_LOOP, on_hot_loop, spawn

MAGIC = b"NITRORB1"
VERSION = 2
HEADER_SIZE = 4096

OFF_MAGIC = 0
OFF_VERSION = 8
OFF_CAPACITY = 16
OFF_DROPPED = 24
OFF_MSG_COUNT = 32
OFF_HEAD = 64
OFF_TAIL = 128

LEN_PREFIX = 4
HANDSHAKE_SIZE = 24

_CONNECT_TIMEOUT_SECS = 5.0
_RECONNECT_MIN_SECS = 0.5
_RECONNECT_MAX_SECS = 10.0
_WARN_EVERY_SECS = 30.0

_U32 = struct.Struct("<I")
_U64 = struct.Struct("<Q")



def _as_topic_str(topic) -> str:
    """Normalise a topic (str or HexBytes) to a lowercase 0x-hex string."""
    if isinstance(topic, (bytes, bytearray)) and hasattr(topic, "to_0x_hex"):
        return topic.to_0x_hex().lower()
    return str(topic).lower()


class ReceiptExportEventListener(EventListener):
    def __init__(self, socket_path: str, provider_name: str = "receipt-shm"):
        self.socket_path = socket_path
        self.provider_name = provider_name

        self._registrations: List[EventListenerRegistration] = []
        # (address.lower(), topic0.lower()) -> registrations. Loop-owned: every mutation is
        # marshalled onto the hot loop, so the decode path needs no lock at all.
        self._index: Dict[Tuple[str, str], List[EventListenerRegistration]] = {}
        self._topics: Set[str] = set()

        self._active = True
        self._sock: Optional[socket.socket] = None
        self._mm: Optional[mmap.mmap] = None
        self._event_fd: Optional[int] = None
        self._capacity = 0
        self._head = 0
        self._last_warn = 0.0
        self._blocks_seen = 0
        self._events_delivered = 0
        self._last_dropped = 0

        spawn(self._run(), name=f"receipt-shm[{provider_name}]")

    # --- helpers -------------------------------------------------------------------------------

    def _call_on_loop(self, fn, *args) -> None:
        if on_hot_loop():
            fn(*args)
        else:
            HOT_LOOP.call_soon_threadsafe(fn, *args)

    def name(self) -> str:
        # No socket path: there is only ever one of these per engine, so the path identified
        # nothing and just made every line it prefixes longer.
        return "ReceiptShm[]"

    def short_name(self) -> str:
        return self.provider_name

    def __str__(self) -> str:
        return self.name()

    __repr__ = __str__

    # --- EventListener API ---------------------------------------------------------------------

    def start_listening(self, event, callback, log_processor=None) -> EventListenerRegistration:
        registration = EventListenerRegistration(self, event, callback, log_processor)
        self._call_on_loop(self._register, registration)
        return registration

    def _register(self, registration: EventListenerRegistration) -> None:
        self._registrations.append(registration)
        key = self._key_for(registration.event)
        if key is not None:
            self._index.setdefault(key, []).append(registration)
            self._topics.add(key[1])

    def stop_listening(self, registration: EventListenerRegistration) -> None:
        self._call_on_loop(self._unregister, registration)

    def _unregister(self, registration: EventListenerRegistration) -> None:
        try:
            self._registrations.remove(registration)
        except ValueError:
            APP_LOG.error(f"{self} stop_listening: registration not listed: {registration}")
        key = self._key_for(registration.event)
        regs = self._index.get(key) if key is not None else None
        if regs:
            try:
                regs.remove(registration)
            except ValueError:
                pass
            if not regs:
                self._index.pop(key, None)

    @staticmethod
    def _key_for(event) -> Optional[Tuple[str, str]]:
        try:
            return (str(event.address).lower(), _as_topic_str(event.topic))
        except Exception:
            APP_LOG.exception(f"ReceiptExportEventListener: event has no address/topic: {event}")
            return None

    def stop(self) -> None:
        self._active = False
        self._call_on_loop(self._teardown)

    # --- connection ----------------------------------------------------------------------------

    @staticmethod
    def _recv_handshake_and_fds(sock) -> Tuple[bytes, int, int]:
        """Receive the 24-byte handshake plus the two SCM_RIGHTS fds ([0] shm, [1] eventfd)."""
        fds = array.array("i")
        anc_size = socket.CMSG_SPACE(2 * fds.itemsize)
        data = b""
        got_fds: List[int] = []
        while len(data) < HANDSHAKE_SIZE:
            chunk, ancdata, _flags, _addr = sock.recvmsg(HANDSHAKE_SIZE - len(data), anc_size)
            if not chunk and not ancdata:
                raise EOFError("exporter closed the connection during handshake")
            data += chunk
            for cmsg_level, cmsg_type, cmsg_data in ancdata:
                if cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS:
                    trunc = len(cmsg_data) - (len(cmsg_data) % fds.itemsize)
                    fa = array.array("i")
                    fa.frombytes(cmsg_data[:trunc])
                    got_fds.extend(fa.tolist())
        if len(got_fds) < 2:
            for fd in got_fds:
                os.close(fd)
            raise RuntimeError(f"expected 2 fds from exporter, got {len(got_fds)}")
        return data, got_fds[0], got_fds[1]

    @staticmethod
    def _parse_handshake(data: bytes) -> int:
        if data[OFF_MAGIC:OFF_MAGIC + 8] != MAGIC:
            raise RuntimeError(f"bad handshake magic {data[:8]!r}, expected {MAGIC!r}")
        version, header_size = struct.unpack_from("<II", data, OFF_VERSION)
        (capacity,) = _U64.unpack_from(data, OFF_CAPACITY)
        if version != VERSION:
            raise RuntimeError(f"unsupported protocol version {version} (need {VERSION})")
        if header_size != HEADER_SIZE:
            raise RuntimeError(f"unexpected header size {header_size}")
        if capacity == 0 or (capacity & (capacity - 1)) != 0:
            raise RuntimeError(f"capacity {capacity} is not a power of two")
        return capacity

    def _connect_blocking(self):
        """Socket connect + handshake + mmap. Blocking, so it runs in a worker thread."""
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(_CONNECT_TIMEOUT_SECS)
        sock.connect(self.socket_path)
        sock.settimeout(None)
        handshake, shm_fd, event_fd = self._recv_handshake_and_fds(sock)
        try:
            capacity = self._parse_handshake(handshake)
            mm = mmap.mmap(shm_fd, HEADER_SIZE + capacity,
                           prot=mmap.PROT_READ | mmap.PROT_WRITE, flags=mmap.MAP_SHARED)
        except Exception:
            os.close(shm_fd)
            os.close(event_fd)
            sock.close()
            raise
        os.close(shm_fd)          # the mapping keeps the segment alive
        if mm[OFF_MAGIC:OFF_MAGIC + 8] != MAGIC:
            mm.close()
            os.close(event_fd)
            sock.close()
            raise RuntimeError("shared-memory magic mismatch")
        return sock, mm, event_fd, capacity

    async def _run(self) -> None:
        if not hasattr(socket, "AF_UNIX"):
            APP_LOG.error(f"{self} AF_UNIX not supported on this platform; receipt exporter disabled")
            return
        import asyncio
        backoff = _RECONNECT_MIN_SECS
        while self._active:
            try:
                sock, mm, event_fd, capacity = await asyncio.to_thread(self._connect_blocking)
                self._sock, self._mm, self._event_fd, self._capacity = sock, mm, event_fd, capacity
                # Start at the PRODUCER's cursor, skipping whatever is already in the ring. The
                # stored head is the previous consumer's, which after a period with nobody attached
                # can be a whole ring (tens of MiB) behind — draining that would spend the hot loop
                # decoding blocks from before we started, which can never be sniped anyway.
                stale_head = _U64.unpack_from(mm, OFF_HEAD)[0]
                self._head = _U64.unpack_from(mm, OFF_TAIL)[0]
                _U64.pack_into(mm, OFF_HEAD, self._head)
                # The drop counter is producer-lifetime, so baseline it here: everything it counted
                # before we attached happened while nothing was consuming (expected, not our
                # problem). Only growth from now on means WE are too slow.
                self._last_dropped = _U64.unpack_from(mm, OFF_DROPPED)[0]
                # Created BEFORE either reader is armed. Both can fire the moment they are
                # installed — a node that died between connect and here makes the socket readable
                # immediately — and _signal_closed would then resolve the PREVIOUS iteration's
                # future (already done) while this one goes on to await a fresh one nothing will
                # ever resolve. That is the same hang this commit is fixing, one lap later.
                self._closed = HOT_LOOP.create_future()
                os.set_blocking(event_fd, False)
                # THE point of this listener: asyncio watches the eventfd itself, so a committed
                # block reaches _on_signal on the hot loop with no thread and no polling.
                HOT_LOOP.add_reader(event_fd, self._on_signal)
                # And the socket, which is the ONLY thing that reports the exporter dying. An
                # eventfd has no EOF: when the node restarts, our copy stays a perfectly valid fd
                # that nobody will ever write to again, so _on_signal is simply never called and
                # the await below would block forever. The socket carries nothing after the
                # handshake — its only remaining job is to become readable when the peer is gone.
                sock.setblocking(False)
                HOT_LOOP.add_reader(sock.fileno(), self._on_socket_event)
                APP_LOG.info(
                    f"{self} connected: ring={capacity // 1024} KiB, starting at head={self._head} "
                    f"(skipped {self._head - stale_head} bytes of pre-existing backlog; "
                    f"producer dropped {self._last_dropped} before we attached)")
                backoff = _RECONNECT_MIN_SECS
                await self._closed
                if self._active:
                    APP_LOG.warning(f"{self} receipt exporter connection lost; reconnecting")
            except asyncio.CancelledError:
                raise
            except (OSError, EOFError, RuntimeError) as e:
                now = time.time()
                if now - self._last_warn > _WARN_EVERY_SECS:
                    APP_LOG.warning(f"{self} not connected to {self.socket_path} ({e}); retrying")
                    self._last_warn = now
            except Exception:
                APP_LOG.exception(f"{self} receipt exporter loop error; reconnecting")
            finally:
                self._teardown()

            if not self._active:
                break
            await asyncio.sleep(backoff)
            backoff = min(_RECONNECT_MAX_SECS, backoff * 2)

    def _teardown(self) -> None:
        if self._event_fd is not None:
            try:
                HOT_LOOP.remove_reader(self._event_fd)
            except Exception:
                pass
            try:
                os.close(self._event_fd)
            except Exception:
                pass
            self._event_fd = None
        if self._mm is not None:
            try:
                self._mm.close()
            except Exception:
                pass
            self._mm = None
        if self._sock is not None:
            try:
                # Before close(): the fd is the reader's key, and closing first would leave the
                # loop selecting on a number that a later connect can be handed straight back.
                HOT_LOOP.remove_reader(self._sock.fileno())
            except Exception:
                pass
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    def _on_socket_event(self) -> None:
        """The exporter socket became readable — which, post-handshake, means it is gone.

        This is the node-restart path. The exporter sends nothing on this socket once the fds are
        handed over, so readable means EOF (peer closed, or the process died and the kernel closed
        it for it). Anything that did arrive is drained and ignored: only the close matters."""
        sock = self._sock
        if sock is None:
            return
        try:
            data = sock.recv(4096)
        except BlockingIOError:
            return                                # spurious wakeup, nothing to read yet
        except OSError:
            self._signal_closed()                 # reset counts as gone, same as EOF
            return
        if not data:
            self._signal_closed()

    def _signal_closed(self) -> None:
        closed = getattr(self, "_closed", None)
        if closed is not None and not closed.done():
            closed.set_result(None)

    # --- read path (hot loop) -------------------------------------------------------------------

    def _on_signal(self) -> None:
        """The eventfd fired: the producer published at least one block."""
        received_time = time.time()
        try:
            buf = os.read(self._event_fd, 8)     # clears the counter
            if len(buf) != 8:
                self._signal_closed()
                return
        except BlockingIOError:
            return                                # spurious wakeup
        except OSError:
            self._signal_closed()
            return
        try:
            self._drain(received_time)
        except Exception:
            APP_LOG.exception(f"{self} draining receipt ring")

    def _read_wrapped(self, pos: int, n: int) -> bytes:
        """Read n bytes at monotonic cursor pos, handling the ring wrap."""
        mm, capacity = self._mm, self._capacity
        off = pos % capacity
        if off + n <= capacity:
            return mm[HEADER_SIZE + off:HEADER_SIZE + off + n]
        first = capacity - off
        return mm[HEADER_SIZE + off:HEADER_SIZE + capacity] + mm[HEADER_SIZE:HEADER_SIZE + (n - first)]

    def _drain(self, received_time: float) -> None:
        mm = self._mm
        head = self._head
        dispatched = 0
        while True:
            tail = _U64.unpack_from(mm, OFF_TAIL)[0]   # acquire; ordered after the eventfd read
            avail = tail - head
            if avail < LEN_PREFIX:
                break
            (plen,) = _U32.unpack(self._read_wrapped(head, LEN_PREFIX))
            if avail < LEN_PREFIX + plen:
                break                                  # producer mid-write; wait for the next signal
            payload = self._read_wrapped(head + LEN_PREFIX, plen)
            head += LEN_PREFIX + plen
            # Publish the freed space BEFORE dispatching, so the producer can reuse it while we work
            # (a slow callback must never stall the node).
            _U64.pack_into(mm, OFF_HEAD, head)
            self._head = head
            try:
                self._dispatch_block(payload, received_time)
                dispatched += 1
            except Exception:
                APP_LOG.exception(f"{self} decoding block payload ({plen} bytes)")

        dropped = _U64.unpack_from(mm, OFF_DROPPED)[0]
        if dropped != self._last_dropped:
            APP_LOG.warning(f"{self} producer dropped {dropped - self._last_dropped} messages "
                            f"(consumer too slow / ring too small); total={dropped}")
            self._last_dropped = dropped

    def _dispatch_block(self, payload: bytes, received_time: float) -> None:
        """Walk one block's binary receipts, delivering only the logs we are registered for.

        Deliberately does not build dicts for logs nobody wants: the address and topic0 are read
        first and matched against the index, and the full event is materialised only on a hit. A
        block's logs must still be walked in order because every field is variable-length.
        """
        o = 0
        block_number = _U64.unpack_from(payload, o)[0]; o += 8
        block_hash = "0x" + payload[o:o + 32].hex(); o += 32
        o += 32                                              # parent_hash (unused)
        o += 8                                               # timestamp (unused)
        o += 4                                               # tx_count (unused)
        receipt_count = _U32.unpack_from(payload, o)[0]; o += 4

        index = self._index
        log_index = 0                                        # block-wide, matching chain semantics
        self._blocks_seen += 1

        for tx_index in range(receipt_count):
            tx_hash = "0x" + payload[o:o + 32].hex(); o += 32
            o += 1 + 1 + 20                                  # status, tx_type, contract_address
            o += 8 + 8 + 8                                   # cumulative/gas_used/gas_used_for_l1
            o += 32                                          # effective_gas_price
            log_count = _U32.unpack_from(payload, o)[0]; o += 4

            for _ in range(log_count):
                addr_raw = payload[o:o + 20]; o += 20
                topic_count = payload[o]; o += 1
                topics_at = o
                o += 32 * topic_count
                data_len = _U32.unpack_from(payload, o)[0]; o += 4
                data_at = o
                o += data_len

                this_log_index = log_index
                log_index += 1

                if topic_count == 0:
                    continue
                regs = index.get(("0x" + addr_raw.hex(), "0x" + payload[topics_at:topics_at + 32].hex()))
                if not regs:
                    continue

                log = {
                    "address": "0x" + addr_raw.hex(),
                    "topics": ["0x" + payload[topics_at + 32 * i:topics_at + 32 * (i + 1)].hex()
                               for i in range(topic_count)],
                    "data": "0x" + payload[data_at:data_at + data_len].hex(),
                    "blockNumber": block_number,
                    "blockHash": block_hash,
                    "transactionHash": tx_hash,
                    # Neither index is in the wire format: transactionIndex is the receipt's position
                    # in the block and logIndex counts every log of the block, which is how the chain
                    # numbers them — so these dedup exactly against the same log seen over WSS/IPC.
                    "transactionIndex": tx_index,
                    "logIndex": this_log_index,
                    "removed": False,
                }
                for registration in regs:
                    self._parse_event_and_call_cb(registration, log, received_time)
                self._events_delivered += 1

    def _parse_event_and_call_cb(self, registration: EventListenerRegistration,
                                 log: dict, received_time: float) -> None:
        try:
            if registration.log_processor:
                result = registration.log_processor(log)
            else:
                result = registration.event.process_log(log)
            result[TIME_TAG_WS_RECEIVED] = received_time
            result["provider"] = self
            registration.listener([result])
        except Exception:
            APP_LOG.exception(f"{self} _parse_event_and_call_cb {registration=} {log=}")
