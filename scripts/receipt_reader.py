#!/usr/bin/env python3
# Copyright 2026, Offchain Labs, Inc.
# For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md
"""
receipt_reader.py -- single-file demo consumer for the nitro shared-memory
receipt exporter (execution.receipt-export).

It connects to the exporter's unix-domain socket, receives the shared-memory
and eventfd file descriptors via SCM_RIGHTS, maps the ring buffer, and drains
block-receipt messages as they are published -- blocking on the eventfd so it
reacts within microseconds of a block being committed, with zero busy-polling.

Wire format (must match execution/gethexec/receiptexporter/protocol.go):

  Shared-memory header (little-endian, 4096-byte page), then data ring:
    off 0   u64  magic       ASCII "NITRORB1"
    off 8   u32  version     == 2
    off 12  u32  header_size == 4096
    off 16  u64  capacity    data ring size (power of two)
    off 24  u64  dropped     producer-only drop counter
    off 32  u64  msg_count   producer-only publish counter
    off 64  u64  head        consumer read cursor (we own/advance this)
    off 128 u64  tail        producer write cursor
    off 4096     data[capacity]

  Records in the ring are length-prefixed:  [u32 payload_len][payload bytes].
  head/tail are monotonic 64-bit counters; physical index = cursor % capacity.
  A record may wrap the end of the ring. The producer publishes a record by
  advancing `tail` only after the whole record is written, so once we observe
  an advanced tail the full record is present.

  Each payload is a compact binary encoding of one block's receipts
  (little-endian unless noted):

    Block:   block_number u64, block_hash [32], parent_hash [32],
             timestamp u64, tx_count u32, receipt_count u32, receipts[]
    Receipt: tx_hash [32], status u8, tx_type u8, contract_address [20],
             cumulative_gas_used u64, gas_used u64, gas_used_for_l1 u64,
             effective_gas_price [32] (big-endian uint256), log_count u32, logs[]
    Log:     address [20], topic_count u8, topics[topic_count][32],
             data_len u32, data[data_len]

  The 24-byte socket handshake is: magic(8) version(4) header_size(4) capacity(8),
  accompanied by two fds in order: [0]=shared memory, [1]=eventfd.

Usage:
    ./receipt_reader.py /path/to/receipts.sock          # summary per receipt
    ./receipt_reader.py /path/to/receipts.sock --full   # full receipt (JSON view)
    ./receipt_reader.py /path/to/receipts.sock --blocks  # one JSON line per block
"""

import argparse
import array
import json
import mmap
import os
import socket
import struct
import sys

MAGIC = b"NITRORB1"
VERSION = 2
HEADER_SIZE = 4096

OFF_MAGIC = 0
OFF_VERSION = 8
OFF_HEADER_SIZE = 12
OFF_CAPACITY = 16
OFF_DROPPED = 24
OFF_MSG_COUNT = 32
OFF_HEAD = 64
OFF_TAIL = 128

LEN_PREFIX = 4
HANDSHAKE_SIZE = 24
ZERO_ADDR = "0x" + "00" * 20


def recv_handshake_and_fds(sock):
    """Receive the 24-byte handshake plus the two SCM_RIGHTS fds."""
    fds = array.array("i")
    anc_size = socket.CMSG_SPACE(2 * fds.itemsize)
    data = b""
    got_fds = []
    while len(data) < HANDSHAKE_SIZE:
        chunk, ancdata, _flags, _addr = sock.recvmsg(
            HANDSHAKE_SIZE - len(data), anc_size
        )
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


def parse_handshake(data):
    magic = data[OFF_MAGIC:OFF_MAGIC + 8]
    if magic != MAGIC:
        raise RuntimeError(f"bad handshake magic {magic!r}, expected {MAGIC!r}")
    version, header_size = struct.unpack_from("<II", data, OFF_VERSION)
    (capacity,) = struct.unpack_from("<Q", data, OFF_CAPACITY)
    if version != VERSION:
        raise RuntimeError(f"unsupported protocol version {version} (need {VERSION})")
    if header_size != HEADER_SIZE:
        raise RuntimeError(f"unexpected header size {header_size}")
    if capacity == 0 or (capacity & (capacity - 1)) != 0:
        raise RuntimeError(f"capacity {capacity} is not a power of two")
    return version, header_size, capacity


def read_u64(mm, off):
    return struct.unpack_from("<Q", mm, off)[0]


def read_wrapped(mm, data_off, capacity, pos, n):
    """Read n bytes at monotonic cursor pos from the data ring, handling wrap."""
    off = pos % capacity
    if off + n <= capacity:
        return mm[data_off + off:data_off + off + n]
    first = capacity - off
    return mm[data_off + off:data_off + capacity] + mm[data_off:data_off + (n - first)]


class Cursor:
    """Sequential little-endian reader over a bytes payload."""

    __slots__ = ("b", "o")

    def __init__(self, buf):
        self.b = buf
        self.o = 0

    def take(self, n):
        v = self.b[self.o:self.o + n]
        self.o += n
        return v

    def u8(self):
        v = self.b[self.o]
        self.o += 1
        return v

    def u32(self):
        v = struct.unpack_from("<I", self.b, self.o)[0]
        self.o += 4
        return v

    def u64(self):
        v = struct.unpack_from("<Q", self.b, self.o)[0]
        self.o += 8
        return v

    def hexn(self, n):
        return "0x" + self.take(n).hex()

    def u256_be(self):
        return int.from_bytes(self.take(32), "big")


def decode_block(payload):
    c = Cursor(payload)
    blk = {
        "blockNumber": c.u64(),
        "blockHash": c.hexn(32),
        "parentHash": c.hexn(32),
        "timestamp": c.u64(),
        "txCount": c.u32(),
    }
    receipt_count = c.u32()
    receipts = []
    for _ in range(receipt_count):
        tx_hash = c.hexn(32)
        status = c.u8()
        tx_type = c.u8()
        contract = c.hexn(20)
        rcpt = {
            "transactionHash": tx_hash,
            "status": status,
            "type": tx_type,
            "contractAddress": None if contract == ZERO_ADDR else contract,
            "cumulativeGasUsed": c.u64(),
            "gasUsed": c.u64(),
            "gasUsedForL1": c.u64(),
            "effectiveGasPrice": c.u256_be(),
        }
        log_count = c.u32()
        logs = []
        for _ in range(log_count):
            addr = c.hexn(20)
            topic_count = c.u8()
            topics = [c.hexn(32) for _ in range(topic_count)]
            data = c.hexn(c.u32())
            logs.append({"address": addr, "topics": topics, "data": data})
        rcpt["logs"] = logs
        receipts.append(rcpt)
    blk["receipts"] = receipts
    return blk


def receipt_summary(rcpt):
    parts = [
        f"tx={rcpt['transactionHash']}",
        f"status={'ok' if rcpt['status'] == 1 else 'FAIL'}",
        f"gasUsed={rcpt['gasUsed']}",
        f"gasUsedForL1={rcpt['gasUsedForL1']}",
        f"logs={len(rcpt['logs'])}",
    ]
    if rcpt["contractAddress"]:
        parts.append(f"created={rcpt['contractAddress']}")
    return "  " + " ".join(parts)


def print_message(payload, mode):
    blk = decode_block(payload)
    if mode == "blocks":
        sys.stdout.write(json.dumps(blk) + "\n")
        sys.stdout.flush()
        return
    receipts = blk["receipts"]
    sys.stdout.write(
        f"block {blk['blockNumber']} {blk['blockHash']} "
        f"txCount={blk['txCount']} receipts={len(receipts)} ts={blk['timestamp']}\n"
    )
    for rcpt in receipts:
        if mode == "full":
            sys.stdout.write("  " + json.dumps(rcpt) + "\n")
        else:
            sys.stdout.write(receipt_summary(rcpt) + "\n")
    sys.stdout.flush()


def drain(mm, data_off, capacity, head, mode):
    """Drain all fully-published records starting at `head`; return new head."""
    while True:
        tail = read_u64(mm, OFF_TAIL)  # acquire (x86 TSO); ordered after eventfd read
        avail = tail - head
        if avail < LEN_PREFIX:
            return head
        (plen,) = struct.unpack("<I", read_wrapped(mm, data_off, capacity, head, LEN_PREFIX))
        if avail < LEN_PREFIX + plen:
            return head  # producer mid-write; wait for next signal
        payload = read_wrapped(mm, data_off, capacity, head + LEN_PREFIX, plen)
        head += LEN_PREFIX + plen
        # Publish the freed space before doing slow work, so the producer can reuse it.
        struct.pack_into("<Q", mm, OFF_HEAD, head)
        print_message(payload, mode)


def main():
    ap = argparse.ArgumentParser(description="nitro shared-memory receipt reader")
    ap.add_argument("socket", help="path to the exporter's unix socket (execution.receipt-export.socket-path)")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--full", action="store_true", help="print full receipt (JSON view of the decoded binary)")
    g.add_argument("--blocks", action="store_true", help="print one compact JSON line per block")
    args = ap.parse_args()
    mode = "full" if args.full else "blocks" if args.blocks else "summary"

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(args.socket)
    hs, shm_fd, event_fd = recv_handshake_and_fds(sock)
    version, header_size, capacity = parse_handshake(hs)

    total = header_size + capacity
    mm = mmap.mmap(shm_fd, total, prot=mmap.PROT_READ | mmap.PROT_WRITE, flags=mmap.MAP_SHARED)
    os.close(shm_fd)  # the mapping keeps the segment alive

    if mm[OFF_MAGIC:OFF_MAGIC + 8] != MAGIC:
        raise RuntimeError("shared-memory magic mismatch")

    sys.stderr.write(
        f"connected: version={version} capacity={capacity} "
        f"(ring={capacity // 1024} KiB) waiting for blocks...\n"
    )
    sys.stderr.flush()

    head = read_u64(mm, OFF_HEAD)  # resume from wherever the ring currently is
    try:
        while True:
            buf = os.read(event_fd, 8)  # block until the producer signals
            if len(buf) != 8:
                break
            head = drain(mm, header_size, capacity, head, mode)
    except KeyboardInterrupt:
        pass
    finally:
        dropped = read_u64(mm, OFF_DROPPED)
        published = read_u64(mm, OFF_MSG_COUNT)
        sys.stderr.write(f"\nexiting: published={published} droppedByProducer={dropped}\n")
        os.close(event_fd)
        sock.close()


if __name__ == "__main__":
    main()
