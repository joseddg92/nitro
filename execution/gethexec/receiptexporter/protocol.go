// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

// Package receiptexporter implements a low-latency, single-producer
// single-consumer (SPSC) export of block receipts to a co-located process.
//
// The node (the single producer) publishes the receipts of every newly
// committed block into a lock-free ring buffer that lives in a shared-memory
// segment (an anonymous memfd). A single external consumer maps the same
// segment and drains it. The consumer is woken via an eventfd so it can block
// (zero busy-polling) yet react within microseconds of a block being
// committed.
//
// Both the shared-memory fd and the eventfd are handed to the consumer over a
// unix-domain socket using SCM_RIGHTS fd passing, so no filesystem paths or
// permissions for the shared segment itself are required.
//
// # Shared memory layout
//
// All integers are little-endian. The segment starts with a fixed 4096-byte
// header (one page) followed by the data ring of `capacity` bytes (capacity is
// a power of two).
//
//	offset  size  field         notes
//	------  ----  -----------   ---------------------------------------------
//	0       8     magic         ASCII "NITRORB1"
//	8       4     version       protocol version (currently 1)
//	12      4     header_size   bytes before the data ring (4096)
//	16      8     capacity      size of the data ring, power of two
//	24      8     dropped       producer-only: records dropped (ring full)
//	32      8     msg_count     producer-only: records published
//	64      8     head          consumer read cursor (monotonic, mod capacity)
//	128     8     tail          producer write cursor (monotonic, mod capacity)
//	4096    ..    data[capacity]
//
// head and tail are monotonically increasing 64-bit counters; the physical
// index into the data ring is (cursor % capacity). The number of readable
// bytes is (tail - head); free space is (capacity - (tail - head)). Because
// the counters are monotonic there is no full/empty ambiguity.
//
// head lives on its own cache line (offset 64) and tail on the next (offset
// 128) to avoid false sharing between producer and consumer.
//
// # Framing (binary, length-prefixed)
//
// Each record written to the ring is:
//
//	[payload_len : uint32 little-endian][payload : payload_len bytes]
//
// A record (the 4-byte length plus its payload) may wrap around the end of the
// data ring; readers and writers must handle the split. The producer only
// advances `tail` (with release ordering) after the whole record has been
// written, so once the consumer observes an advanced `tail` (with acquire
// ordering) the full record is guaranteed to be present.
//
// The payload itself is a compact little-endian binary encoding of one block's
// receipts (chosen over JSON to minimise producer-side serialisation cost); see
// payload.go for the exact layout. The ring framing is payload-agnostic.
//
// # Payload layout (protocol version 2, little-endian unless noted)
//
//	Block:
//	  block_number         u64
//	  block_hash           [32]byte
//	  parent_hash          [32]byte
//	  timestamp            u64
//	  tx_count             u32
//	  receipt_count        u32
//	  receipts[receipt_count]
//	Receipt:
//	  tx_hash              [32]byte
//	  status               u8            (0 = failed, 1 = success)
//	  tx_type              u8
//	  contract_address     [20]byte      (all-zero if none)
//	  cumulative_gas_used  u64
//	  gas_used             u64
//	  gas_used_for_l1      u64           (Arbitrum extra)
//	  effective_gas_price  [32]byte      big-endian uint256
//	  log_count            u32
//	  logs[log_count]
//	Log:
//	  address              [20]byte
//	  topic_count          u8
//	  topics               [topic_count][32]byte
//	  data_len             u32
//	  data                 [data_len]byte
//
// # Handshake
//
// When a consumer connects to the unix socket the producer replies with a
// 24-byte handshake (and two fds via SCM_RIGHTS, in order: [0]=shm, [1]=event):
//
//	offset  size  field
//	0       8     magic        ASCII "NITRORB1"
//	8       4     version
//	12      4     header_size
//	16      8     capacity
package receiptexporter

const (
	// HeaderSize is the number of bytes reserved before the data ring.
	HeaderSize = 4096

	// ProtocolVersion is bumped on any incompatible layout change.
	// v2: binary receipt payload (was JSON in v1).
	ProtocolVersion = 2

	// Header field offsets.
	offMagic      = 0
	offVersion    = 8
	offHeaderSize = 12
	offCapacity   = 16
	offDropped    = 24
	offMsgCount   = 32
	offHead       = 64
	offTail       = 128

	// lenPrefixSize is the size of the per-record binary length prefix.
	lenPrefixSize = 4

	// handshakeSize is the size of the fixed handshake sent to a new consumer.
	handshakeSize = 24
)

// magic identifies the segment/handshake. Exactly 8 bytes.
var magic = [8]byte{'N', 'I', 'T', 'R', 'O', 'R', 'B', '1'}
