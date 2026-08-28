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
// The payload itself is a compact little-endian binary encoding (chosen over
// JSON to minimise producer-side serialisation cost); see payload.go for the
// exact layout. The ring framing is payload-agnostic.
//
// # Record types (protocol version 3)
//
// Every payload starts with a one-byte record type:
//
//	0x01 RecordBlock  one block's full receipts, published once the block is
//	                  built. Authoritative: carries the block hash and every
//	                  receipt field.
//	0x02 RecordTx     one transaction's logs, published from inside the block
//	                  loop the instant that tx finishes executing - before the
//	                  state root, the block hash, and the DB write. This is the
//	                  low-latency path: for a 19-tx block the first tx's logs go
//	                  out ~18ms before the block record does.
//
// A RecordTx is SPECULATIVE. It is emitted before the block is known to be
// good, so a consumer acting on one must tolerate the block being abandoned
// afterwards (rare: whole-block filter rejection, a balance-delta mismatch, or
// an error). The matching RecordBlock is the reconciliation point - it is only
// published if the block was actually committed. On the follower/digest path an
// individual tx is never retracted on its own, because NoopSequencingHooks
// reports SupportsGroupRollback() == false, so the group-rollback that truncates
// receipts mid-loop can never fire there.
//
// RecordTx carries no block hash (not yet computed) and no gas figures; it is
// for log delivery only. Everything else comes from the RecordBlock.
//
// # RecordBlock payload (little-endian unless noted)
//
//	Block:
//	  record_type          u8            = 0x01
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
// # RecordTx payload (little-endian)
//
//	record_type            u8            = 0x02
//	block_number           u64
//	tx_index               u32           position of this tx's receipt in the block
//	first_log_index        u32           block-wide log index of this tx's first log
//	tx_hash                [32]byte
//	status                 u8            (0 = failed, 1 = success)
//	log_count              u32
//	logs[log_count]                      same Log layout as above
//
// tx_index and first_log_index are supplied so a consumer can reproduce the
// chain's own transactionIndex/logIndex numbering (logIndex counts every log in
// the block, not per-transaction) and therefore dedup a log delivered here
// against the same log arriving later over RPC or in the RecordBlock.
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
	// v3: every payload is prefixed with a record-type byte, and per-tx log
	//     records (RecordTx) are streamed from inside the block loop.
	ProtocolVersion = 3

	// Record types, the first byte of every payload.
	RecordBlock = 0x01
	RecordTx    = 0x02

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
