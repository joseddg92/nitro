// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package receiptexporter

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"
)

// ring is the producer side of the SPSC shared-memory ring buffer. It owns an
// anonymous shared-memory segment (a memfd) and an eventfd used to wake the
// consumer. It is safe for a single producer goroutine; it must not be used
// concurrently by multiple producers.
type ring struct {
	shmFd    int
	eventFd  int
	mem      []byte // full mmap: header + data ring
	data     []byte // mem[HeaderSize : HeaderSize+capacity]
	capacity uint64

	head    *uint64 // consumer cursor (we only read this, with acquire load)
	tail    *uint64 // producer cursor (we own it)
	dropped *uint64
	msgs    *uint64
}

// newRing allocates the shared-memory segment (memfd of HeaderSize+capacity
// bytes) and an eventfd, mmaps the segment, and writes the header. capacity
// must be a power of two.
func newRing(capacity uint64) (*ring, error) {
	if capacity == 0 || capacity&(capacity-1) != 0 {
		return nil, fmt.Errorf("ring capacity must be a power of two, got %d", capacity)
	}
	total := uint64(HeaderSize) + capacity

	shmFd, err := unix.MemfdCreate("nitro-receipt-ring", unix.MFD_CLOEXEC)
	if err != nil {
		return nil, fmt.Errorf("memfd_create: %w", err)
	}
	if err := unix.Ftruncate(shmFd, int64(total)); err != nil {
		unix.Close(shmFd)
		return nil, fmt.Errorf("ftruncate shm to %d: %w", total, err)
	}

	mem, err := unix.Mmap(shmFd, 0, int(total), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		unix.Close(shmFd)
		return nil, fmt.Errorf("mmap shm: %w", err)
	}

	// Blocking (no EFD_NONBLOCK) so the consumer can sleep on read(); the
	// producer's writes never block for any realistic counter value.
	eventFd, err := unix.Eventfd(0, unix.EFD_CLOEXEC)
	if err != nil {
		unix.Munmap(mem)
		unix.Close(shmFd)
		return nil, fmt.Errorf("eventfd: %w", err)
	}

	r := &ring{
		shmFd:    shmFd,
		eventFd:  eventFd,
		mem:      mem,
		data:     mem[HeaderSize : HeaderSize+capacity],
		capacity: capacity,
		head:     (*uint64)(unsafe.Pointer(&mem[offHead])),
		tail:     (*uint64)(unsafe.Pointer(&mem[offTail])),
		dropped:  (*uint64)(unsafe.Pointer(&mem[offDropped])),
		msgs:     (*uint64)(unsafe.Pointer(&mem[offMsgCount])),
	}

	// Write the header. Cursors start at zero (segment is zero-filled by the
	// kernel). Write magic last-ish; consumers key off it but they only map
	// after the handshake, which we send after this returns.
	copy(mem[offMagic:offMagic+8], magic[:])
	binary.LittleEndian.PutUint32(mem[offVersion:], ProtocolVersion)
	binary.LittleEndian.PutUint32(mem[offHeaderSize:], HeaderSize)
	binary.LittleEndian.PutUint64(mem[offCapacity:], capacity)

	return r, nil
}

// writeAt copies p into the data ring starting at the monotonic cursor pos,
// wrapping around the end of the ring if necessary.
func (r *ring) writeAt(pos uint64, p []byte) {
	off := pos % r.capacity
	n := copy(r.data[off:], p)
	if n < len(p) {
		copy(r.data, p[n:])
	}
}

// push writes one length-prefixed record. It returns false (and increments the
// dropped counter) if the payload does not currently fit; it never blocks.
// The caller is responsible for signalling the eventfd after a successful push.
func (r *ring) push(payload []byte) bool {
	recLen := uint64(lenPrefixSize + len(payload))
	if recLen > r.capacity {
		// Can never fit; count as a drop.
		atomic.AddUint64(r.dropped, 1)
		return false
	}

	tail := atomic.LoadUint64(r.tail)    // we own tail; plain load suffices
	head := atomic.LoadUint64(r.head)    // acquire: pairs with consumer's release store
	if recLen > r.capacity-(tail-head) { // not enough free space
		atomic.AddUint64(r.dropped, 1)
		return false
	}

	var lenBuf [lenPrefixSize]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	r.writeAt(tail, lenBuf[:])
	r.writeAt(tail+lenPrefixSize, payload)

	// Publish: the release store makes the record bytes visible to a consumer
	// that observes the new tail with an acquire load.
	atomic.StoreUint64(r.tail, tail+recLen)
	atomic.AddUint64(r.msgs, 1)
	return true
}

// signal wakes the consumer by incrementing the eventfd counter.
func (r *ring) signal() error {
	var one [8]byte
	binary.LittleEndian.PutUint64(one[:], 1)
	_, err := unix.Write(r.eventFd, one[:])
	return err
}

func (r *ring) droppedCount() uint64 { return atomic.LoadUint64(r.dropped) }

func (r *ring) close() {
	if r.mem != nil {
		_ = unix.Munmap(r.mem)
		r.mem = nil
	}
	if r.eventFd >= 0 {
		_ = unix.Close(r.eventFd)
		r.eventFd = -1
	}
	if r.shmFd >= 0 {
		_ = unix.Close(r.shmFd)
		r.shmFd = -1
	}
}
