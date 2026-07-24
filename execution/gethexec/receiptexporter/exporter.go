// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package receiptexporter

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"

	"github.com/spf13/pflag"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"

	"golang.org/x/sys/unix"

	"github.com/offchainlabs/nitro/util/stopwaiter"
)

// Config configures the shared-memory receipt exporter. The feature is enabled
// simply by setting SocketPath to a non-empty value; leaving it empty disables
// it entirely.
type Config struct {
	SocketPath string `koanf:"socket-path"`
	RingSize   int    `koanf:"ring-size"`
	QueueSize  int    `koanf:"queue-size"`
}

var DefaultConfig = Config{
	SocketPath: "",
	RingSize:   32 * 1024 * 1024, // 32 MiB data ring
	QueueSize:  1024,             // blocks buffered before the serializer
}

func ConfigAddOptions(prefix string, f *pflag.FlagSet) {
	f.String(prefix+".socket-path", DefaultConfig.SocketPath, "unix-domain socket path where the exporter hands the shared-memory and event file descriptors to a single consumer; setting this enables low-latency block-receipt export over a shared-memory SPSC ring buffer signalled by an eventfd (empty = disabled)")
	f.Int(prefix+".ring-size", DefaultConfig.RingSize, "size in bytes of the shared-memory data ring; rounded up to a power of two (floor 64 KiB)")
	f.Int(prefix+".queue-size", DefaultConfig.QueueSize, "number of blocks buffered between the block-commit path and the serializer before receipts are dropped")
}

// Enabled reports whether the exporter should run, i.e. a socket path is set.
func (c *Config) Enabled() bool {
	return c.SocketPath != ""
}

func (c *Config) Validate() error {
	if !c.Enabled() {
		return nil
	}
	if c.QueueSize <= 0 {
		return errors.New("receipt-export.queue-size must be positive when receipt-export.socket-path is set")
	}
	return nil
}

type blockReceipts struct {
	block    *types.Block
	receipts types.Receipts
}

// Exporter is the single-producer side of the receipt export. Its Start opens
// the fd-passing socket and launches background workers; PublishBlockReceipts
// is called from the block-commit path and never blocks.
type Exporter struct {
	stopwaiter.StopWaiter

	config   Config
	capacity uint64
	ring     *ring

	blockCh chan blockReceipts

	// buf is reused across serializeLoop iterations (single goroutine) to
	// avoid per-block allocations while encoding the binary payload.
	buf []byte

	listener *net.UnixListener

	// queueDropped counts blocks dropped because the serializer queue was
	// full (distinct from ring.dropped, which counts ring-full drops).
	queueDropped atomic.Uint64
}

// New allocates the shared-memory segment and eventfd but does not yet accept
// consumers; call Start for that.
func New(config *Config) (*Exporter, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	capacity := normalizeCapacity(config.RingSize)
	r, err := newRing(capacity)
	if err != nil {
		return nil, err
	}
	return &Exporter{
		config:   *config,
		capacity: capacity,
		ring:     r,
		blockCh:  make(chan blockReceipts, config.QueueSize),
	}, nil
}

func (e *Exporter) Start(ctxIn context.Context) error {
	e.StopWaiter.Start(ctxIn, e)

	// Remove a stale socket from a previous run, then listen.
	if err := os.Remove(e.config.SocketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("removing stale receipt-export socket %q: %w", e.config.SocketPath, err)
	}
	addr := &net.UnixAddr{Name: e.config.SocketPath, Net: "unix"}
	listener, err := net.ListenUnix("unix", addr)
	if err != nil {
		return fmt.Errorf("listening on receipt-export socket %q: %w", e.config.SocketPath, err)
	}
	e.listener = listener

	log.Info("receipt exporter started",
		"socket", e.config.SocketPath,
		"ringSize", e.capacity,
		"queueSize", e.config.QueueSize,
	)

	// Close the listener on shutdown to unblock the accept loop.
	e.LaunchThread(func(ctx context.Context) {
		<-ctx.Done()
		_ = e.listener.Close()
	})
	e.LaunchThread(e.acceptLoop)
	e.LaunchThread(e.serializeLoop)
	return nil
}

func (e *Exporter) StopAndWait() {
	e.StopWaiter.StopAndWait()
	if e.ring != nil {
		e.ring.close()
	}
	if e.config.SocketPath != "" {
		_ = os.Remove(e.config.SocketPath)
	}
}

// PublishBlockReceipts enqueues a freshly committed block's receipts for
// export. It is non-blocking: if the serializer queue is full the block is
// dropped and counted, so the block-commit path is never stalled.
func (e *Exporter) PublishBlockReceipts(block *types.Block, receipts types.Receipts) {
	if e == nil || block == nil {
		return
	}
	select {
	case e.blockCh <- blockReceipts{block: block, receipts: receipts}:
	default:
		n := e.queueDropped.Add(1)
		if n == 1 || n%1000 == 0 {
			log.Warn("receipt exporter queue full, dropping block receipts", "totalDropped", n, "block", block.NumberU64())
		}
	}
}

func (e *Exporter) serializeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case br := <-e.blockCh:
			e.exportOne(br)
		}
	}
}

func (e *Exporter) exportOne(br blockReceipts) {
	e.buf = encodeBlock(e.buf[:0], br.block, br.receipts)
	if !e.ring.push(e.buf) {
		// Ring full or message larger than the ring; the consumer is behind.
		if d := e.ring.droppedCount(); d == 1 || d%1000 == 0 {
			log.Warn("receipt exporter ring full, dropping block receipts",
				"totalDropped", d, "block", br.block.NumberU64(), "payloadLen", len(e.buf), "ringSize", e.capacity)
		}
		return
	}
	if err := e.ring.signal(); err != nil {
		log.Error("receipt exporter failed to signal eventfd", "err", err)
	}
}

// acceptLoop accepts consumer connections and hands each the shared-memory and
// event file descriptors via SCM_RIGHTS, preceded by a fixed handshake.
func (e *Exporter) acceptLoop(ctx context.Context) {
	for {
		conn, err := e.listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			log.Warn("receipt exporter accept error", "err", err)
			continue
		}
		if err := e.sendFds(conn); err != nil {
			log.Warn("receipt exporter failed to send fds to consumer", "err", err)
			_ = conn.Close()
			continue
		}
		log.Info("receipt exporter consumer connected")
		// Keep the connection open as a liveness channel; drain and close on EOF.
		e.LaunchThread(func(ctx context.Context) {
			go func() { <-ctx.Done(); _ = conn.Close() }()
			_, _ = io.Copy(io.Discard, conn)
			_ = conn.Close()
			log.Info("receipt exporter consumer disconnected")
		})
	}
}

func (e *Exporter) sendFds(conn *net.UnixConn) error {
	var hs [handshakeSize]byte
	copy(hs[0:8], magic[:])
	binary.LittleEndian.PutUint32(hs[8:], ProtocolVersion)
	binary.LittleEndian.PutUint32(hs[12:], HeaderSize)
	binary.LittleEndian.PutUint64(hs[16:], e.capacity)

	// fd order: [0]=shared memory, [1]=eventfd.
	rights := unix.UnixRights(e.ring.shmFd, e.ring.eventFd)
	_, _, err := conn.WriteMsgUnix(hs[:], rights, nil)
	return err
}

// normalizeCapacity rounds n up to a power of two, with a 64 KiB floor.
func normalizeCapacity(n int) uint64 {
	const floor = 1 << 16
	if n < floor {
		n = floor
	}
	c := uint64(floor)
	for c < uint64(n) {
		c <<= 1
	}
	return c
}
