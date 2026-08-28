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
	"sync"

	"github.com/spf13/pflag"

	"github.com/ethereum/go-ethereum/common"
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
}

var DefaultConfig = Config{
	SocketPath: "",
	RingSize:   32 * 1024 * 1024, // 32 MiB data ring
}

func ConfigAddOptions(prefix string, f *pflag.FlagSet) {
	f.String(prefix+".socket-path", DefaultConfig.SocketPath, "unix-domain socket path where the exporter hands the shared-memory and event file descriptors to a single consumer; setting this enables low-latency block-receipt export over a shared-memory SPSC ring buffer signalled by an eventfd (empty = disabled)")
	f.Int(prefix+".ring-size", DefaultConfig.RingSize, "size in bytes of the shared-memory data ring; rounded up to a power of two (floor 64 KiB)")
}

// Enabled reports whether the exporter should run, i.e. a socket path is set.
func (c *Config) Enabled() bool {
	return c.SocketPath != ""
}

func (c *Config) Validate() error {
	return nil
}

// Exporter is the single-producer side of the receipt export. Its Start opens
// the fd-passing socket; PublishBlockReceipts is called inline from the
// block-commit path (serialising, encoding and pushing to the ring on the
// caller's goroutine) and never blocks.
type Exporter struct {
	stopwaiter.StopWaiter

	config   Config
	capacity uint64
	ring     *ring

	// pubMu serialises PublishBlockReceipts and guards buf and the ring's
	// single-producer state. In practice appendBlock is already serialised by
	// the engine's createBlocksMutex, so this is uncontended.
	pubMu sync.Mutex
	// buf is reused across publishes to avoid per-block allocations while
	// encoding the binary payload.
	buf []byte

	listener *net.UnixListener
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
	)

	// Close the listener on shutdown to unblock the accept loop.
	e.LaunchThread(func(ctx context.Context) {
		<-ctx.Done()
		_ = e.listener.Close()
	})
	e.LaunchThread(e.acceptLoop)
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

// PublishBlockReceipts encodes and pushes a block's receipts to the ring inline
// on the caller's goroutine, then signals the consumer via the eventfd. It is
// non-blocking: if the ring is full the block is dropped and counted, so the
// block-commit path is never stalled. It is intended to be called from the
// engine's serialised block-commit path (single producer).
func (e *Exporter) PublishBlockReceipts(block *types.Block, receipts types.Receipts) {
	if e == nil || block == nil {
		return
	}
	e.pubMu.Lock()
	defer e.pubMu.Unlock()

	e.buf = encodeBlock(e.buf[:0], block, receipts)
	e.pushLocked(block.NumberU64(), "block receipts")
}

// PublishTxLogs pushes a single transaction's logs (a RecordTx) as soon as that
// tx has executed, from inside the block-building loop - well before the state
// root, the block hash or the DB write. Same non-blocking contract as
// PublishBlockReceipts: a full ring drops the record rather than stalling
// execution.
//
// These records are SPECULATIVE: the block they belong to is not yet known to
// be good. The RecordBlock published later is the reconciliation point. See
// protocol.go.
func (e *Exporter) PublishTxLogs(blockNumber uint64, txIndex, firstLogIndex uint32, txHash common.Hash, status uint64, logs []*types.Log) {
	if e == nil || len(logs) == 0 {
		return // nothing to notify about; don't spend a ring slot or an eventfd write
	}
	e.pubMu.Lock()
	defer e.pubMu.Unlock()

	e.buf = encodeTxLogs(e.buf[:0], blockNumber, txIndex, firstLogIndex, txHash, status, logs)
	e.pushLocked(blockNumber, "tx logs")
}

// pushLocked pushes e.buf to the ring and signals the consumer. Caller must
// hold pubMu and have already filled e.buf.
func (e *Exporter) pushLocked(blockNumber uint64, what string) {
	if !e.ring.push(e.buf) {
		// Ring full or message larger than the ring; the consumer is behind.
		if d := e.ring.droppedCount(); d == 1 || d%1000 == 0 {
			log.Warn("receipt exporter ring full, dropping "+what,
				"totalDropped", d, "block", blockNumber, "payloadLen", len(e.buf), "ringSize", e.capacity)
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
