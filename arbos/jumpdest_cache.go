// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package arbos

import (
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/lru"
	"github.com/ethereum/go-ethereum/core/vm"
)

// A process-wide cache of JUMPDEST analysis results, shared across every EVM
// this process builds.
//
// vm.NewEVM allocates a fresh, empty mapJumpDests per instance, and
// ProduceBlockAdvanced constructs a new EVM for every transaction in the block.
// The two together mean each tx re-derives the JUMPDEST bitmap of every
// contract it touches from scratch - and with proxy-heavy contracts, the same
// implementation code is re-analysed on every single tx. Profiling a live node
// put vm.codeBitmapInternal at ~12% of block-execution CPU.
//
// geth's own NewEVM comment says an EVM "meant to be used throughout the entire
// state transition of a block, with the transaction context switched as needed
// by calling evm.SetTxContext", and it exposes SetJumpDestCache for exactly this
// - an extension point nothing was using. Hoisting the EVM out of the tx loop
// would be the other fix, but rollbackToGroupCheckpoint *replaces*
// buildState.statedb, so a hoisted EVM would be left holding a stale StateDB.
// Sharing the analysis cache gets the same win without that hazard.
//
// Safe to share because the analysis is a pure, deterministic function of the
// contract's code: a cache hit returns exactly what recomputing would. Keyed by
// code hash, so distinct code can never collide. Eviction order cannot affect
// results either - an evicted entry is simply recomputed to the same value.
//
// Installation is opt-in and off by default, which deliberately leaves the
// replay/WAVM fraud-proof binary on precisely the code path it uses today:
// nothing calls InstallSharedJumpDestCache there, jumpDestCache() returns nil,
// and ProduceBlockAdvanced keeps geth's stock per-EVM map.
type sharedJumpDestCache struct {
	cache *lru.Cache[common.Hash, vm.BitVec]
}

func (c *sharedJumpDestCache) Load(codeHash common.Hash) (vm.BitVec, bool) {
	return c.cache.Get(codeHash)
}

func (c *sharedJumpDestCache) Store(codeHash common.Hash, vec vm.BitVec) {
	c.cache.Add(codeHash, vec)
}

// DefaultJumpDestCacheEntries bounds the shared cache. A BitVec is one bit per
// code byte, so an entry costs len(code)/8 bytes: ~3 KiB for a 24 KiB contract,
// ~12 KiB at the 98304-byte MaxCodeSize some Orbit chains configure. 4096
// entries is therefore a few tens of MiB worst case and far less in practice,
// while comfortably covering the working set of contracts a chain actually
// touches block to block.
const DefaultJumpDestCacheEntries = 4096

// atomic so installation cannot race a concurrent reader; in practice it is
// installed once during node construction, before any block is executed, but
// the prefetch goroutine and the main execution path both read it afterwards.
var sharedJumpDests atomic.Pointer[sharedJumpDestCache]

// InstallSharedJumpDestCache enables the process-wide JUMPDEST analysis cache.
// Call once during node startup, before block processing begins. A non-positive
// size leaves the cache disabled (stock per-EVM behaviour).
func InstallSharedJumpDestCache(entries int) {
	if entries <= 0 {
		return
	}
	sharedJumpDests.Store(&sharedJumpDestCache{
		cache: lru.NewCache[common.Hash, vm.BitVec](entries),
	})
}

// jumpDestCache returns the installed shared cache, or a nil interface when
// none was installed (so callers fall back to the stock per-EVM map).
func jumpDestCache() vm.JumpDestCache {
	if c := sharedJumpDests.Load(); c != nil {
		return c
	}
	return nil
}
