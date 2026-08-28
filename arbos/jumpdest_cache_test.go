// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package arbos

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
)

// The shared cache is only safe because a hit is indistinguishable from a miss.
// These tests pin that round-trip property, plus the opt-in default that keeps
// the replay/WAVM binary on geth's stock per-EVM cache.
//
// (vm.codeBitmap itself is unexported, so these exercise the cache contract
// rather than the analysis; the analysis being a pure function of the code is
// geth's invariant, not something this package changes.)

func TestJumpDestCacheDisabledByDefault(t *testing.T) {
	sharedJumpDests.Store(nil)
	// A typed nil leaking into the interface would make the `!= nil` guard in
	// block_processor.go true and silently move replay off its current path.
	if got := jumpDestCache(); got != nil {
		t.Fatalf("expected a nil vm.JumpDestCache when nothing is installed, got %#v", got)
	}
}

func TestInstallSharedJumpDestCacheRejectsNonPositive(t *testing.T) {
	sharedJumpDests.Store(nil)
	for _, entries := range []int{0, -1} {
		InstallSharedJumpDestCache(entries)
		if jumpDestCache() != nil {
			t.Fatalf("InstallSharedJumpDestCache(%d) should leave the cache disabled", entries)
		}
	}
}

func TestJumpDestCacheRoundTrip(t *testing.T) {
	sharedJumpDests.Store(nil)
	InstallSharedJumpDestCache(16)
	cache := jumpDestCache()
	if cache == nil {
		t.Fatal("cache should be installed")
	}

	code := []byte{byte(vm.PUSH2), 0x5b, 0x5b, byte(vm.JUMPDEST), byte(vm.STOP)}
	hash := crypto.Keccak256Hash(code)

	if _, ok := cache.Load(hash); ok {
		t.Fatal("unexpected hit on an empty cache")
	}

	want := vm.BitVec{0b0001_0001, 0b0000_0000}
	cache.Store(hash, want)

	got, ok := cache.Load(hash)
	if !ok {
		t.Fatal("expected a hit after Store")
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("cached bitmap differs from what was stored:\n got %x\nwant %x", got, want)
	}
}

func TestJumpDestCacheKeyedByCodeHash(t *testing.T) {
	sharedJumpDests.Store(nil)
	InstallSharedJumpDestCache(16)
	cache := jumpDestCache()

	hashA := crypto.Keccak256Hash([]byte{byte(vm.JUMPDEST), byte(vm.STOP)})
	hashB := crypto.Keccak256Hash([]byte{byte(vm.PUSH1), 0x5b, byte(vm.STOP)})
	vecA := vm.BitVec{0b0000_0011}
	vecB := vm.BitVec{0b0000_0101}

	cache.Store(hashA, vecA)
	cache.Store(hashB, vecB)

	gotA, okA := cache.Load(hashA)
	gotB, okB := cache.Load(hashB)
	if !okA || !okB {
		t.Fatal("both entries should be present")
	}
	if !bytes.Equal(gotA, vecA) || !bytes.Equal(gotB, vecB) {
		t.Fatal("distinct code hashes must not alias in the cache")
	}
	if _, ok := cache.Load(common.Hash{}); ok {
		t.Fatal("unknown code hash should miss")
	}
}

// Eviction must never change results: an evicted entry is simply recomputed to
// the same value, so a full cache degrades to today's behaviour and no further.
func TestJumpDestCacheEvictionIsLossless(t *testing.T) {
	sharedJumpDests.Store(nil)
	InstallSharedJumpDestCache(2)
	cache := jumpDestCache()

	hashes := make([]common.Hash, 4)
	vecs := make([]vm.BitVec, 4)
	for i := range hashes {
		hashes[i] = crypto.Keccak256Hash([]byte{byte(i)})
		vecs[i] = vm.BitVec{byte(i + 1)}
		cache.Store(hashes[i], vecs[i])
	}

	// Whatever survived a capacity-2 cache must still be byte-exact; entries
	// that were evicted simply miss, which the caller handles by recomputing.
	for i := range hashes {
		got, ok := cache.Load(hashes[i])
		if ok && !bytes.Equal(got, vecs[i]) {
			t.Fatalf("entry %d came back corrupted: got %x want %x", i, got, vecs[i])
		}
	}
}
