// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package gethexec

import (
	"crypto/ecdsa"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"

	"github.com/offchainlabs/nitro/arbos/l1pricing"
	"github.com/offchainlabs/nitro/cmd/chaininfo"
)

// prewarming is an optimisation, so what these tests pin is that it cannot be
// observed: whatever it caches must equal what the execution loop would have
// computed on its own, including when the signer it guessed turns out to be the
// wrong one.

func prewarmTestChainConfig() *params.ChainConfig {
	cfg := *chaininfo.ArbitrumOneChainConfig()
	return &cfg
}

func signedTxs(t *testing.T, signer types.Signer, key *ecdsa.PrivateKey, n int) types.Transactions {
	t.Helper()
	to := common.HexToAddress("0x00000000000000000000000000000000000000ff")
	txes := make(types.Transactions, 0, n)
	for i := range n {
		tx, err := types.SignNewTx(key, signer, &types.DynamicFeeTx{
			ChainID:   signer.ChainID(),
			Nonce:     uint64(i),
			To:        &to,
			Gas:       21000,
			GasFeeCap: big.NewInt(1_000_000_000),
			GasTipCap: big.NewInt(1),
		})
		if err != nil {
			t.Fatalf("signing tx %d: %v", i, err)
		}
		txes = append(txes, tx)
	}
	return txes
}

// waitForSenders gives the detached workers a moment to land. They are
// fire-and-forget by design, so a test cannot join them.
func waitForSenders(signer types.Signer, txes types.Transactions) {
	for range 200 {
		warm := 0
		for _, tx := range txes {
			if from, err := types.Sender(signer, tx); err == nil && from != (common.Address{}) {
				warm++
			}
		}
		if warm == len(txes) {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPrewarmSendersMatchesSerialRecovery(t *testing.T) {
	cfg := prewarmTestChainConfig()
	parent := &types.Header{Number: big.NewInt(1_000_000), Time: uint64(time.Now().Unix())}
	signer := types.MakeSigner(cfg, new(big.Int).Add(parent.Number, big.NewInt(1)), parent.Time, params.MaxArbosVersionSupported)

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	want := crypto.PubkeyToAddress(key.PublicKey)
	txes := signedTxs(t, signer, key, 8)

	prewarmSenders(cfg, parent, params.MaxArbosVersionSupported, txes)
	waitForSenders(signer, txes)

	// What the execution loop would compute, on the objects prewarming touched.
	for i, tx := range txes {
		got, err := types.Sender(signer, tx)
		if err != nil {
			t.Fatalf("tx %d: %v", i, err)
		}
		if got != want {
			t.Fatalf("tx %d: sender %s, want %s", i, got, want)
		}
	}
}

// The whole safety argument: if the signer guessed from the parent header picks
// a different fork rule than the loop's, Sender must discard the cached entry
// and recompute - a wasted goroutine, never a wrong sender.
func TestPrewarmSendersWrongSignerIsHarmless(t *testing.T) {
	cfg := prewarmTestChainConfig()
	parent := &types.Header{Number: big.NewInt(1_000_000), Time: uint64(time.Now().Unix())}

	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	want := crypto.PubkeyToAddress(key.PublicKey)

	realSigner := types.MakeSigner(cfg, new(big.Int).Add(parent.Number, big.NewInt(1)), parent.Time, params.MaxArbosVersionSupported)
	txes := signedTxs(t, realSigner, key, 8)

	// Poison the cache the way a fork-boundary mis-guess would: a signer for a
	// different chain, which Equal() rejects.
	otherCfg := prewarmTestChainConfig()
	otherCfg.ChainID = big.NewInt(987654321)
	wrongSigner := types.MakeSigner(otherCfg, new(big.Int).Add(parent.Number, big.NewInt(1)), parent.Time, params.MaxArbosVersionSupported)
	for _, tx := range txes {
		_, _ = types.Sender(wrongSigner, tx)
	}

	// The loop's authoritative signer must still produce the right answer.
	for i, tx := range txes {
		got, err := types.Sender(realSigner, tx)
		if err != nil {
			t.Fatalf("tx %d: %v", i, err)
		}
		if got != want {
			t.Fatalf("tx %d: stale cache leaked through: sender %s, want %s", i, got, want)
		}
	}
}

func TestPrewarmSendersNoopOnTinyBlocks(t *testing.T) {
	cfg := prewarmTestChainConfig()
	parent := &types.Header{Number: big.NewInt(1_000_000), Time: uint64(time.Now().Unix())}
	// Must not panic or spawn work for 0 or 1 transactions.
	prewarmSenders(cfg, parent, params.MaxArbosVersionSupported, nil)
	prewarmSenders(cfg, parent, params.MaxArbosVersionSupported, types.Transactions{})

	signer := types.MakeSigner(cfg, new(big.Int).Add(parent.Number, big.NewInt(1)), parent.Time, params.MaxArbosVersionSupported)
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	prewarmSenders(cfg, parent, params.MaxArbosVersionSupported, signedTxs(t, signer, key, 1))
}

// --- calldata-units prewarming -------------------------------------------------

// Level 0 is ArbOS's default, so it must be a usable level and not read as
// "unobserved". This is the bug the level+1 encoding exists to prevent.
func TestObservedBrotliLevelZeroIsUsable(t *testing.T) {
	observedBrotliLevel.Store(0)
	if _, ok := loadObservedBrotliLevel(); ok {
		t.Fatal("zero value must mean 'not yet observed'")
	}
	setObservedBrotliLevel(0)
	level, ok := loadObservedBrotliLevel()
	if !ok || level != 0 {
		t.Fatalf("level 0 must round-trip as observed, got level=%d ok=%v", level, ok)
	}
	setObservedBrotliLevel(11)
	if level, ok := loadObservedBrotliLevel(); !ok || level != 11 {
		t.Fatalf("level 11 round-trip failed, got level=%d ok=%v", level, ok)
	}
}

// What prewarming caches must equal what the block loop would compute itself.
func TestPrewarmCalldataUnitsMatchesSerialComputation(t *testing.T) {
	cfg := prewarmTestChainConfig()
	signer := types.MakeSigner(cfg, big.NewInt(1_000_001), uint64(time.Now().Unix()), params.MaxArbosVersionSupported)
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	txes := signedTxs(t, signer, key, 4)

	const level = 0
	setObservedBrotliLevel(level)
	prewarmCalldataUnits(txes, 0, 1)

	for i, tx := range txes {
		cached := tx.GetCachedCalldataUnits(level)
		want, err := l1pricing.PosterUnitsForTx(tx, l1pricing.BatchPosterAddress, level)
		if err != nil {
			t.Fatalf("tx %d: %v", i, err)
		}
		// Non-poster txs legitimately yield 0 units, which the cache treats as
		// empty; only assert when there is something to cache.
		if want != 0 {
			if cached == nil {
				t.Fatalf("tx %d: expected a cached value", i)
			}
			if *cached != want {
				t.Fatalf("tx %d: cached %d, serial computation %d", i, *cached, want)
			}
		}
	}
}

// A stale/wrong level must miss rather than hand back the wrong units.
func TestPrewarmCalldataUnitsWrongLevelMisses(t *testing.T) {
	cfg := prewarmTestChainConfig()
	signer := types.MakeSigner(cfg, big.NewInt(1_000_001), uint64(time.Now().Unix()), params.MaxArbosVersionSupported)
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	txes := signedTxs(t, signer, key, 4)

	setObservedBrotliLevel(4) // prewarm at a level the "loop" will not ask for
	prewarmCalldataUnits(txes, 0, 1)

	for i, tx := range txes {
		if got := tx.GetCachedCalldataUnits(11); got != nil {
			t.Fatalf("tx %d: level 11 must miss a level-4 entry, got %d", i, *got)
		}
	}
}

func TestPrewarmCalldataUnitsNoopWhenUnobserved(t *testing.T) {
	cfg := prewarmTestChainConfig()
	signer := types.MakeSigner(cfg, big.NewInt(1_000_001), uint64(time.Now().Unix()), params.MaxArbosVersionSupported)
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	txes := signedTxs(t, signer, key, 3)

	observedBrotliLevel.Store(0) // unobserved
	prewarmCalldataUnits(txes, 0, 1)
	for i, tx := range txes {
		if got := tx.GetCachedCalldataUnits(0); got != nil {
			t.Fatalf("tx %d: nothing should be cached before a level is observed, got %d", i, *got)
		}
	}
}
