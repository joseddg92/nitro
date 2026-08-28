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
