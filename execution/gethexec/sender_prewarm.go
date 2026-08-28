// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package gethexec

import (
	"math/big"
	"runtime"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
)

// prewarmSenders recovers the signers of a block's transactions on spare cores,
// ahead of the serial execution loop that would otherwise do it one at a time.
//
// ProduceBlockAdvanced calls types.Sender(signer, tx) for every transaction
// before executing it, which is a secp256k1 public-key recovery. On a live node
// that showed up as 37% of all cgo time (~1ms per block). It is pure setup: it
// depends on nothing the previous transactions did, so there is no reason for it
// to sit on the critical path.
//
// The work is not currently reused anywhere either. types.Sender memoises the
// result on the transaction object (tx.from), but ParseL2Transactions re-decodes
// from raw bytes on every call, so the prefetch goroutine builds its own
// transaction objects and its recoveries die with them. Warming the very objects
// the loop will execute is what makes this pay.
//
// Correctness rests on the failure mode being harmless rather than on getting
// the signer right. The loop always calls types.Sender itself with the
// authoritative signer built from the new block's header, and Sender discards a
// cached result whenever the cached signer differs (transaction_signing.go:159).
// So if the signer guessed here selects a different fork rule, the entry is
// simply ignored and recomputed correctly - the cost is a wasted goroutine, not
// a wrong sender. Concurrent recovery of the same transaction is safe too:
// tx.from is an atomic pointer and both writers store the same value.
func prewarmSenders(chainConfig *params.ChainConfig, lastBlockHeader *types.Header, lastArbosVersion uint64, txes types.Transactions) {
	// One transaction is going to be recovered by the loop before a goroutine
	// could plausibly beat it there.
	if len(txes) < 2 {
		return
	}

	// Derived from the parent header, because the new block's header does not
	// exist until ProduceBlockAdvanced builds it. Number is exact; Time and the
	// ArbOS version can only differ from the real ones across a fork boundary,
	// which costs a block's worth of prewarming and nothing else.
	signer := types.MakeSigner(
		chainConfig,
		new(big.Int).Add(lastBlockHeader.Number, big.NewInt(1)),
		lastBlockHeader.Time,
		lastArbosVersion,
	)

	// Leave a core for the execution loop itself - the point is to use the idle
	// ones, not to contend with the work being accelerated.
	workers := runtime.GOMAXPROCS(0) - 1
	if workers > len(txes) {
		workers = len(txes)
	}
	if workers > maxSenderPrewarmWorkers {
		workers = maxSenderPrewarmWorkers
	}
	if workers < 1 {
		return
	}

	// Striped rather than chunked so worker w starts at txes[w]: the loop
	// consumes transactions in order, so the earliest ones are the ones worth
	// having ready first.
	//
	// Deliberately not waited on. These goroutines only read transactions and
	// populate tx.from, so they are safe to outlive this call, and blocking
	// would trade the parallelism for a barrier.
	for w := 0; w < workers; w++ {
		go func(start int) {
			for i := start; i < len(txes); i += workers {
				// Errors (malformed signature) are the loop's to report; it will
				// hit the same error at the same point. Nothing to do here.
				_, _ = types.Sender(signer, txes[i])
			}
		}(w)
	}
}

// maxSenderPrewarmWorkers caps the fan-out on large machines: recovery is a
// fixed ~75µs per transaction and blocks hold tens of transactions, so past a
// few workers the goroutine overhead outweighs the parallelism.
const maxSenderPrewarmWorkers = 3
