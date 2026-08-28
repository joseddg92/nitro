// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package gethexec

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"

	"github.com/offchainlabs/nitro/arbos"
	"github.com/offchainlabs/nitro/arbos/arbosState"
	"github.com/offchainlabs/nitro/arbos/arbostypes"
	"github.com/offchainlabs/nitro/execution/gethexec/receiptexporter"
)

// streamingSequencingHooks is NoopSequencingHooks plus a PostTxFilter that
// publishes each transaction's logs to the receipt exporter the moment that tx
// finishes executing.
//
// Why a hook rather than a callback threaded through arbos: PostTxFilter is
// already invoked per-tx at exactly the right point inside
// ProduceBlockAdvanced's tx loop - after the state transition, with the tx's
// logs sitting in statedb - so nothing in arbos/block_processor.go has to
// change. That file compiles into the replay/WAVM binary used for fraud
// proofs, and this keeps it untouched.
//
// The alternative (publishing from appendBlock, as the RecordBlock path does)
// waits for every remaining tx in the block plus FinalizeBlock's state-root
// computation and the block hash. For a 19-tx block that is ~18ms of extra
// latency on the first tx's logs.
//
// Delivery is speculative; see receiptexporter/protocol.go. This is only
// installed on the digest (follower) path, where SupportsGroupRollback() is
// false, so the group rollback that truncates receipts mid-loop can never fire
// and an individual tx is therefore never retracted on its own.
type streamingSequencingHooks struct {
	arbos.NoopSequencingHooks

	exporter    *receiptexporter.Exporter
	blockNumber uint64

	// nextLogIndex is the block-wide running log count, which is how the chain
	// numbers logIndex (across the whole block, not per-tx). Incremented as each
	// tx's logs are published so consumers can reproduce the numbering exactly.
	nextLogIndex uint32
}

func newStreamingSequencingHooks(txes types.Transactions, exporter *receiptexporter.Exporter, blockNumber uint64) *streamingSequencingHooks {
	return &streamingSequencingHooks{
		NoopSequencingHooks: *arbos.NewNoopSequencingHooks(txes),
		exporter:            exporter,
		blockNumber:         blockNumber,
	}
}

// PostTxFilter runs immediately after a tx's state transition, with its logs
// already recorded in statedb. It never rejects a tx - it only publishes, then
// defers to the embedded noop behaviour.
//
// positionInBlock is len(receipts) at this point, i.e. the index this tx's
// receipt will occupy, which is the chain's transactionIndex.
func (h *streamingSequencingHooks) PostTxFilter(
	header *types.Header,
	statedb *state.StateDB,
	arbState *arbosState.ArbosState,
	tx *types.Transaction,
	sender common.Address,
	dataGas uint64,
	result *core.ExecutionResult,
	positionInBlock int,
) error {
	// GetLogs stamps BlockNumber/BlockHash/BlockTimestamp onto the logs it
	// returns. The block hash is not known yet, so pass the zero hash; the real
	// one is written over it later by ProduceBlockAdvanced's touch-up pass, so
	// nothing downstream ever observes the placeholder.
	logs := statedb.GetLogs(tx.Hash(), header.Number.Uint64(), common.Hash{}, header.Time)
	if len(logs) > 0 {
		status := types.ReceiptStatusSuccessful
		if result != nil && result.Failed() {
			status = types.ReceiptStatusFailed
		}
		// #nosec G115 -- a block cannot hold enough txs or logs to overflow uint32
		h.exporter.PublishTxLogs(h.blockNumber, uint32(positionInBlock), h.nextLogIndex, tx.Hash(), status, logs)
		// #nosec G115
		h.nextLogIndex += uint32(len(logs))
	}
	return h.NoopSequencingHooks.PostTxFilter(header, statedb, arbState, tx, sender, dataGas, result, positionInBlock)
}

// produceBlockStreaming mirrors arbos.ProduceBlock but installs
// streamingSequencingHooks so each tx's logs are published as it executes.
// Kept in lockstep with arbos.ProduceBlock: same parsing, same fallback to an
// empty tx list on a malformed message.
func produceBlockStreaming(
	message *arbostypes.L1IncomingMessage,
	delayedMessagesRead uint64,
	lastBlockHeader *types.Header,
	statedb *state.StateDB,
	chainContext core.ChainContext,
	runCtx *core.MessageRunContext,
	exposeMultiGas bool,
	exporter *receiptexporter.Exporter,
) (*types.Block, *state.StateDB, types.Receipts, error) {
	chainConfig := chainContext.Config()
	lastArbosVersion := types.DeserializeHeaderExtraInformation(lastBlockHeader).ArbOSFormatVersion
	txes, err := arbos.ParseL2Transactions(message, chainConfig.ChainID, lastArbosVersion)
	if err != nil {
		log.Warn("error parsing incoming message", "err", err)
		txes = types.Transactions{}
	}

	// Recover the senders on spare cores while the loop below gets going. Must
	// happen here, on these exact transaction objects, because that is where
	// types.Sender caches its result. See sender_prewarm.go.
	prewarmSenders(chainConfig, lastBlockHeader, lastArbosVersion, txes)

	hooks := newStreamingSequencingHooks(txes, exporter, lastBlockHeader.Number.Uint64()+1)

	return arbos.ProduceBlockAdvanced(
		message.Header,
		delayedMessagesRead,
		lastBlockHeader,
		statedb,
		chainContext,
		hooks,
		false, // never the prefetch run: prefetch results are discarded, so publishing them would be wrong
		runCtx,
		exposeMultiGas,
		// nil, matching arbos.ProduceBlock. Address checking is opt-in and only
		// wired up on the delayed-message filtering path, which calls
		// ProduceBlockAdvanced directly with the engine's checker; the ordinary
		// digest path must not enable it.
		nil,
	)
}

// compile-time assertion that the hooks still satisfy the interface arbos expects
var _ arbos.SequencingHooks = (*streamingSequencingHooks)(nil)
