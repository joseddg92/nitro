// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

package receiptexporter

import (
	"encoding/binary"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// encodeBlock appends the binary encoding of one block's receipts to dst and
// returns the extended slice. Pass dst[:0] of a reused buffer to avoid
// allocations across calls. The layout is documented in protocol.go.
//
// This is deliberately allocation-light and free of reflection/hex encoding:
// it is meaningfully cheaper to produce than the equivalent JSON.
func encodeBlock(dst []byte, block *types.Block, receipts types.Receipts) []byte {
	le := binary.LittleEndian
	dst = append(dst, RecordBlock)
	dst = le.AppendUint64(dst, block.NumberU64())
	dst = append(dst, block.Hash().Bytes()...)
	dst = append(dst, block.ParentHash().Bytes()...)
	dst = le.AppendUint64(dst, block.Time())
	dst = le.AppendUint32(dst, uint32(len(block.Transactions())))
	dst = le.AppendUint32(dst, uint32(len(receipts)))

	// Receipts are index-aligned with transactions (see Receipts.DeriveFields).
	txs := block.Transactions()
	baseFee := block.BaseFee()

	var word [32]byte
	for i, r := range receipts {
		dst = append(dst, r.TxHash.Bytes()...)
		dst = append(dst, byte(r.Status))
		dst = append(dst, r.Type)
		dst = append(dst, r.ContractAddress.Bytes()...)
		dst = le.AppendUint64(dst, r.CumulativeGasUsed)
		dst = le.AppendUint64(dst, r.GasUsed)
		dst = le.AppendUint64(dst, r.GasUsedForL1)

		// EffectiveGasPrice is populated by DeriveFields, which only runs on
		// DB-reloaded receipts; the in-memory receipts we get here have it nil,
		// so derive it cheaply from the base fee and the tx (no signature
		// recovery). Falls back to a zero word if unavailable.
		word = [32]byte{}
		egp := r.EffectiveGasPrice
		if egp == nil && i < len(txs) {
			egp = effectiveGasPrice(txs[i], baseFee)
		}
		if egp != nil {
			egp.FillBytes(word[:]) // big-endian, right-aligned
		}
		dst = append(dst, word[:]...)

		dst = appendLogs(dst, r.Logs)
	}
	return dst
}

// encodeTxLogs appends a RecordTx - one transaction's logs, published from
// inside the block loop as soon as that tx finishes executing, long before the
// block is finalised. Carries only what a log consumer needs: no block hash
// (not computed yet) and no gas figures.
//
// txIndex is the receipt's position in the block and firstLogIndex is the
// block-wide index of this tx's first log, so the consumer can reproduce the
// chain's transactionIndex/logIndex numbering and dedup against the same log
// seen elsewhere. See protocol.go for the layout and the speculative-delivery
// caveat.
func encodeTxLogs(dst []byte, blockNumber uint64, txIndex, firstLogIndex uint32, txHash common.Hash, status uint64, logs []*types.Log) []byte {
	le := binary.LittleEndian
	dst = append(dst, RecordTx)
	dst = le.AppendUint64(dst, blockNumber)
	dst = le.AppendUint32(dst, txIndex)
	dst = le.AppendUint32(dst, firstLogIndex)
	dst = append(dst, txHash.Bytes()...)
	dst = append(dst, byte(status))
	return appendLogs(dst, logs)
}

// appendLogs writes the shared [log_count][logs...] tail used by both records.
func appendLogs(dst []byte, logs []*types.Log) []byte {
	le := binary.LittleEndian
	dst = le.AppendUint32(dst, uint32(len(logs)))
	for _, lg := range logs {
		dst = append(dst, lg.Address.Bytes()...)
		dst = append(dst, byte(len(lg.Topics)))
		for _, t := range lg.Topics {
			dst = append(dst, t.Bytes()...)
		}
		dst = le.AppendUint32(dst, uint32(len(lg.Data)))
		dst = append(dst, lg.Data...)
	}
	return dst
}

// effectiveGasPrice mirrors go-ethereum's receipt derivation:
// min(gasFeeCap, baseFee + gasTipCap). For legacy txs feeCap==tipCap==gasPrice
// so this reduces to gasPrice. With no base fee it is just the gas price.
func effectiveGasPrice(tx *types.Transaction, baseFee *big.Int) *big.Int {
	if baseFee == nil {
		return tx.GasPrice()
	}
	price := new(big.Int).Add(baseFee, tx.GasTipCap())
	if feeCap := tx.GasFeeCap(); price.Cmp(feeCap) > 0 {
		return feeCap
	}
	return price
}
