// Copyright 2026, Offchain Labs, Inc.
// For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md

// Command receiptexporter-demo is a throwaway end-to-end harness: it starts the
// real receiptexporter on a socket and publishes synthetic block receipts so a
// consumer (e.g. scripts/receipt_reader.py) can be exercised without a full node.
package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/offchainlabs/nitro/execution/gethexec/receiptexporter"
)

func synthBlock(n uint64) (*types.Block, types.Receipts) {
	header := &types.Header{
		Number:     new(big.Int).SetUint64(n),
		Time:       uint64(time.Now().Unix()),
		ParentHash: common.BigToHash(big.NewInt(int64(n - 1))),
		GasLimit:   30_000_000,
		GasUsed:    42_000,
		BaseFee:    big.NewInt(100_000_000),
	}
	block := types.NewBlockWithHeader(header)

	r := &types.Receipt{
		Type:              types.DynamicFeeTxType,
		Status:            types.ReceiptStatusSuccessful,
		CumulativeGasUsed: 21000,
		GasUsed:           21000,
		GasUsedForL1:      3000,
		TxHash:            common.BigToHash(big.NewInt(int64(n*1000 + 1))),
		EffectiveGasPrice: big.NewInt(100_000_000),
		BlockNumber:       new(big.Int).SetUint64(n),
		BlockHash:         block.Hash(),
		TransactionIndex:  0,
		Logs: []*types.Log{{
			Address: common.HexToAddress("0x00000000000000000000000000000000000000ff"),
			Topics:  []common.Hash{common.BigToHash(big.NewInt(7))},
			Data:    []byte{0xde, 0xad, 0xbe, 0xef},
		}},
	}
	return block, types.Receipts{r}
}

func main() {
	socketPath := "/e2e/receipts.sock"
	if len(os.Args) > 1 {
		socketPath = os.Args[1]
	}
	cfg := receiptexporter.DefaultConfig
	cfg.SocketPath = socketPath
	cfg.RingSize = 1 << 20 // 1 MiB is plenty for the demo

	exp, err := receiptexporter.New(&cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "new:", err)
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := exp.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "start:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "demo exporter on %s; publishing 1 block/sec\n", socketPath)

	var n uint64 = 1
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			exp.StopAndWait()
			return
		case <-ticker.C:
			block, receipts := synthBlock(n)
			exp.PublishBlockReceipts(block, receipts)
			n++
		}
	}
}
