#!/bin/bash
set -euo pipefail

# =============================================================================
# Nitro node launcher — tuned for:
#   Host   : AWS EC2, Intel Xeon E5-2686 v4 @ 2.30GHz, 4 vCPU (2 cores + HT)
#   RAM    : 30 GiB, dedicated to this node
#   Role   : full RPC node (staker disabled, forwards txs to the sequencer)
#   Scheme : pathdb (non-archive)
#
# Tuning follows:
#   https://docs.arbitrum.io/run-arbitrum-node/nitro/node-tuning-and-monitoring
# =============================================================================

CHAIN_INFO="$(jq -r '.chain."info-json"' /home/robinhood/robinhood-nodeConfig.json)"
CHAIN_NAME="$(jq -r '.chain.name'      /home/robinhood/robinhood-nodeConfig.json)"

# -----------------------------------------------------------------------------
# Memory budget (all values in MiB)
# -----------------------------------------------------------------------------
#   Pebble block cache   (CGO)        database-cache            2048
#   Pebble memtables     (CGO)        database-cache / 2        1024
#   Trie-clean cache     (mmap)       trie-clean-cache          1024
#   Snapshot cache       (mmap)       snapshot-cache             512
#   Stylus WASM cache    (Rust/CGO)   stylus-lru-cache-capacity  256   (default)
#   glibc malloc arenas               MALLOC_ARENA_MAX=2         128
#   Native thread stacks                                         300
#                                                             ------
#   Non-Go total                                                5292 MiB
#
#   GOMEMLIMIT ............................................... 16384 MiB
#   Worst-case Nitro RSS ~ 16384 + 5292 ....................  ~21.2 GiB
#   Left for kernel, page cache, sshd, agents ...............   ~8.8 GiB
#
# This is deliberately *below* what the doc's container formula would give
# (~24 GiB). On bare metal there is no cgroup limit to protect us, and the OS
# page cache in front of EBS is worth more on a 4-vCPU box than a larger Go heap.
# -----------------------------------------------------------------------------

# Cap glibc malloc arenas. Default is 8 x CPU_count (= 32 here) x 64 MiB = 2 GiB
# of pure arena overhead that shows up as slow, unexplained RSS growth.
export MALLOC_ARENA_MAX=2

# Soft ceiling for the Go heap only. Non-Go memory above is NOT counted by Go.
export GOMEMLIMIT=16384MiB

# GC frequency. Raised from the default 100 after measuring: live heap sits at
# ~1.2 GiB against a 16 GiB GOMEMLIMIT with ~26 GiB free, while the node churns
# ~70M allocs/sec -- so at GOGC=100 the collector was running constantly for no
# reason, adding jitter to the block pipeline. 400 collects at ~5x live heap;
# GOMEMLIMIT remains the real backstop. Do NOT lower this below 100.
export GOGC=400

# GOMAXPROCS deliberately unset. Go auto-detects 4 logical CPUs, which is
# correct for a bare-metal main node. (The x2 multiplier in the docs is a
# validator-only recommendation; this node is not a validator.)

# Raise the file-descriptor limit as far as the hard limit allows (Pebble keeps
# many SSTs open). Never fatal if the hard limit is low.
HARD_NOFILE="$(ulimit -Hn)"
[ "$HARD_NOFILE" = "unlimited" ] && HARD_NOFILE=1048576
ulimit -n "$HARD_NOFILE" 2>/dev/null || true

# RPC bind address. 0.0.0.0 exposes this node to anything that can reach the
# instance — keep the security group tight, or set this to 127.0.0.1 / the
# private IP and terminate TLS + auth in front of it.
RPC_BIND="0.0.0.0"

RPC_BIND="127.0.0.1"

exec /home/robinhood/nitro \
  `# ---------- caches (sized for 30 GiB, non-archive) ----------` \
  --execution.caching.database-cache=2048 \
  --execution.caching.trie-dirty-cache=1024 \
  --execution.caching.trie-clean-cache=1024 \
  --execution.caching.snapshot-cache=512 \
  --execution.caching.stylus-lru-cache-capacity=256 \
  --execution.caching.state-scheme=path \
  `# pathdb forces a flush every N blocks; at ~10 blocks/s the default 128 put a` \
  `# ~100ms writetodb stall every ~13s. 32 trades more frequent, smaller flushes` \
  `# for a shorter tail -- the volume is at <1% utilisation, so it has the room.` \
  --execution.caching.pathdb-max-diff-layers=32 \
  \
  --persistent.handles=2048 \
  \
  `# ---------- OOM protection: throttle RPC when free RAM is low ----------` \
  --node.resource-mgmt.mem-free-limit=2GB \
  \
  `# ---------- chain / connectivity (unchanged) ----------` \
  --parent-chain.connection.url=https://eth1.lava.build/ \
  --parent-chain.blob-client.beacon-url=https://eth-mainnetbeacon.g.alchemy.com/v2/Oz73zu-SDlEhvQKmY-lusVERM6_tne9k \
  --chain.info-json="$CHAIN_INFO" \
  --chain.name="$CHAIN_NAME" \
  --persistent.chain=/home/robinhood/.arbitrum/robinhoodchain \
  --init.genesis-json-file=/home/robinhood/robinhood-genesis.json \
  --node.feed.input.url=wss://feed.mainnet.chain.robinhood.com,wss://feed.mainnet.chain.robinhood.com,wss://feed.mainnet.chain.robinhood.com,wss://feed.mainnet.chain.robinhood.com,wss://feed.mainnet.chain.robinhood.com,wss://localhost:1335 \
  --execution.forwarding-target=https://sequencer.mainnet.chain.robinhood.com \
  --node.staker.enable=false \
  \
  `# ---------- Prometheus metrics (scraped by the local monitoring stack) ----------` \
  `# geth serves the registry at /debug/metrics/prometheus, NOT /metrics.` \
  `# 127.0.0.1 only: Prometheus runs on the host network and scrapes it locally.` \
  --metrics \
  --metrics-server.addr=127.0.0.1 \
  --metrics-server.port=6070 \
  \
  `# ---------- RPC ----------` \
  --http.addr="$RPC_BIND" \
  --http.port=28547 \
  --http.api=eth,net,web3,arb,debug,txpool \
  --http.corsdomain='*' \
  --http.vhosts='*' \
  --ws.addr="$RPC_BIND" \
  --ws.port=28548 \
  --ws.origins='*' \
  --ws.api=eth,net,web3,arb,txpool \
  --ipc.path=/home/robinhood/nitro.ipc \
  --execution.receipt-export.socket-path /home/robinhood/fast-receipts.ipc
