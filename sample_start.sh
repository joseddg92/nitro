#!/bin/bash
set -euo pipefail

# =============================================================================
# Nitro node launcher — tuned for:
#   Host   : AWS EC2 (KVM), Intel Xeon Platinum 8559C @ ~3.2GHz, 4 vCPU
#            (1 socket, 2 cores, HT on -> 4 threads; AVX-512F/DQ/VL/BW/CD/VNNI,
#            AVX2, BMI1/2, FMA all present -- see `lscpu`)
#   RAM    : 30 GiB, dedicated to this node
#   Role   : full RPC node (staker disabled, forwards txs to the sequencer)
#   Scheme : pathdb (non-archive)
#
# Tuning follows:
#   https://docs.arbitrum.io/run-arbitrum-node/nitro/node-tuning-and-monitoring
#
# Everything below the memory-budget block is aimed specifically at driving
# down arb/block/sendipc (execute + receipt-IPC-publish latency, see
# arb/block/waittime for the queueing time this deliberately excludes). Cache
# sizes are a reasoned starting point, not a benchmarked optimum -- this host
# has no synthetic load generator, so re-tune them by watching
# arb/block/execution and arb/block/sendipc on the real feed and adjusting
# from there rather than trusting these numbers blindly.
# =============================================================================

CHAIN_INFO="$(jq -r '.chain."info-json"' /home/robinhood/robinhood-nodeConfig.json)"
CHAIN_NAME="$(jq -r '.chain.name'      /home/robinhood/robinhood-nodeConfig.json)"

# -----------------------------------------------------------------------------
# Memory budget (all values in MiB)
# -----------------------------------------------------------------------------
#   Pebble block cache   (CGO)        database-cache            2048
#   Pebble memtables     (CGO)        database-cache / 2        1024
#   Trie-clean cache     (mmap)       trie-clean-cache          1536  (was 1024)
#   Snapshot cache       (mmap)       snapshot-cache            1024  (was  512)
#   Stylus WASM cache    (Rust/CGO)   stylus-lru-cache-capacity  384  (was  256)
#   glibc malloc arenas               MALLOC_ARENA_MAX=2         128
#   Native thread stacks                                         300
#                                                             ------
#   Non-Go total                                                6444 MiB
#
#   GOMEMLIMIT ............................................... 16384 MiB
#   Worst-case Nitro RSS ~ 16384 + 6444 ....................  ~22.3 GiB
#   Left for kernel, page cache, sshd, agents ...............   ~7.7 GiB
#
# This is deliberately *below* what the doc's container formula would give
# (~24 GiB). On bare metal there is no cgroup limit to protect us, and the OS
# page cache in front of EBS is worth more on a 4-vCPU box than a larger Go heap.
#
# snapshot-cache and trie-clean-cache were raised from their previous values:
# a snapshot-cache hit answers a state read in one lookup, where a miss falls
# through to a multi-level trie walk -- state reads inside ProduceBlockAdvanced
# are the dominant cost of arb/block/execution (and therefore arb/block/sendipc),
# so a higher hit rate there is the most direct lever this file has over it.
# Kept modest (not maxed out) to preserve OS page-cache headroom in front of the
# persistent.chain disk, which matters just as much on a 4-vCPU box.
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

# Pinned explicitly rather than left to auto-detection: `nproc` already
# resolves to 4 here, but this host is a KVM guest, and pinning removes any
# dependency on that continuing to be true if the guest's vCPU count or cgroup
# view ever changes underneath the script. (The x2 multiplier in the docs is a
# validator-only recommendation; this node is not a validator.)
export GOMAXPROCS=4

# Skip the runtime's pointer-passing safety checks on cgo calls. Every block
# with a Stylus call or a brotli/BLS operation crosses into C/Rust through cgo;
# cgocheck's scan is pure overhead once that code is trusted, and it runs
# inline on the same goroutine doing the call -- i.e. inside arb/block/execution.
export GODEBUG=cgocheck=0

# Best-effort: ask the kernel to prefer scheduling this process's threads over
# others on the box (e.g. this script's own `jq` calls, sshd, agents) when all
# 4 vCPUs are momentarily contended, so scheduling delay doesn't show up as
# arb/block/waittime or lengthen arb/block/execution. Silently a no-op without
# CAP_SYS_NICE / root -- not worth failing startup over.
renice -n -5 -p $$ >/dev/null 2>&1 || true

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
  --execution.caching.trie-clean-cache=1536 \
  --execution.caching.snapshot-cache=1024 \
  --execution.caching.stylus-lru-cache-capacity=384 \
  --execution.caching.state-scheme=path \
  `# pathdb forces a flush every N blocks; at ~10 blocks/s the default 128 put a` \
  `# ~100ms writetodb stall every ~13s. 32 trades more frequent, smaller flushes` \
  `# for a shorter tail -- the volume is at <1% utilisation, so it has the room.` \
  --execution.caching.pathdb-max-diff-layers=32 \
  \
  --persistent.handles=2048 \
  \
  `# ---------- tx indexer: stop it spinning on the tail key ----------` \
  `# Profiled (120s, warm node): rawdb.ReadTxIndexTail was 23.9% of ALL node CPU` \
  `# and 85% of every Pebble read, while the profile contained ZERO` \
  `# IndexTransactions/UnindexTransactions samples -- i.e. the indexer was doing` \
  `# no indexing at all. This chain is fully indexed (tail == 0), so txIndexer.run` \
  `# reads the tail key, finds nothing to do, returns; txIndexer.loop then reads` \
  `# the same key again (core/txindexer.go:300). That cycle repeats every` \
  `# min-batch-delay forever, and each read fans out across a 339GB LSM paying a` \
  `# CRC32 verify per sstable block it touches (~45ms of CPU per read).` \
  `# 60s cuts the frequency ~60x. The indexer stays functional -- it just stops` \
  `# asking 'is there anything to do?' once a second.` \
  --execution.tx-indexer.min-batch-delay=60s \
  \
  `# ---------- lowest-latency block pipeline (arb/block/sendipc) ----------` \
  `# Speculatively execute msg+1 in the background while msg is mid-commit, so` \
  `# by the time msg+1 reaches DigestMessage its state reads are already warm.` \
  `# This is the default already; pinned explicitly so an upgrade can't` \
  `# silently flip it off underneath this tuning.` \
  --execution.enable-prefetch-block=true \
  `# Stylus programs are JIT-compiled once per (module, target) and cached; the` \
  `# built-in default target (x86_64-linux-unknown+sse4.2+lzcnt+bmi) is deliberately` \
  `# conservative for portability across arbitrary deploy hosts. This host's actual` \
  `# CPU features (see lscpu / header above) let the compiler emit AVX2/FMA/BMI2 and` \
  `# AVX-512F/DQ/VL directly, which matters for Stylus contracts doing real compute` \
  `# -- that execution happens inside arb/block/execution, same as everything else` \
  `# in ProduceBlockAdvanced. avx512bw/cd/vnni are NOT set: the vendored wasmer` \
  `# CpuFeature enum (crates/tools/wasmer/lib/types/src/target.rs) only models` \
  `# avx512f/dq/vl, so those are the ceiling regardless of what the CPU supports.` \
  --execution.stylus-target.amd64="x86_64-linux-unknown+sse4.2+lzcnt+bmi+bmi2+popcnt+avx+avx2+fma+avx512f+avx512dq+avx512vl" \
  \
  `# ---------- OOM protection: throttle RPC when free RAM is low ----------` \
  --node.resource-mgmt.mem-free-limit=2GB \
  \
  `# ---------- chain / connectivity (unchanged) ----------` \
  --parent-chain.connection.url=https://eth.drpc.org \
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
