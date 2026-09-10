#!/usr/bin/env bash
# Copyright 2026, Offchain Labs, Inc.
# For license information, see https://github.com/OffchainLabs/nitro/blob/master/LICENSE.md
#
# Builds ./target/bin/nitro (and the rest of `make build`'s targets) as
# aggressively optimized for the machine this script runs on as the toolchain
# allows:
#
#   - Go:   GOAMD64 set to the highest microarchitecture level (v1-v4) this
#           CPU actually supports, auto-detected from /proc/cpuinfo. There is
#           no Go equivalent of "-march=native" - GOAMD64 must be told an
#           explicit level, and setting one the CPU can't run crashes with
#           SIGILL at the first instruction that needs it. This is the whole
#           reason this is a script and not just an env-var line in a README.
#   - C/C++ (brotli, cgo boundaries): CFLAGS/CXXFLAGS/CGO_*FLAGS=-march=native -O3.
#   - Rust  (prover, jit, stylus libs): RUSTFLAGS=-C target-cpu=native.
#
# gcc/clang/rustc's own "native" support means those three need no detection
# logic - they ask the local CPU directly. Go's GOAMD64 does not have that
# option, hence the function below.
#
# ============================================================================
# IMPORTANT: THE RESULTING BINARY IS TIED TO THIS MACHINE'S CPU.
# ============================================================================
# A binary built here with, say, GOAMD64=v4 and AVX-512 code paths will crash
# with "illegal instruction" on any machine that lacks AVX-512 - including an
# older sibling in the same fleet. Do not copy target/bin/nitro to a different
# host. Run this script ON each machine you deploy to; it is meant to be
# cloned alongside the repo and re-run per host, not built once and shipped.
#
# Usage:
#   scripts/build-native-optimized.sh                 # bootstrap + build
#   scripts/build-native-optimized.sh --skip-bootstrap # build only, toolchains
#                                                       # already installed
#   scripts/build-native-optimized.sh --target foo     # `make <target>`
#                                                       # instead of `make build`
#
# Safe to re-run: every bootstrap step checks what is already present first.

set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$PWD"

SKIP_BOOTSTRAP=false
MAKE_TARGET="build"
while [ $# -gt 0 ]; do
	case "$1" in
	--skip-bootstrap)
		SKIP_BOOTSTRAP=true
		shift
		;;
	--target)
		MAKE_TARGET="$2"
		shift 2
		;;
	-h | --help)
		sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
		exit 0
		;;
	*)
		echo "unknown argument: $1" >&2
		exit 1
		;;
	esac
done

GO_VERSION=1.25.9
RUST_VERSION=1.93.0
CBINDGEN_VERSION=0.29.2
NODE_MAJOR=22
FOUNDRY_VERSION=1.2.3
WABT_VERSION=1.0.37

log() { printf '\033[38;5;161;1m==>\033[0;0m %s\n' "$*"; }
warn() { printf '\033[33;1m!!\033[0;0m %s\n' "$*" >&2; }

# ----------------------------------------------------------------------------
# 0. Disk space sanity check.
#
# Worth checking explicitly: a from-scratch build (Go module cache, cargo
# registry + target dir, node_modules, forge/solc artifacts, the wasm-libs
# docker stage) comfortably needs 10+ GiB, and a small root volume filling up
# mid-build fails in confusing ways deep inside a subprocess rather than here.
# ----------------------------------------------------------------------------
check_disk_space() {
	local avail_kb
	avail_kb=$(df --output=avail -k "$REPO_ROOT" | tail -1)
	local avail_gib=$((avail_kb / 1024 / 1024))
	if [ "$avail_gib" -lt 10 ]; then
		warn "only ${avail_gib} GiB free on the filesystem holding $REPO_ROOT."
		warn "A full build wants ~10+ GiB (Go/cargo/node caches, build artifacts)."
		warn "If there's a bigger data volume on this host, clone/build there instead,"
		warn "or relocate caches after the fact (mv + symlink: ~/.cache, ~/go, ~/.cargo,"
		warn "~/.rustup are the usual big ones)."
	else
		log "disk: ${avail_gib} GiB free on $REPO_ROOT - OK"
	fi
}

# ----------------------------------------------------------------------------
# 1. GOAMD64 detection.
#
# Levels and their required flags per Go's own definition
# (https://go.dev/wiki/MinimumRequirements#amd64). Checked from v4 down;
# the first fully-satisfied level wins. Flag names match /proc/cpuinfo's
# underscore style (sse4_1, not sse4.1).
# ----------------------------------------------------------------------------
detect_goamd64() {
	local flags
	flags=" $(grep -m1 '^flags' /proc/cpuinfo | cut -d: -f2) "
	has() { case "$flags" in *" $1 "*) return 0 ;; *) return 1 ;; esac; }
	local v4=(avx512f avx512bw avx512cd avx512dq avx512vl)
	local v3=(avx avx2 bmi1 bmi2 f16c fma movbe)
	local v2=(cx16 popcnt sse3 sse4_1 sse4_2 ssse3)
	local all
	all() {
		local f
		for f in "$@"; do has "$f" || return 1; done
		return 0
	}
	if all "${v4[@]}"; then
		echo v4
	elif all "${v3[@]}"; then
		echo v3
	elif all "${v2[@]}"; then
		echo v2
	else
		echo v1
	fi
}

# ----------------------------------------------------------------------------
# 2. Toolchain bootstrap. Skipped entirely with --skip-bootstrap.
#
# Package-manager support: dnf/yum (Amazon Linux, RHEL, Fedora) and apt
# (Debian, Ubuntu). Everything else gets a clear message naming what's
# missing rather than a guess at how to install it.
# ----------------------------------------------------------------------------
PKG_MGR=""
detect_pkg_mgr() {
	if command -v dnf >/dev/null 2>&1; then
		PKG_MGR=dnf
	elif command -v yum >/dev/null 2>&1; then
		PKG_MGR=yum
	elif command -v apt-get >/dev/null 2>&1; then
		PKG_MGR=apt
	else
		PKG_MGR=""
	fi
}

pkg_install() {
	case "$PKG_MGR" in
	dnf | yum) sudo "$PKG_MGR" install -y "$@" ;;
	apt) sudo apt-get update -qq && sudo apt-get install -y "$@" ;;
	*)
		warn "no supported package manager detected; install manually: $*"
		return 1
		;;
	esac
}

ensure_system_packages() {
	log "system packages (gcc, cmake, clang, git, make)"
	local missing=()
	for bin in gcc cmake clang git make curl tar; do
		command -v "$bin" >/dev/null 2>&1 || missing+=("$bin")
	done
	if [ ${#missing[@]} -eq 0 ]; then
		log "  already present"
		return
	fi
	case "$PKG_MGR" in
	dnf | yum) pkg_install gcc gcc-c++ cmake clang git make curl tar ;;
	apt) pkg_install build-essential cmake clang git make curl tar ;;
	*) warn "missing: ${missing[*]} - install these manually, then re-run" ;;
	esac
}

ensure_go() {
	if command -v go >/dev/null 2>&1 && go version 2>/dev/null | grep -q "go${GO_VERSION}"; then
		log "go ${GO_VERSION} already installed"
		return
	fi
	log "installing go ${GO_VERSION}"
	local arch
	case "$(uname -m)" in
	x86_64) arch=amd64 ;;
	aarch64) arch=arm64 ;;
	*)
		warn "unsupported arch $(uname -m) for the go.dev tarball; install go ${GO_VERSION} manually"
		return 1
		;;
	esac
	curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-${arch}.tar.gz" -o /tmp/go.tar.gz
	sudo rm -rf /usr/local/go
	sudo tar -C /usr/local -xzf /tmp/go.tar.gz
	rm -f /tmp/go.tar.gz
	export PATH="/usr/local/go/bin:$PATH"
}

ensure_rust() {
	if command -v rustc >/dev/null 2>&1 && rustc --version 2>/dev/null | grep -q "$RUST_VERSION"; then
		log "rust ${RUST_VERSION} already installed"
	else
		log "installing rust ${RUST_VERSION}"
		local native_target
		case "$(uname -m)" in
		x86_64) native_target=x86_64-unknown-linux-gnu ;;
		aarch64) native_target=aarch64-unknown-linux-gnu ;;
		*)
			warn "unsupported arch $(uname -m) for the rustup native target; install rust ${RUST_VERSION} manually"
			return 1
			;;
		esac
		curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |
			sh -s -- -y --default-toolchain "$RUST_VERSION" --profile default \
				--target "${native_target},wasm32-unknown-unknown,wasm32-wasip1"
	fi
	# shellcheck source=/dev/null
	[ -f "$HOME/.cargo/env" ] && source "$HOME/.cargo/env"
	export PATH="$HOME/.cargo/bin:$PATH"
	if ! command -v cbindgen >/dev/null 2>&1 || ! cbindgen --version 2>/dev/null | grep -q "$CBINDGEN_VERSION"; then
		log "installing cbindgen ${CBINDGEN_VERSION}"
		cargo install --force cbindgen --version "$CBINDGEN_VERSION"
	fi
}

ensure_node_yarn() {
	if command -v node >/dev/null 2>&1 && command -v yarn >/dev/null 2>&1; then
		log "node/yarn already installed ($(node --version))"
		return
	fi
	log "installing node ${NODE_MAJOR}.x + yarn"
	case "$PKG_MGR" in
	dnf | yum)
		curl -fsSL "https://rpm.nodesource.com/setup_${NODE_MAJOR}.x" | sudo bash
		pkg_install nodejs
		;;
	apt)
		curl -fsSL "https://deb.nodesource.com/setup_${NODE_MAJOR}.x" | sudo bash
		pkg_install nodejs
		;;
	*)
		warn "install node ${NODE_MAJOR}.x manually, then: npm install -g yarn"
		return 1
		;;
	esac
	sudo npm install -g yarn
}

ensure_foundry() {
	export PATH="$HOME/.foundry/bin:$PATH"
	if command -v forge >/dev/null 2>&1 && forge --version 2>/dev/null | grep -q "$FOUNDRY_VERSION"; then
		log "foundry ${FOUNDRY_VERSION} already installed"
		return
	fi
	log "installing foundry ${FOUNDRY_VERSION}"
	curl -fsSL https://foundry.paradigm.xyz | bash
	export PATH="$HOME/.foundry/bin:$PATH"
	foundryup -i "$FOUNDRY_VERSION"
}

ensure_wabt() {
	if command -v wat2wasm >/dev/null 2>&1; then
		log "wabt already installed"
		return
	fi
	log "installing wabt ${WABT_VERSION}"
	if [ "$PKG_MGR" = apt ] && pkg_install wabt 2>/dev/null; then
		return
	fi
	# dnf/yum have no wabt package; use upstream's prebuilt release. Only
	# published for a handful of platforms - Ubuntu-built binaries also run
	# fine on other glibc-based x86_64 Linux, which covers Amazon Linux/RHEL.
	if [ "$(uname -m)" != x86_64 ]; then
		warn "no wabt package for $PKG_MGR and no prebuilt release for $(uname -m);"
		warn "install wat2wasm manually (https://github.com/WebAssembly/wabt)"
		return 1
	fi
	local url="https://github.com/WebAssembly/wabt/releases/download/${WABT_VERSION}/wabt-${WABT_VERSION}-ubuntu-20.04.tar.gz"
	curl -fsSL "$url" -o /tmp/wabt.tar.gz
	tar -C /tmp -xzf /tmp/wabt.tar.gz
	sudo cp "/tmp/wabt-${WABT_VERSION}/bin/"* /usr/local/bin/
	rm -rf /tmp/wabt.tar.gz "/tmp/wabt-${WABT_VERSION}"
}

# ----------------------------------------------------------------------------
# 3. Stale brotli marker guard.
#
# `make build` tracks the compiled C brotli libraries with empty marker files
# (.make/cbrotli-lib, .make/cbrotli-wasm). The Makefile recipe only rebuilds
# brotli when the *marker* is missing - it never re-checks that the .a files
# the marker stands for are still on disk. A `make clean`, a wiped/partial
# target/ dir, or an interrupted earlier build can leave the marker behind
# with no libraries, and then the stylus/jit cargo builds fail to link with:
#   error: could not find native static library `brotlienc-static`
# Drop any marker whose libraries are actually missing so make rebuilds them.
# ----------------------------------------------------------------------------
drop_stale_brotli_markers() {
	local lib_files=(
		target/include/brotli/encode.h
		target/include/brotli/decode.h
		target/lib/libbrotlicommon-static.a
		target/lib/libbrotlienc-static.a
		target/lib/libbrotlidec-static.a
	)
	local wasm_files=(
		target/lib-wasm/libbrotlicommon-static.a
		target/lib-wasm/libbrotlienc-static.a
		target/lib-wasm/libbrotlidec-static.a
	)
	local f
	if [ -f .make/cbrotli-lib ]; then
		for f in "${lib_files[@]}"; do
			if [ ! -f "$f" ]; then
				warn "stale .make/cbrotli-lib (missing $f) - dropping it so make rebuilds brotli"
				rm -f .make/cbrotli-lib
				break
			fi
		done
	fi
	if [ -f .make/cbrotli-wasm ]; then
		for f in "${wasm_files[@]}"; do
			if [ ! -f "$f" ]; then
				warn "stale .make/cbrotli-wasm (missing $f) - dropping it so make rebuilds brotli-wasm"
				rm -f .make/cbrotli-wasm
				break
			fi
		done
	fi
}

# ----------------------------------------------------------------------------
# 4. Recommend a --execution.stylus-target.amd64 value for this CPU.
#
# Runtime configuration, not a build flag - printed as a suggestion for this
# host's sample_start.sh, not applied to anything. The Stylus JIT (wasmer/
# cranelift) only models a subset of x86_64 features regardless of what the
# CPU actually has: sse4.2, lzcnt, bmi, bmi2, popcnt, avx, avx2, fma,
# avx512f, avx512dq, avx512vl (crates/tools/wasmer/lib/types/src/target.rs).
# Anything beyond that (avx512bw/cd/vnni, etc.) has no effect here even if
# the CPU supports it.
# ----------------------------------------------------------------------------
recommend_stylus_target() {
	local flags
	flags=" $(grep -m1 '^flags' /proc/cpuinfo | cut -d: -f2) "
	has() { case "$flags" in *" $1 "*) return 0 ;; *) return 1 ;; esac; }
	# Some hosts (seen on this exact box) report the AMD-style "abm" bit
	# instead of "lzcnt" for the same CPUID feature. Rust's own
	# is_x86_feature_detected!("lzcnt") resolves this correctly via CPUID
	# directly; a plain /proc/cpuinfo grep does not, so it's special-cased
	# here rather than silently under-reporting a real capability.
	has_lzcnt() { has lzcnt || has abm; }
	local candidates=(sse4_2:sse4.2 bmi1:bmi bmi2:bmi2 popcnt:popcnt
		avx:avx avx2:avx2 fma:fma avx512f:avx512f avx512dq:avx512dq avx512vl:avx512vl)
	local out="x86_64-linux-unknown"
	has_lzcnt && out="${out}+lzcnt"
	local c cpuflag wasmerflag
	for c in "${candidates[@]}"; do
		cpuflag="${c%%:*}"
		wasmerflag="${c##*:}"
		has "$cpuflag" && out="${out}+${wasmerflag}"
	done
	echo "$out"
}

# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------
check_disk_space

if [ "$SKIP_BOOTSTRAP" = false ]; then
	detect_pkg_mgr
	ensure_system_packages
	ensure_go
	ensure_rust
	ensure_node_yarn
	ensure_foundry
	ensure_wabt
else
	log "--skip-bootstrap: assuming go/rust/node/yarn/foundry/wabt/cmake/clang are already on PATH"
fi

export PATH="/usr/local/go/bin:$HOME/.cargo/bin:$HOME/.foundry/bin:$PATH"

if [ ! -f go-ethereum/go.mod ] && [ ! -f contracts/package.json ]; then
	log "submodules look uninitialized; running git submodule update --init --recursive"
	git submodule update --init --recursive --depth 1
fi

drop_stale_brotli_markers

GOAMD64_LEVEL="$(detect_goamd64)"
log "CPU microarchitecture level detected: GOAMD64=${GOAMD64_LEVEL}"
if [ "$GOAMD64_LEVEL" = v1 ]; then
	warn "no SSE4.2/AVX2/AVX-512 detected - this may be a constrained VM."
	warn "Building at the safe baseline (v1); -march=native/target-cpu=native"
	warn "for the C/Rust pieces still apply and will still use whatever this"
	warn "CPU actually offers."
fi

export GOAMD64="$GOAMD64_LEVEL"
export CGO_ENABLED=1
export CFLAGS="-march=native -O3"
export CXXFLAGS="-march=native -O3"
export CGO_CFLAGS="-march=native -O3"
export CGO_CXXFLAGS="-march=native -O3"
export RUSTFLAGS="-C target-cpu=native"

JOBS="$(nproc)"
log "building: make -j${JOBS} ${MAKE_TARGET}"
log "  GOAMD64=${GOAMD64}"
log "  CGO_CFLAGS=${CGO_CFLAGS}"
log "  RUSTFLAGS=${RUSTFLAGS}"

NITRO_VERSION="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)-native-${GOAMD64_LEVEL}"
make -j"$JOBS" "$MAKE_TARGET" NITRO_VERSION="$NITRO_VERSION"

echo
log "done."
if [ -x target/bin/nitro ]; then
	log "binary: target/bin/nitro"
	target/bin/nitro --version || true
	echo
	log "build flags actually baked in (go version -m):"
	go version -m target/bin/nitro 2>/dev/null | grep -E 'GOAMD64|CGO_ENABLED|CGO_CFLAGS|vcs\.revision' | sed 's/^/  /'
fi

echo
log "Runtime note (not applied by this script - this is a launch-flag suggestion"
log "for sample_start.sh on THIS host, separate from the build above):"
echo "  --execution.stylus-target.amd64=\"$(recommend_stylus_target)\""
echo
log "Reminder: this binary is tied to this machine's CPU (GOAMD64=${GOAMD64_LEVEL})."
log "Do not copy target/bin/nitro elsewhere - re-run this script on each host instead."
