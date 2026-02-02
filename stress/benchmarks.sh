#!/bin/bash
# Benchmark pq commands with hyperfine
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"
PQ="./target/release/pq"

# Check dependencies
if ! command -v hyperfine &> /dev/null; then
    echo "Error: hyperfine not found. Install with: brew install hyperfine"
    exit 1
fi

if [[ ! -f "$PQ" ]]; then
    echo "Building pq in release mode..."
    cargo build --release
fi

if [[ ! -d "$FIXTURES_DIR" ]]; then
    echo "Error: Fixtures not found. Run ./stress/generate.sh first"
    exit 1
fi

echo "=== pq Benchmark Suite ==="
echo ""

# ============================================================================
echo ">>> COUNT (should use metadata only, instant)"
echo ""

if [[ -f "$FIXTURES_DIR/baseline_100k.parquet" ]]; then
    hyperfine --warmup 5 --runs 20 \
        "$PQ count $FIXTURES_DIR/baseline_100k.parquet" \
        --export-markdown "$FIXTURES_DIR/../results/count_100k.md" 2>/dev/null || \
    hyperfine --warmup 5 --runs 20 "$PQ count $FIXTURES_DIR/baseline_100k.parquet"
fi

if [[ -f "$FIXTURES_DIR/huge_100m.parquet" ]]; then
    echo ""
    hyperfine --warmup 3 --runs 10 \
        "$PQ count $FIXTURES_DIR/huge_100m.parquet"
fi

# ============================================================================
echo ""
echo ">>> HEAD (streaming, should be fast regardless of file size)"
echo ""

if [[ -f "$FIXTURES_DIR/baseline_100k.parquet" ]]; then
    hyperfine --warmup 5 \
        "$PQ head -n 10 $FIXTURES_DIR/baseline_100k.parquet" \
        "$PQ head -n 100 $FIXTURES_DIR/baseline_100k.parquet" \
        "$PQ head -n 1000 $FIXTURES_DIR/baseline_100k.parquet"
fi

if [[ -f "$FIXTURES_DIR/huge_100m.parquet" ]]; then
    echo ""
    echo "Head on 100M row file:"
    hyperfine --warmup 3 --runs 5 \
        "$PQ head -n 10 $FIXTURES_DIR/huge_100m.parquet" \
        "$PQ head -n 1000 $FIXTURES_DIR/huge_100m.parquet" \
        "$PQ head -n 10000 $FIXTURES_DIR/huge_100m.parquet"
fi

# ============================================================================
echo ""
echo ">>> TAIL (full file scan required)"
echo ""

if [[ -f "$FIXTURES_DIR/medium_1m.parquet" ]]; then
    hyperfine --warmup 2 --runs 5 \
        "$PQ tail -n 10 $FIXTURES_DIR/medium_1m.parquet"
fi

if [[ -f "$FIXTURES_DIR/large_10m.parquet" ]]; then
    echo ""
    echo "Tail on 10M row file:"
    hyperfine --warmup 1 --runs 3 \
        "$PQ tail -n 10 $FIXTURES_DIR/large_10m.parquet"
fi

# ============================================================================
echo ""
echo ">>> SCHEMA (metadata only)"
echo ""

if [[ -f "$FIXTURES_DIR/wide_1000col.parquet" ]]; then
    hyperfine --warmup 5 \
        "$PQ schema $FIXTURES_DIR/wide_1000col.parquet"
fi

# ============================================================================
echo ""
echo ">>> STATS (row group iteration)"
echo ""

if [[ -f "$FIXTURES_DIR/medium_1m.parquet" ]]; then
    hyperfine --warmup 2 --runs 5 \
        "$PQ stats $FIXTURES_DIR/medium_1m.parquet"
fi

if [[ -f "$FIXTURES_DIR/large_10m.parquet" ]]; then
    echo ""
    hyperfine --warmup 1 --runs 3 \
        "$PQ stats $FIXTURES_DIR/large_10m.parquet"
fi

# ============================================================================
echo ""
echo ">>> INFO (metadata only)"
echo ""

if [[ -f "$FIXTURES_DIR/huge_100m.parquet" ]]; then
    hyperfine --warmup 5 \
        "$PQ info $FIXTURES_DIR/huge_100m.parquet"
fi

# ============================================================================
echo ""
echo ">>> QUERY (DataFusion SQL engine)"
echo ""

if [[ -f "$FIXTURES_DIR/large_10m.parquet" ]]; then
    hyperfine --warmup 2 --runs 5 \
        "$PQ query 'SELECT COUNT(*) FROM t' $FIXTURES_DIR/large_10m.parquet"

    echo ""
    hyperfine --warmup 2 --runs 3 \
        "$PQ query 'SELECT bool_3, COUNT(*), AVG(int_0) FROM t GROUP BY bool_3' $FIXTURES_DIR/large_10m.parquet"
fi

# ============================================================================
echo ""
echo ">>> OUTPUT FORMATS"
echo ""

if [[ -f "$FIXTURES_DIR/baseline_100k.parquet" ]]; then
    hyperfine --warmup 3 \
        "$PQ head -n 10000 $FIXTURES_DIR/baseline_100k.parquet -o table > /dev/null" \
        "$PQ head -n 10000 $FIXTURES_DIR/baseline_100k.parquet -o json > /dev/null" \
        "$PQ head -n 10000 $FIXTURES_DIR/baseline_100k.parquet -o jsonl > /dev/null" \
        "$PQ head -n 10000 $FIXTURES_DIR/baseline_100k.parquet -o csv > /dev/null"
fi

# ============================================================================
echo ""
echo ">>> GLOB PATTERNS"
echo ""

if [[ -d "$FIXTURES_DIR/many" ]]; then
    hyperfine --warmup 2 --runs 5 \
        "$PQ count '$FIXTURES_DIR/many/*.parquet'"
fi

echo ""
echo "=== Benchmark complete ==="
