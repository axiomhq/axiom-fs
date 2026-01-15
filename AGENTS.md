# AGENTS.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

axiom-fs exposes Axiom datasets as a filesystem via NFS. File paths compile deterministically to APL queries - no LLMs, just a grammar-based path-to-APL compiler.

**Key concept:** Reading a file executes a query. The path `/logs/q/range/ago/1h/where/status>=500/result.csv` compiles to APL and returns query results.

## Build & Development Commands

```bash
# Go backend
go build ./cmd/axiom-fs          # Build CLI
go test ./...                    # Run all tests
go test -race -v ./...           # Run tests with race detector (CI default)
go vet ./...                     # Static analysis
golangci-lint run                # Lint (uses .golangci.yml, v2.4.0+)

# Run a single test
go test -v -run TestCompileSegments_RangeAgo ./internal/compiler

# macOS app (Swift)
cd macos-app/AxiomKit && swift test                    # Test AxiomKit package
cd macos-app && swiftlint --strict --config .swiftlint.yml  # Lint Swift
cd macos-app/AxiomFS && xcodebuild -scheme AxiomFS -configuration Release  # Build app
```

## Running Locally

```bash
export AXIOM_TOKEN=...
export AXIOM_ORG_ID=...  # only for personal tokens

./axiom-fs --listen 127.0.0.1:2049

# Mount (macOS/Linux)
sudo mount -t nfs -o vers=3,tcp,port=2049,mountport=2049 127.0.0.1:/ /mnt/axiom
```

## Architecture

### Layer Structure

```
cmd/axiom-fs/main.go     Entry point, flag parsing, NFS server setup
internal/
  nfsfs/                 Billy filesystem adapter for go-nfs
  vfs/                   Virtual filesystem nodes (Root, DatasetDir, QueryPathDir, etc.)
  compiler/              Path-to-APL compiler (deterministic grammar)
  query/                 Query execution with caching and spill-to-disk
  cache/                 LRU cache with TTL and size bounds
  axiomclient/           Axiom API wrapper
  config/                Configuration with env var support (AXIOM_FS_ prefix)
  store/                 Raw APL query persistence (_queries/)
  presets/               Built-in preset query templates
```

### Core Flow

1. **NFS request** → `nfsfs.FS.resolve()` walks path through VFS nodes
2. **Path parsing** → `vfs.QueryPathDir.Lookup()` builds segment list
3. **Compilation** → `compiler.CompileSegments()` converts path to APL
4. **Execution** → `query.Executor` runs query (with singleflight dedup + cache)
5. **Response** → Results encoded as ndjson/csv/json, spills to disk if large

### Mount Layout

```
/mnt/axiom/
  README.txt, datasets/, examples/, _presets/, _queries/
  <dataset>/
    schema.json, schema.csv, sample.ndjson
    fields/<field>/top.csv, histogram.csv
    presets/
    q/  <- Query path builder
```

### Query Path Grammar

Each segment maps to one APL operator. Expressions are URL-encoded or base64url-encoded.

| Segment | APL |
|---------|-----|
| `range/ago/1h/` | `where _time between (ago(1h) .. now())` |
| `range/from/<iso>/to/<iso>/` | `where _time between (datetime(...) .. datetime(...))` |
| `where/<expr>/` | `where <expr>` |
| `search/<term>/` | `search "<term>"` |
| `summarize/<agg>/by/<fields>/` | `summarize <agg> by <fields>` |
| `project/<fields>/` | `project <fields>` |
| `order/<field>:<dir>/` | `order by <field> <dir>` |
| `limit/<n>/` | `take <n>` |
| `result.<ext>` | Output format (ndjson/csv/json) |

### Safety Defaults

- Time range: `ago(1h)` injected if missing
- Row limit: 10,000 (configurable, max 100,000)
- Large results spill to disk instead of exhausting RAM
- Singleflight prevents duplicate concurrent queries

### macOS App (macos-app/)

SwiftUI menu bar app wrapping the Go NFS server:
- `AxiomKit/` - Swift package: API client, keychain, config parsing
- `AxiomFS/` - Xcode project: menu bar UI, process management

**Not yet implemented:** File Provider extension (planned for native Finder integration, see docs/MACOS_APP_PLAN.md).

## Testing Patterns

The compiler has extensive table-driven tests in `internal/compiler/compiler_test.go`:
- Segment parsing and APL generation
- Error cases (missing args, invalid formats)
- Range/limit constraint enforcement
- URL and base64 encoding

Integration tests in `internal/integration/` test the full NFS stack against a mock Axiom API.

## Configuration

All flags have `AXIOM_FS_` env var equivalents. Key options:

| Flag | Default | Description |
|------|---------|-------------|
| `--listen` | 127.0.0.1:2049 | NFS server address |
| `--default-range` | 1h | Default time range |
| `--default-limit` | 10000 | Default row limit |
| `--max-limit` | 100000 | Maximum allowed limit |
| `--max-range` | 24h | Maximum allowed range |
| `--cache-ttl` | 60s | Query result cache TTL |
