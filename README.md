# axiom-fs

Mount Axiom datasets as a filesystem. Deterministic paths compile to APL. No LLMs, no magic, just fast, inspectable queries.

## Why it rules
- Files are queries: read a file, get results.
- Deterministic path grammar: every path segment maps to one APL operator.
- Safe defaults: time range and limit are enforced unless explicitly overridden.
- Agent friendly: presets and self-describing layout.
- Raw APL escape hatch: write APL, read results.

## Install

### macOS (macFUSE)
1) Install macFUSE:
   - `brew install --cask macfuse`
2) Enable kernel extensions:
   - Boot into Recovery, open Startup Security Utility.
   - Choose "Reduced Security" and "Allow user management of kernel extensions".
   - Reboot.

### Linux
Install FUSE 3:
```
sudo apt-get install fuse3
```

## Quickstart

Set Axiom credentials:
```
export AXIOM_TOKEN=...
export AXIOM_ORG_ID=... # only for personal tokens
```

Mount:
```
go run ./cmd/axiom-fs --mount /mnt/axiom
```

Peek:
```
ls /mnt/axiom
ls /mnt/axiom/datasets
ls /mnt/axiom/<dataset>/presets
```

## Mount layout

```
/mnt/axiom/
  datasets/
  README.txt
  examples/
  _presets/
  _queries/
  <dataset>/
    schema.json
    schema.csv
    sample.ndjson
    fields/
      <field>/
        top.csv
        histogram.csv
    presets/
    q/
```

## Query paths (q/)

Each segment appends one operator to the pipeline. Order is left to right.

```
range/ago/1h/                    -> where _time between (ago(1h) .. now())
range/from/<iso>/to/<iso>/       -> where _time between (datetime(...) .. datetime(...))
where/<expr>/                    -> where <expr>
search/<term>/                   -> search "<term>"
summarize/<agg>/                 -> summarize <agg>
summarize/<agg>/by/<fields>/     -> summarize <agg> by <fields>
project/<fields>/                -> project <fields>
project-away/<fields>/           -> project-away <fields>
order/<field>:<dir>/             -> order by <field> <dir>
limit/<n>/                       -> take <n>
top/<n>/by/<field>:<dir>/        -> top <n> by <field> <dir>
format/<ndjson|csv|json>/        -> output format
result.<ext>                     -> triggers execution
```

Encoding rules:
- `<expr>` and `<term>`: URL-encode or base64url-encode.
- `<fields>`: comma-separated.

Example:
```
/mnt/axiom/logs/q/range/ago/1h/where/status>=500/summarize/count()/by/service/order/count_:desc/limit/50/result.csv
```

## Presets

Preset results live at:
```
/mnt/axiom/<dataset>/presets/
```

Preset templates and metadata live at:
```
/mnt/axiom/_presets/
```

## Raw APL escape hatch

```
/mnt/axiom/_queries/<name>/apl          # write APL here
/mnt/axiom/_queries/<name>/result.csv   # read results
/mnt/axiom/_queries/<name>/result.error # APL + error details
```

`<name>` must be <= 64 chars and only contain `a-zA-Z0-9-_.`.

## Cache + safety

Defaults:
- time range: `ago(1h) .. now()`
- limit: 10,000 rows

Bounded cache:
- memory cache with max entries/bytes
- on-disk cache with TTL and size bounds

Large result sets spill to disk instead of eating RAM.

## Configuration

Flags are also available as env vars with `AXIOM_FS_` prefix.

```
--mount                 mount point
--default-range         default range for queries (ago duration)
--default-limit         default row limit
--max-limit             max allowed limit
--max-range             max allowed range
--cache-ttl             cache TTL
--cache-max-entries     max cache entries
--cache-max-bytes       max cache size in bytes
--cache-dir             directory for persistent cache
--max-in-memory-bytes   spill to disk after this size
--query-dir             directory for raw APL files
--temp-dir              temp dir for spilled results
--sample-limit          sample.ndjson row count
--axiom-url             API base URL (overrides env)
--axiom-token           API token (overrides env)
--axiom-org             org ID (overrides env)
```

## Troubleshooting

- `no FUSE mount utility found`: macFUSE or fuse3 is not installed or not enabled.
- On macOS: you must allow kernel extensions in Recovery and reboot.
- If reads are empty: check `result.error` for details.

## Development

Run tests:
```
go test ./...
```
