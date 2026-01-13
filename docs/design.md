# Axiom FUSE FS: Agent-First APL Surface (Design)

## Goal
Expose Axiom datasets through a FUSE mount in a way that is maximally usable by agents without embedding LLMs in the filesystem. The core must be deterministic, discoverable, and simple, while still giving full APL power.

## Elegant Core
A deterministic path-to-APL compiler with strong defaults, plus a small preset library and a raw APL escape hatch.

1) Deterministic path grammar (each segment -> one APL operator)
2) Always-on time filter default (overridable)
3) Preset templates mapped to human-named files
4) Raw APL file for full coverage
5) Self-describing schema and examples in the mount

## Mount Layout

/mnt/axiom/
  datasets/                         # list datasets
  README.txt                        # quick usage + defaults
  examples/                         # canonical patterns
  _presets/                         # built-in preset definitions
  _queries/                         # raw APL escape hatch
  <dataset>/
    schema.json
    schema.csv
    sample.ndjson
    fields/
      <field>/
        top.csv
        histogram.csv
    presets/                        # dataset-specific symlinks to _presets
    q/                              # path-grammar query builder

### Raw APL Escape Hatch
/mnt/axiom/_queries/<name>/
  apl                              # write APL
  result.ndjson
  result.csv
  schema.csv
  stats.json

- Writing `apl` stores the query.
- Reading a result file executes the query and streams output.

## Defaults
- Implicit time window: `_time between (ago(1h) .. now())` unless a time range is specified in the path.
- Default format: `ndjson` unless a result file extension requests otherwise.
- Default limit: 10,000 rows (configurable), unless `limit/` is set.

## Deterministic Path Grammar (q/)
Each segment appends one operator to the pipeline, left to right.

/mnt/axiom/<dataset>/q/
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

Encoding rules:
- <expr> is URL-encoded or base64url-encoded to avoid path parser ambiguity.
- <fields> is comma-separated.
- <term> is URL-encoded.

Example:
/mnt/axiom/logs/q/range/ago/1h/where/status>=500/summarize/count()/by/service/order/count_:desc/limit/50/result.csv

Compiles to:
['logs']
| where _time between (ago(1h) .. now())
| where status >= 500
| summarize count() by service
| order by count_ desc
| take 50

## Presets (Agent-Friendly)
Presets are named, intent-focused queries exposed as files.

/mnt/axiom/_presets/
  errors.csv          # status >= 500
  latency.csv         # percentiles
  traffic.csv         # count over time
  slow-requests.csv   # duration > threshold
  top-endpoints.csv

Dataset-specific mapping:
/mnt/axiom/<dataset>/presets/errors.csv -> _presets/errors.csv (templated with dataset)

Preset definitions are stored as templates:
- template: APL with tokens (e.g., ${DATASET}, ${RANGE})
- metadata: output format, default range, display description

## Preset Packs (OTel, Stripe, Segment)

These are dataset-specific presets that map to the most common questions agents ask. They should be exposed under `/mnt/axiom/<dataset>/presets/` and backed by templates in `_presets/`.

### OTel (traces/logs/metrics)
- `errors.csv`: error rate by service and endpoint
- `latency.csv`: p50/p95/p99 by service and endpoint
- `traffic.csv`: request rate over time (bin_auto)
- `slow-requests.csv`: slow traces with top endpoints
- `dependencies.csv`: service-to-service call volume and latency
- `top-spans.csv`: slowest spans with attributes
- `slo-burn.csv`: error budget burn over time

### Stripe Events
- `payments.csv`: counts by status and method
- `refunds.csv`: refund rate over time
- `disputes.csv`: dispute volume by reason
- `latency.csv`: processing latency percentiles
- `top-customers.csv`: top customers by volume

### Segment Events
- `events.csv`: top event names over time
- `sources.csv`: volume by source and integration
- `schemas.csv`: top fields by event type
- `errors.csv`: delivery failures by destination
- `latency.csv`: ingestion latency percentiles

Each preset should include a short description in metadata so agents can pick the right file without inspecting APL.

## Schema Discovery
/mnt/axiom/<dataset>/schema.csv
/mnt/axiom/<dataset>/schema.json

Implementation:
- If Axiom API supports schema endpoint, use it.
- Otherwise: `['dataset'] | where _time between (...) | getschema`

## Field Discovery
/mnt/axiom/<dataset>/fields/<field>/top.csv
/mnt/axiom/<dataset>/fields/<field>/histogram.csv

Examples:
- `summarize topk(field, 10)`
- `summarize histogram(field, 100)`

## Execution Model
- ReadDir: expose directories, files, and generated entries.
- Open/Read:
  - For static files (README, examples), return content.
  - For query results, compile APL and run Axiom query API.
- Cache:
  - Query results cached by (dataset + APL + format + range) for TTL (default 60s).

## Safety and Performance
- Enforce max time range (configurable) and max rows.
- Force `_time between` in every query (prepend if missing).
- Prefer `has`/`has_cs` over `contains` when transforming presets.
- Surface errors as stderr-like files:
  - `result.error` includes APL + API error.

## Minimal Implementation Steps
1) FUSE skeleton with static README/examples.
2) Implement APL compiler for q/ path grammar.
3) Implement raw APL files in _queries.
4) Add presets with templating.
5) Add schema + field discovery files.

## Example README.txt (In-Mount)
- Most useful files: /<dataset>/presets/*.csv
- For advanced: /<dataset>/q/<...>/result.ndjson
- For anything complex: /_queries/<name>/apl

## Non-Goals
- No LLM inside the FUSE layer.
- No complex query planning or joins via path grammar; use raw APL.

## Test Plan (TDD, World-Class)

### 1) Path -> APL compiler (unit)
- Table-driven tests for every path segment:
  - `range/ago/1h`, `range/from/.../to/...`
  - `where/<expr>`, `search/<term>`
  - `summarize/<agg>/by/<fields>`
  - `project`, `project-away`, `order`, `limit`, `top`
- Golden tests for full pipelines (path -> APL string).
- Encoding tests:
  - URL-encoded and base64url expressions decode cleanly.
  - Invalid encodings return deterministic errors.
- Invariants:
  - `_time between` is always injected if missing.
  - Output format defaults to `ndjson` unless file extension overrides.
  - Deterministic operator ordering.

### 2) Preset rendering (unit + golden)
- Template expansion tests for `${DATASET}`, `${RANGE}`.
- Dataset-specific preset catalogs (OTel/Stripe/Segment) render to expected APL.
- Metadata sanity tests (name, description, format, default range).

### 3) Schema/fields virtual files (unit with mock client)
- `schema.csv` uses `getschema` APL when no schema endpoint.
- `fields/<field>/top.csv` renders `summarize topk(field, 10)`.
- CSV output format validation.

### 4) Query execution (integration via `httptest`)
- Mock Axiom API with `httptest.Server`.
- Validate request payload for APL queries.
- Streaming response to file with correct content-type.
- Error mapping to `result.error` (include APL + API error).

### 5) Cache correctness (unit + race)
- Same APL + format hits cache.
- Different range/format misses cache.
- TTL expiry.
- Concurrency: N readers -> single API call (singleflight).

### 6) FUSE behavior (integration)
- `ReadDir` exposes expected mount layout.
- Reading static files (README/examples) returns deterministic content.
- Reading `result.ndjson` triggers query execution.
- Invalid path returns ENOENT/EINVAL.

### 7) Safety tests
- Max range enforcement (reject > configured).
- Max rows/limit enforcement.
- Prevent query execution without `_time between`.

