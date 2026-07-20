# URL shortener with YDB

This project implements a link shortener in the Rust language with YDB as the data storage platform.
YDB stores links in a row-oriented table and visits in a column-oriented table.
Statistics are collected asynchronously through a producer and consumer over YDB topics.

## Technologies

- Rust
- YDB
- Docker

## Endpoints

| Method | Path                     | Description                                              |
| ------ | ------------------------ | -------------------------------------------------------- |
| `POST` | `/api/shorten`           | Creates a short link. Rate limited per client IP.        |
| `GET`  | `/cc/{code}`             | Redirects to the target URL, or `404` for unknown codes. |
| `GET`  | `/health`                | Liveness probe.                                          |
| `GET`  | `/swagger-ui`            | API docs. Disabled by default when `APP_ENV=production`. |
| `GET`  | `/api-docs/openapi.json` | OpenAPI schema. Follows the same toggle as Swagger UI.   |

### Creating a link

```bash
curl -X POST http://localhost:8080/api/shorten \
  -H 'content-type: application/json' \
  -d '{"url": "https://example.com/item", "utm_source": "google"}'
```

```json
{ "code": "aB3dEf7h", "short_url": "http://localhost:8080/cc/aB3dEf7h" }
```

UTM parameters are appended to the redirect target, so visitors land on
`https://example.com/item?utm_source=google`. They are also stored in their own
columns for analytics.

Only `http` and `https` targets are accepted. URLs that are malformed, have no
host, embed credentials (`https://trusted.example@evil.test`), or use another
scheme (`javascript:`, `data:`, `file:`) are rejected with `400`.

## Short codes

Codes cannot collide. They are minted from a counter, not drawn at random:

```text
counter (YDB, serializable)  ->  keyed Feistel permutation  ->  base62
        1, 2, 3, ...                    bijection              "k9Xm2Qw4"
```

The counter never repeats a value, and a Feistel network is a bijection, so no
two counter values can ever encode to the same code. There is no retry loop and
no collision probability to reason about — the previous random scheme only made
duplicates unlikely, and could still exhaust its attempts under load.

The permutation exists because a bare counter would publish `1, 2, 3, …`, letting
anyone walk every link the service has created. Keyed by `CODE_SECRET`, the
output is indistinguishable from random without the key while staying provably
duplicate-free.

Codes are still not derived from the target URL. A content hash lets an attacker
precompute a colliding URL, register it first, and take over the code a later
caller expects to receive.

### Operational notes

- **`CODE_SECRET` must never change once links exist.** A different key is a
  different permutation, so codes minted after a rotation can collide with codes
  minted before it. The `PRIMARY KEY` on `urls.code` remains as a backstop: a
  clash is rejected and logged rather than silently overwriting someone's link.
- **Counter values are claimed in blocks** of `CODE_BLOCK_SIZE`, so the counter
  row is written once per block rather than once per link. Values left unused
  when a process exits are skipped; codes have no reason to be contiguous.
- **`CODE_LENGTH` caps total links** at `62^n` — about 2.2 × 10¹⁴ at the default
  8 characters. Raising it later is safe and widens the space from that point on.

## Deployment topology

The image runs in one of two roles, selected by `APP_ROLE`:

```text
APP_ROLE=server            APP_ROLE=consumer
serverless container       always-on (VM / Managed k8s)
  POST /api/shorten          /topics/visits -> visits table
  GET  /cc/{code}
     |
     +-- writes the visit to the topic and waits for the
         acknowledgement before returning the redirect
```

Both roles run from the same image; only the environment differs. A third role,
`APP_ROLE=all`, runs both in one process — convenient for docker compose and
single-VM deployments, and subject to the same always-on requirement as
`consumer`.

### Running both locally

```bash
docker compose up --build
```

This starts a local YDB plus both roles, mirroring the production split. It uses
`YDB_CREDENTIALS=anonymous`, because the container image has no `yc` CLI and the
default `cli` credential source cannot work there.

The compose file deliberately does **not** read `.env` — that file points at the
real cloud database, and loading it here would aim local containers at
production.

### Why the visit write is synchronous

A serverless container is suspended between requests — Yandex's documentation
states that a suspended instance's "running processes remain in RAM but are not
processed by the CPU". Work handed to a background task after the response is
returned may therefore never run: the task is frozen mid-flight and discarded
when the instance is eventually terminated.

So the redirect handler writes the visit to the topic and awaits the server's
acknowledgement before responding. That costs one topic round-trip per redirect
and is the only way to guarantee the write happens at all. A failed write is
logged and swallowed: degraded analytics must never turn into a failed redirect.

For the same reason the consumer cannot live in the container, and runs as its
own always-on deployment.

### Why each instance gets its own producer id

YDB deduplicates topic messages on `(producer_id, seq_no)`, and a writer resumes
its sequence from the server's `last_seq_no` at init. A shared constant producer
id therefore breaks under scale-out: every concurrent instance starts from the
same sequence number and emits the same values, so the server keeps one
instance's messages and silently skips the rest as duplicates. Each process now
generates a random producer id at startup.

The skip cannot be detected from application code — the SDK's
`MessageWriteStatus`, which distinguishes `Written` from `Skipped(AlreadyWritten)`,
lives in a `pub(crate)` module and cannot be named outside the `ydb` crate. It
has to be prevented rather than observed.

## Configuration

All configuration comes from the environment; see [env.example](env.example) for
the full annotated list. Startup fails fast on anything missing or malformed.

The most consequential setting is `CLIENT_IP_SOURCE` — it must match your
deployment topology. It drives both visit analytics and the rate-limiter key, so
a wrong value either lets clients spoof past the rate limit or buckets every
user behind your proxy into a single limit.

## Local development

```bash
docker compose up -d          # start a local YDB
cp env.example .env           # then edit as needed
cargo run
```

### Migrations

Set `RUN_MIGRATIONS=true` to create the `urls`, `code_counter` and `visits`
tables on startup. The process continues serving afterwards, so unset it again
for normal runs.

`code_counter` is new. An existing deployment must run migrations once before
links can be created, and should keep `CODE_LENGTH` at its previous value so
codes stay a consistent width.

## Checks

```bash
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-features --locked
cargo audit
```

### Integration tests

Every query in `db.rs` is only ever validated by the server, so the database
layer has tests that run against a real YDB. They are `#[ignore]`d to keep
`cargo test` hermetic:

```bash
docker compose up -d ydb
cargo test --all-features --locked -- --ignored --test-threads=1
```

Single-threaded because the tests share one schema and the short-code counter
is a single row, so parallel runs contend on it. CI runs them in a separate job
against a YDB service container.

Two caveats when running locally. The tests pin the endpoint with
`StaticDiscovery`, because the container advertises its own hostname through
discovery and the host cannot resolve it. And `ydbplatform/local-ydb` has no
column-store support, so `visits` is created as a row-store table with
identical columns — the bulk upsert is covered, the `STORE = COLUMN` clause in
`init_visits_tables` is not.
