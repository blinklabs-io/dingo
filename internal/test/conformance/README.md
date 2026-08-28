# Ledger Rules Conformance Tests

This package runs the [Amaru ledger rules conformance vectors](https://github.com/pragma-org/amaru)
against Dingo's ledger implementation. The shared harness and embedded test
data live in `github.com/blinklabs-io/ouroboros-mock/conformance`; this package
provides `DingoStateManager`, an adapter that drives Dingo's real
`database.Database` and `ledger/governance` packages -- the same production
persistence code the node uses, not a hand-rolled second implementation --
so every vector's UTxO/certificate/governance writes and reads go through a
real, configured metadata backend. The same adapter runs against a real,
local SQLite backend (the default, no setup required -- see
[`state_manager.go`](state_manager.go)), a real PostgreSQL backend (see
[PostgreSQL backend](#postgresql-backend)), or a real MySQL backend (see
[MySQL backend](#mysql-backend)).

## What the vectors cover

The vectors exercise **Conway era** ledger rules:

- UTxO validation — inputs, outputs, fees, collateral
- Certificate processing — stake, pool, DRep, committee
- Governance — proposals, voting, enactment
- Script execution — native scripts, Plutus V1/V2/V3

## Running the tests

Run the full suite:

```bash
go test ./internal/test/conformance/
```

Run with verbose vector-level output (useful when investigating a failure):

```bash
go test -v ./internal/test/conformance/ -run TestRulesConformanceVectors
```

Run the variant that reports per-vector pass/fail statistics:

```bash
go test -v ./internal/test/conformance/ -run TestRulesConformanceVectorsWithResults
```

Run a single vector by substring match (delegated by the harness):

```bash
go test -v ./internal/test/conformance/ -run TestRulesConformanceVectors -vector <name>
```

## PostgreSQL backend

By default the tests use a real, local SQLite database (see
[How it works](#how-it-works)) and need no setup. A second, build-tag-gated
variant runs the identical harness against a real PostgreSQL database,
using the same `dingo_extra_plugins` build tag as
`database/plugin/metadata/postgres` (the actual Postgres metadata store
plugin) and the same `POSTGRES_HOST/PORT/USER/PASSWORD/DATABASE/SSLMODE`
environment variables that plugin's tests and CI's `go-test-linux` job
already use.

Bring up a local Postgres and run it:

```bash
docker compose -f internal/test/conformance/docker-compose.yml up -d

POSTGRES_HOST=localhost POSTGRES_PORT=5432 POSTGRES_USER=postgres \
POSTGRES_PASSWORD=postgres POSTGRES_DATABASE=dingo_test \
  go test -tags dingo_extra_plugins -v ./internal/test/conformance/... -run Postgres
```

Without a `POSTGRES_PASSWORD` or `POSTGRES_DSN` set, both Postgres tests
skip (they never fail a plain `go test ./...`). CI's `go-test-linux` job
already runs a `postgres:16` service with those exact env vars, so the
Postgres variant runs automatically as part of the existing tagged
`go test -tags dingo_extra_plugins ./...` step (`.github/workflows/go-test.yml`'s
pull-request job omits `-race` to keep PR feedback fast; `-race` only runs
during publish/release validation).

**Schema isolation.** `database/plugin/metadata/postgres`'s own tests
connect to the same `dingo_test` database. Since `go test ./...` runs
different packages as separate, concurrent processes, and a local run can
overlap another `go test` invocation (or two CI shards) against the same
server, sharing one fixed schema across every process would let those
processes race on the same tables and truncate each other's in-progress
state. `NewDingoPostgresStateManager` instead migrates into a schema unique
to this one test binary process (`conformance_<pid>_<timestamp>`, computed
once at package load and shared by every call within that process --
`CREATE SCHEMA IF NOT EXISTS` plus a connection-level `search_path` pinned
to that schema via `PostgresDSNWithSearchPath`, baked into the DSN's
connection startup parameters so it applies to every connection the pool
opens, not just one).

**Local blob directory.** The metadata store is remote and persistent for
this process's lifetime (the process schema is truncated, never dropped,
between ordinary constructions -- see the next paragraph), so the local
Badger blob store paired with it must persist for that same lifetime:
`database.New`'s commit-timestamp consistency check requires both stores
in one `Database` to have last committed the same timestamp, and a fresh,
empty local directory paired with an already-advanced remote schema fails
that check. `NewDingoPostgresStateManager` creates one `os.MkdirTemp`
directory the first time it's called in a process (via `sync.Once`) and
reuses it for every later call in that same process, rather than a new
temporary one per call; that directory is never cleared between vectors
(stale blob entries are keyed by a truncated vector's own transaction
hashes and are simply never looked up again -- harmless, see
`state_manager_postgres.go`'s doc comment). `TestMain` drops the process
schema and removes this directory once, after every test in the process
has finished -- see `conformance_main_test.go`.

**Reset semantics.** Between vectors, `DingoStateManager.Reset()` does not
call the metadata store's own `Resettable.Reset` (`database/plugin/metadata/postgres`'s
`resetDatabase`): that callback drops tables outright, without recreating
them, requiring a fresh migration run before the store is usable again --
and, more importantly, scans and drops tables across *every* non-system
schema in the target database, not just `conformance`, so calling it here
would also destroy `database/plugin/metadata/postgres`'s own concurrently
running tests' tables in the shared `dingo_test` database. Reset instead
`TRUNCATE`s every table in the `conformance` schema in place, over a
separate admin connection, discovering the table list from
`information_schema` rather than hardcoding it (see
`state_manager_postgres.go`'s `wipeMetadata`). This keeps the already-open
store's connection pool live throughout -- no close, no reopen, no
re-migration -- which is what keeps the cost of a Reset (and so the whole
vector suite, which resets once per vector) from being a real
close/reopen/re-migrate network round trip every time.

`TestRulesConformanceVectorsWithResultsPostgres` runs the SQLite and
Postgres harnesses in the same test and compares vector counts instead of
asserting a hardcoded number, so the two runs should exercise the identical
vector count with identical pass counts, and the comparison stays correct
even as the embedded `ouroboros-mock` vector corpus grows or shrinks.

## MySQL backend

Same idea as the PostgreSQL backend above, using the same `dingo_extra_plugins`
build tag as `database/plugin/metadata/mysql` and the same
`MYSQL_HOST/PORT` environment variables that plugin's tests and CI's
`go-test-linux` job already use.

Bring up a local MySQL and run it:

```bash
docker compose -f internal/test/conformance/docker-compose.yml up -d mysql

MYSQL_HOST=localhost MYSQL_PORT=3306 MYSQL_ROOT_PASSWORD=mysql \
  go test -tags dingo_extra_plugins -v ./internal/test/conformance/... -run Mysql
```

Without a `MYSQL_ROOT_PASSWORD` or `MYSQL_DSN` set, both MySQL tests skip.
CI's `go-test-linux` job already runs a `mysql:8` service and sets
`MYSQL_ROOT_PASSWORD`, so the MySQL variant runs automatically as part of
the existing tagged `go test -tags dingo_extra_plugins ./...` step
(`.github/workflows/go-test.yml`'s pull-request job omits `-race` to keep
PR feedback fast; `-race` only runs during publish/release validation).

**Database isolation.** MySQL has no schema/database distinction the way
Postgres does — a MySQL "schema" *is* a database. `database/plugin/metadata/mysql`'s
own tests connect to the shared `dingo_test` database with a user the
official `mysql` image's bootstrap grants access to *only* that database, so
this suite can't reuse that user to carve out an isolated namespace the way
the Postgres one does with `CREATE SCHEMA`. Instead,
`NewDingoMysqlStateManager` authenticates as `root` (the one account
guaranteed to have `CREATE DATABASE` privileges) and migrates into a
database unique to this one test binary process
(`dingo_conformance_<pid>_<timestamp>`, computed once at package load and
shared by every call within that process; the mysql metadata plugin's own
`openStore` provisions it automatically -- `CREATE DATABASE IF NOT EXISTS`,
via its `ensureDatabaseExists` step -- whenever the DSN it's given names a
database). This is why the MySQL tests key off `MYSQL_ROOT_PASSWORD`
specifically rather than the `MYSQL_PASSWORD` the plugin's own tests use.

**Local blob directory and reset semantics** follow the same reasoning as
the Postgres backend above: `NewDingoMysqlStateManager` creates one
`os.MkdirTemp` directory the first time it's called in a process (via
`sync.Once`) and reuses it for every later call in that same process,
paired with the process-scoped database, and `Reset()` `TRUNCATE`s every
table in that database in place, over a separate admin connection (rather
than calling `Resettable.Reset`, which drops tables individually without
recreating them), keeping the already-open store's connection pool live
throughout instead of paying for a close/reopen/re-migrate cycle on every
vector. `TestMain` drops the process database and removes this directory
once, after every test in the process has finished -- see
`conformance_main_test.go`.

`TestRulesConformanceVectorsWithResultsMysql` follows the same
count-comparison approach as the Postgres variant, for the same reason.

## When to run them

**Conformance tests are mandatory after every ledger-affecting change**, not
just once at the end of a branch. Specifically, run them after any edit under
`ledger/`, `database/plugin/metadata/`, `database/models/`, or any dependency
bump of `gouroboros`, `plutigo`, or `ouroboros-mock`. A regression here almost
always indicates a correctness bug that CI on unit tests will miss.

Cross-repo change cascades that must re-run this suite:

| Changed repo | Must run conformance tests in |
|---|---|
| `plutigo`    | plutigo → gouroboros → **dingo** |
| `gouroboros` | gouroboros → **dingo** |
| `dingo`      | **dingo** |

## How it works

1. The test extracts embedded vectors from `ouroboros-mock/conformance` into
   a temp directory (`ExtractEmbeddedTestdata`).
2. A fresh `DingoStateManager` composes a real `database.Database` (a real
   sqlite/postgres/mysql metadata store plus a local Badger blob store,
   through the same `plugin.Resolve` path the production node uses at
   startup) and runs the versioned SQL-store migrations.
3. The harness (`conformance.NewHarness`) walks every vector, feeding
   transactions through the state manager -- which applies UTxOs,
   certificates, and governance state to the real backend via
   `database.SetTransactionMetadataOnly` and `ledger/governance`, not an
   in-memory mirror -- and comparing expected vs. actual ledger state after
   each step.
4. `RunAllVectors` fails the Go test on any vector mismatch;
   `RunAllVectorsWithResults` returns structured pass/fail counts instead
   so progress can be tracked.
5. Between vectors, `Reset()` clears the real backend (not just in-memory
   bookkeeping) so each vector starts from a genuinely empty database --
   see each backend's own "Reset semantics" above for how.

## Files

| File | Purpose |
|---|---|
| `conformance_test.go` | Go test entry points, SQLite backend (`TestRulesConformanceVectors`, `…WithResults`) |
| `conformance_postgres_test.go` | Go test entry points, PostgreSQL backend, including restart/rollback/invalid-DSN acceptance tests (`dingo_extra_plugins` build tag) |
| `conformance_mysql_test.go` | Go test entry points, MySQL backend, including restart/rollback/invalid-DSN acceptance tests (`dingo_extra_plugins` build tag) |
| `conformance_main_test.go` | `TestMain` — drops this process's Postgres schema and MySQL database and removes their paired blob directories once, after every test in the process has finished (`dingo_extra_plugins` build tag) |
| `database.go` | `openRealDatabase`/`closeRealDatabase` — composes a real blob+metadata `database.Database` via `plugin.Resolve`, shared by all three backend constructors |
| `state_manager.go`    | `DingoStateManager` — implements `conformance.StateManager` against a real Dingo `database.Database` and `ledger/governance`, reusing production persistence code |
| `state_manager_postgres.go` | `NewDingoPostgresStateManager` — same `DingoStateManager`, real Postgres connection with schema isolation (`dingo_extra_plugins` build tag) |
| `state_manager_mysql.go` | `NewDingoMysqlStateManager` — same `DingoStateManager`, real MySQL connection with database isolation (`dingo_extra_plugins` build tag) |
| `state_manager_backend_test.go` | Real-backend acceptance tests against the default SQLite manager: restart survival, transaction rollback, and an epoch-transition/stake-snapshot test driving `ledger/governance.ProcessEpoch` and `ledger/snapshot.Manager` end to end |
| `state_provider.go`   | State-query adapters used by the harness -- every read queries the real backend live (see its type doc comment for the one narrow, documented exception) |
| `docker-compose.yml`  | Local PostgreSQL and MySQL for the SQL-backed tests |

## Updating vectors

The vectors themselves are **embedded in `ouroboros-mock`**, not in this repo.
To update the corpus, bump the `ouroboros-mock` dependency in `go.mod` and
re-run the suite. Do not add or mutate vectors locally.

## Debugging a failing vector

1. Re-run the failing vector in isolation with `-v` so the harness prints
   per-step diagnostics.
2. Check whether the failure is Dingo-side (ledger logic) or state-manager-side
   (`state_manager.go` mapping between `common.*` types and the SQL-store
   models). State-manager bugs usually surface as the same vector failing
   identically across multiple eras; ledger bugs are usually era-specific.
3. If the upstream vector itself looks wrong, file an issue against
   `blinklabs-io/ouroboros-mock` rather than patching around it here.
