# Ledger Rules Conformance Tests

This package runs the [Amaru ledger rules conformance vectors](https://github.com/pragma-org/amaru)
against Dingo's ledger implementation. The shared harness and embedded test
data live in `github.com/blinklabs-io/ouroboros-mock/conformance`; this package
provides `DingoStateManager`, an adapter that drives Dingo's database and
ledger packages so every vector runs against a clean state. `DingoStateManager`
only ever talks to the database through `*gorm.DB`, so the same adapter runs
against an in-memory SQLite backend (the default, no setup required), a real
PostgreSQL backend (see [PostgreSQL backend](#postgresql-backend)), or a real
MySQL backend (see [MySQL backend](#mysql-backend)).

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

By default the tests use an in-memory SQLite database and need no setup. A
second, build-tag-gated variant runs the identical harness against a real
PostgreSQL database, using the same `dingo_extra_plugins` build tag as
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
`go test -race ./...` step.

**Schema isolation.** `database/plugin/metadata/postgres`'s own tests
connect to the same `dingo_test` database. Since `go test ./...` runs
different packages as separate, concurrent processes, sharing the default
`public` schema would let the two suites race on the same tables.
`NewDingoPostgresStateManager` migrates into a dedicated `conformance`
schema instead (via `CREATE SCHEMA IF NOT EXISTS` + `SET search_path`, with
the connection pool pinned to one connection so every statement lands on
the session the `search_path` was set on).

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
the existing tagged `go test -race ./...` step.

**Database isolation.** MySQL has no schema/database distinction the way
Postgres does — a MySQL "schema" *is* a database. `database/plugin/metadata/mysql`'s
own tests connect to the shared `dingo_test` database with a user the
official `mysql` image's bootstrap grants access to *only* that database, so
this suite can't reuse that user to carve out an isolated namespace the way
the Postgres one does with `CREATE SCHEMA`. Instead,
`NewDingoMysqlStateManager` authenticates as `root` (the one account
guaranteed to have `CREATE DATABASE` privileges) and migrates into its own
dedicated `dingo_conformance_test` database. This is why the MySQL tests key
off `MYSQL_ROOT_PASSWORD` specifically rather than the `MYSQL_PASSWORD` the
plugin's own tests use.

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
2. A fresh `DingoStateManager` spins up an in-memory SQLite database and
   applies Dingo's GORM migrations.
3. The harness (`conformance.NewHarness`) walks every vector, feeding
   transactions through the state manager and comparing expected vs. actual
   ledger state after each step.
4. `RunAllVectors` fails the Go test on any vector mismatch;
   `RunAllVectorsWithResults` returns structured pass/fail counts instead
   so progress can be tracked.

## Files

| File | Purpose |
|---|---|
| `conformance_test.go` | Go test entry points, SQLite backend (`TestRulesConformanceVectors`, `…WithResults`) |
| `conformance_postgres_test.go` | Go test entry points, PostgreSQL backend (`dingo_extra_plugins` build tag) |
| `conformance_mysql_test.go` | Go test entry points, MySQL backend (`dingo_extra_plugins` build tag) |
| `state_manager.go`    | `DingoStateManager` — implements `conformance.StateManager` against Dingo's DB/ledger |
| `state_manager_postgres.go` | `NewDingoPostgresStateManager` — same `DingoStateManager`, Postgres connection (`dingo_extra_plugins` build tag) |
| `state_manager_mysql.go` | `NewDingoMysqlStateManager` — same `DingoStateManager`, MySQL connection (`dingo_extra_plugins` build tag) |
| `state_provider.go`   | State-query adapters used by the harness |
| `docker-compose.yml`  | Local PostgreSQL and MySQL for the SQL-backed tests |

## Updating vectors

The vectors themselves are **embedded in `ouroboros-mock`**, not in this repo.
To update the corpus, bump the `ouroboros-mock` dependency in `go.mod` and
re-run the suite. Do not add or mutate vectors locally.

## Debugging a failing vector

1. Re-run the failing vector in isolation with `-v` so the harness prints
   per-step diagnostics.
2. Check whether the failure is Dingo-side (ledger logic) or state-manager-side
   (`state_manager.go` mapping between `common.*` types and Dingo's GORM
   models). State-manager bugs usually surface as the same vector failing
   identically across multiple eras; ledger bugs are usually era-specific.
3. If the upstream vector itself looks wrong, file an issue against
   `blinklabs-io/ouroboros-mock` rather than patching around it here.
