# Storage Plugin Conformance Tests

This package is a shared conformance suite for `blob.BlobStore` and
`metadata.MetadataStore` implementations. Every storage plugin (`badger`,
`aws`, `gcs` for blob; `sqlite`, `mysql`, `postgres` for metadata) runs the
same suite against its own store instead of inventing its own CRUD test
shape, so a change to one plugin's behavior that breaks the shared contract
is caught the same way regardless of which plugin it happens in.

## What the suite covers

- `RunBlobStoreConformance` — KV and block/UTxO/tx round-trips, the
  `types.ErrBlobKeyNotFound`/`types.ErrNilTxn`/`types.ErrHistoryExpired`
  sentinels, same-transaction read-your-writes, rollback, iteration,
  concurrent writes to distinct keys, a large (1MiB) payload round-trip, and
  a basic operation-timeout bound.
- `RunMetadataStoreConformance` — the dialect-neutral capability surface
  every metadata plugin shares (`SettingsStore`, `TxnStore`, `SlotRangeStore`
  when the concrete store implements it, and the extracted storage domains):
  commit-timestamp and node-settings round-trips, settings-gate
  first-write-wins semantics including a concurrent insert-if-absent race,
  transaction commit/rollback, slot-range stats on empty data, one
  empty-state read through each domain interface's narrowed handle plus a
  constitution round trip through `GovernanceStore`, and the same
  operation-timeout bound.
- `AssertNoGoroutineLeak` / `AssertRepeatedLifecycleIsSafe` — resource checks
  run outside the two suites above (see [Resource checks](#resource-checks)).

Each plugin additionally has its own `TestBlobStore{UnreachableEndpoint,
BadCredentials}FailsCleanly` / `TestMetadataStore{UnreachableHost,
BadCredentials}FailsCleanly` tests, living in that plugin's own package
rather than in this shared suite because the failure mode differs per
backend (a bad endpoint URL vs. a bad host:port vs. a bad credentials file).

What this suite intentionally does not cover: the domain methods themselves
(accounts, pools, rewards, protocol parameters, ...). `sqlite`, `mysql`, and
`postgres` are thin driver shims around one shared
`database/plugin/metadata/sqlstore.Store` implementation, so those methods
have no per-dialect logic to differentiate here;
`database/plugin/metadata/sqlstore/dialect_integration_test.go` already
exercises that shared implementation against real Postgres/MySQL. The one
read per extracted domain above is not an exception to that: it exists to
prove each newly narrowed interface is wired to a working backend on this
dialect -- something a compile-time assertion cannot show -- not to test the
domain's behavior.

## Adding a new plugin's conformance test

Add the conformance test in the plugin's own package (in-package so it can
use the plugin's real constructor), in the `_test.go` file named for the
implementation file it exercises:

```go
func TestBlobStoreConformance(t *testing.T) {
    storagetest.RunBlobStoreConformance(t, func(t *testing.T) blob.BlobStore {
        store, err := New(WithBucket(bucket))
        require.NoError(t, err)
        require.NoError(t, store.Start())
        t.Cleanup(func() { require.NoError(t, store.Stop()) })
        return store
    })
}
```

`newStore` is called once; the suite reuses that store across every subtest,
so construction against a real bucket or database stays cheap. A
cloud- or database-backed plugin should skip cleanly (never fail
`go test ./...`) when its backend is not configured — see the credential
checks already in `database/plugin/blob/aws/tests_test.go` etc. for
the pattern, and the table below for which environment variables each
backend reads.

## Resource checks

`RunBlobStoreConformance`/`RunMetadataStoreConformance` reuse one store
across every subtest and only stop it via `t.Cleanup` after the whole suite
returns, so there is no point mid-suite where "just stopped, nothing else
running yet" is true. A resource check needs exactly that point, so it is a
separate, standalone test per plugin instead of a subtest inside the shared
suite:

- `AssertNoGoroutineLeak(t, run)` — for a plugin whose `Stop`/`Close` fully
  releases what it opened (a file handle, a `database/sql` connection pool):
  runs `run` twice and asserts the goroutine count after the second call is
  no higher than after the first.
- `AssertRepeatedLifecycleIsSafe(t, iterations, run)` — for a plugin backed
  by an HTTP or gRPC client whose SDK gives no way to force-close it (`aws`,
  `gcs`): Go's `net/http` keeps a `persistConn` read/write loop goroutine
  alive per pooled connection until its own idle timeout regardless of how
  many client values get constructed and abandoned, so a strict
  before/after goroutine-count diff misreports that expected, bounded
  behavior as a leak. This instead runs `run` `iterations` times in a row
  and requires each cycle to leave the test un-failed, catching the failure
  mode that actually matters for a plugin like this: a lifecycle bug
  (double-close panic, resource exhaustion, deadlock) that only surfaces
  after repeated open/close cycles.

## Migration tests

`internal/integration/storage_migration_test.go` covers a distinct concern
from the conformance suite above: migrating data between two *different*
plugins, not just checking each plugin in isolation. It writes a small
dataset through one backend's typed API and replays the exact retrieved
values into a second backend (badger→S3, badger→GCS, sqlite→postgres,
sqlite→mysql), then asserts the destination reads back identically to what
was written.

## Environment variables and CI availability

| Plugin | Configured via | Available in CI |
|---|---|---|
| `blob/badger` | always (local disk) | yes |
| `metadata/sqlite` | always (local disk) | yes |
| `blob/aws` (S3) | `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` or `~/.aws/credentials`; `AWS_ENDPOINT` for MinIO | yes, via CI's MinIO service |
| `metadata/postgres` | `POSTGRES_PASSWORD` or `POSTGRES_DSN` | yes, via CI's `postgres:16` service |
| `metadata/mysql` | `MYSQL_ROOT_PASSWORD` or `MYSQL_DSN` (needs `CREATE DATABASE`) | yes, via CI's `mysql:8` service |
| `blob/gcs` | `STORAGE_EMULATOR_HOST`, or `GOOGLE_APPLICATION_CREDENTIALS`/ADC for a real bucket; `DINGO_TEST_GCS_BUCKET` names the bucket | yes, via CI's in-memory fake GCS service |

Each plugin's `TestBlobStoreUnreachableEndpointFailsWithoutHanging` /
`TestMetadataStoreUnreachableHostFailsWithoutHanging` test needs none of the
above: it points at a closed local port, so it always runs, everywhere.
Production GCS uses the gRPC transport; setting `STORAGE_EMULATOR_HOST` selects
the SDK's HTTP transport for an emulator without changing real-bucket behavior.

## Running locally

The GCS checks can run locally without cloud credentials. In one terminal,
start the same in-memory emulator image used by
`.github/workflows/go-test.yml`. In another terminal, create a dedicated
bucket and run the required suites:

```bash
docker run --rm --name dingo-gcs -p 127.0.0.1:4443:4443 \
  fsouza/fake-gcs-server:1.56.1 \
  -scheme http -backend memory -public-host 127.0.0.1:4443
curl --fail --request POST --header 'Content-Type: application/json' \
  --data '{"name":"dingo-test"}' \
  'http://127.0.0.1:4443/storage/v1/b?project=dingo-test'
STORAGE_EMULATOR_HOST=http://127.0.0.1:4443 \
DINGO_TEST_GCS_BUCKET=dingo-test DINGO_REQUIRE_GCS_TESTS=1 \
  go test -tags dingo_extra_plugins \
  -run '^(TestBlobStoreConformance|TestBlobStoreResourceLifecycle)$' \
  ./database/plugin/blob/gcs
STORAGE_EMULATOR_HOST=http://127.0.0.1:4443 \
DINGO_TEST_GCS_BUCKET=dingo-test DINGO_REQUIRE_GCS_TESTS=1 \
  go test -tags dingo_extra_plugins -run '^TestBlobStoreMigrationGCS$' \
  ./internal/integration
```

Without any of those environment variables set: a plain, untagged
`go test ./...` still runs the `badger` and `sqlite` conformance tests (the
only two plugins built by default). `go test -tags dingo_extra_plugins ./...`
additionally compiles in `aws`/`gcs`/`postgres`/`mysql` and still runs each
of their unreachable-endpoint/unreachable-host tests (those need no
credentials or running server); every other test in those four packages
skips cleanly instead of failing.
