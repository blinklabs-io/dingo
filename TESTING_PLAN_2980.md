# dingo#2980 manual testing plan

Verifies the Postgres/MySQL/S3/GCS `Backuper`/`Restorer` implementations and
the Docker snapshot-dir fix on branch `feat/2980/db_snapshot_restore_truncate_gaps`,
end to end, before pushing. This file is temporary — delete it once you're
done (`rm TESTING_PLAN_2980.md`).

Requires Docker Desktop (or equivalent) running locally. `pg_dump`/`pg_restore`/
`mysqldump`/`mysql` are Linux binaries and won't run natively on macOS, so
Phase 3's real-backend Go tests and Phase 4's CLI checks run inside a small
throwaway Linux container that has them installed — no need to install
anything on the host itself besides Docker.

GCS cannot be exercised locally at all (see "Known gaps" at the bottom) —
skip Phase 3/4's GCS steps unless you have real GCP credentials.

## Phase 0: Start backing services

```shell
docker network create dingo-test-net

docker run -d --name dingo-test-postgres --network dingo-test-net \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=dingo_test \
  -p 55432:5432 postgres:16

docker run -d --name dingo-test-mysql --network dingo-test-net \
  -e MYSQL_ROOT_PASSWORD=mysql -e MYSQL_USER=mysql -e MYSQL_PASSWORD=mysql -e MYSQL_DATABASE=dingo_test \
  -p 53306:3306 mysql:8

docker run -d --name dingo-test-minio --network dingo-test-net \
  -p 9000:9000 -p 9001:9001 blinklabs/minio:main

# Wait for health (repeat until each reports healthy/ready)
docker exec dingo-test-postgres pg_isready -U postgres
docker exec dingo-test-mysql mysqladmin ping -h 127.0.0.1 -uroot -pmysql
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:9000/minio/health/live   # expect 200

# Create the MinIO bucket CI uses
docker run --rm --network dingo-test-net --entrypoint sh minio/mc:latest -c "
  mc alias set localminio http://dingo-test-minio:9000 minioadmin minioadmin &&
  mc mb localminio/dingo-test || true"
```

## Phase 1: Unit tests (no services needed)

Run from the repo root, on the host, normally:

```shell
go build -tags dingo_extra_plugins ./...
go build ./...
go vet -tags dingo_extra_plugins ./...

go test -tags dingo_extra_plugins ./database/plugin/metadata/postgres/... -v
go test -tags dingo_extra_plugins ./database/plugin/metadata/mysql/... -v
go test -tags dingo_extra_plugins ./database/plugin/blob/aws/... -v
go test -tags dingo_extra_plugins ./database/plugin/blob/gcs/... -v
go test -tags dingo_extra_plugins ./database/plugin/metadata/sqlstore/... -v
go test -tags dingo_extra_plugins ./database/lifecycle/... -v
go test ./internal/config/... -v
```

All should pass. The S3/GCS live-backend tests inside these packages will
print `SKIP ... no S3/GCS credentials configured` at this stage — expected,
they run for real in Phase 3.

## Phase 2: Build the Linux test-runner image

This gives you a `go test`/`dingo` environment with the Postgres/MySQL
client tools actually installed, matching CI's ubuntu runner and the
project's own (now-fixed) Docker image.

```shell
mkdir -p /tmp/dingo-testrunner
cat > /tmp/dingo-testrunner/Dockerfile << 'EOF'
FROM golang:1.26-bookworm
RUN apt-get update -y && \
  apt-get install -y --no-install-recommends gnupg wget && \
  install -d /usr/share/postgresql-common/pgdg && \
  wget -qO /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
    https://www.postgresql.org/media/keys/ACCC4CF8.asc && \
  echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
    > /etc/apt/sources.list.d/pgdg.list && \
  apt-get update -y && \
  apt-get install -y default-mysql-client postgresql-client-16 && \
  rm -rf /var/lib/apt/lists/*
WORKDIR /code
EOF
docker build -t dingo-test-runner /tmp/dingo-testrunner

# Sanity check versions -- must match the Dockerfile's own pinned client
# (postgresql-client-16): a newer client can fail to dump from an older
# server, and can fail to restore into one even when dumping from it
# works (see "Bugs found" below).
docker run --rm dingo-test-runner pg_dump --version
docker run --rm dingo-test-runner pg_restore --version
docker run --rm dingo-test-runner mysqldump --version
```

## Phase 3: Integration tests against real services

Run from the repo root (mounts the working tree read-only into the
container):

```shell
docker run --rm --network dingo-test-net \
  -v "$(pwd)":/code:ro \
  -v dingo-gomod-cache:/go/pkg/mod \
  -v dingo-gobuild-cache:/root/.cache/go-build \
  -e DINGO_POSTGRES_DSN="postgres://postgres:postgres@dingo-test-postgres:5432/dingo_test?sslmode=disable" \
  -e DINGO_MYSQL_DSN="root:mysql@tcp(dingo-test-mysql:3306)/dingo_test?parseTime=true" \
  -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin \
  -e AWS_ENDPOINT="http://dingo-test-minio:9000/" -e AWS_REGION=us-east-1 \
  -e DINGO_TEST_S3_BUCKET=dingo-test \
  dingo-test-runner \
  go test -tags 'dingo_extra_plugins dingo_db_integration' \
    ./database/plugin/metadata/postgres/... \
    ./database/plugin/metadata/mysql/... \
    ./database/plugin/blob/aws/... \
    ./database/plugin/blob/gcs/... \
    -v
```

Expect: every Postgres/MySQL/S3 test passes for real (round-trip,
reject-existing-destination, reject-non-empty-target, and
`TestResetThenRestoreIntegration` — the regression test for the
resolve-then-migrate-then-restore bug described below). GCS tests
print `SKIP ... no GCS credentials configured` — expected without real
GCP credentials (see "Known gaps").

If you *do* have real GCP credentials and want to exercise GCS for real,
mount them and set `DINGO_TEST_GCS_BUCKET` to a bucket **dedicated to this
test** (the GCS plugin has no key-prefix isolation, so the test skips if
the bucket isn't already empty):

```shell
docker run --rm --network dingo-test-net \
  -v "$(pwd)":/code:ro \
  -v dingo-gomod-cache:/go/pkg/mod -v dingo-gobuild-cache:/root/.cache/go-build \
  -v "$HOME/.config/gcloud":/root/.config/gcloud:ro \
  -e DINGO_TEST_GCS_BUCKET=<your-dedicated-empty-bucket> \
  dingo-test-runner \
  go test -tags dingo_extra_plugins ./database/plugin/blob/gcs/... -v
```

## Phase 4: Real end-to-end CLI verification

Builds the actual shipped Docker image and runs real `dingo database
snapshot`/`restore` commands against the live services — the most
authoritative check, since it exercises the exact orchestration path
(`internal/dblifecycle`, `database/lifecycle/restore.go`) the CLI/bark use,
not just the plugin packages directly.

```shell
docker build --target dingo -t dingo-local-test .

# Sanity: confirm the pinned non-root user/UID and bundled client tools
docker run --rm --entrypoint id dingo-local-test          # expect uid=1000(dingo) gid=1000(dingo)
docker run --rm --entrypoint pg_dump dingo-local-test --version
docker run --rm --entrypoint mysqldump dingo-local-test --version

mkdir -p /tmp/dingo-e2e
```

### Postgres round trip

```shell
docker exec dingo-test-postgres psql -U postgres -c "CREATE DATABASE dingo_e2e;"
docker exec dingo-test-postgres psql -U postgres -c "CREATE DATABASE dingo_e2e_restore;"

cat > /tmp/dingo-e2e/pg-src.yaml << 'EOF'
network: preview
plugins:
  storage:
    metadata:
      provider: "postgres"
      config:
        host: "dingo-test-postgres"
        port: 5432
        user: "postgres"
        password: "postgres"
        database: "dingo_e2e"
        sslMode: "disable"
EOF
cat > /tmp/dingo-e2e/pg-dst.yaml << 'EOF'
network: preview
plugins:
  storage:
    metadata:
      provider: "postgres"
      config:
        host: "dingo-test-postgres"
        port: 5432
        user: "postgres"
        password: "postgres"
        database: "dingo_e2e_restore"
        sslMode: "disable"
EOF

docker run --rm --network dingo-test-net -v /tmp/dingo-e2e:/e2e \
  --entrypoint dingo dingo-local-test \
  database snapshot --config /e2e/pg-src.yaml --data-dir /e2e/pg-src-data --dir /e2e/pg-snap

docker run --rm --network dingo-test-net -v /tmp/dingo-e2e:/e2e \
  --entrypoint dingo dingo-local-test \
  database restore /e2e/pg-snap --config /e2e/pg-dst.yaml --data-dir /e2e/pg-dst-data

# Confirm real tables landed
docker exec dingo-test-postgres psql -U postgres -d dingo_e2e_restore -c "\dt" | head
docker exec dingo-test-postgres psql -U postgres -d dingo_e2e_restore -c "SELECT count(*) FROM node_settings;"
```

Expect: `Snapshot written to /e2e/pg-snap ...` then `Database restored to
/e2e/pg-dst-data ...`, no errors, and `\dt` lists ~90 tables.

### MySQL round trip

```shell
docker exec dingo-test-mysql mysql -uroot -pmysql -e "CREATE DATABASE dingo_e2e; CREATE DATABASE dingo_e2e_restore;"

cat > /tmp/dingo-e2e/mysql-src.yaml << 'EOF'
network: preview
plugins:
  storage:
    metadata:
      provider: "mysql"
      config:
        host: "dingo-test-mysql"
        port: 3306
        user: "root"
        password: "mysql"
        database: "dingo_e2e"
EOF
cat > /tmp/dingo-e2e/mysql-dst.yaml << 'EOF'
network: preview
plugins:
  storage:
    metadata:
      provider: "mysql"
      config:
        host: "dingo-test-mysql"
        port: 3306
        user: "root"
        password: "mysql"
        database: "dingo_e2e_restore"
EOF

docker run --rm --network dingo-test-net -v /tmp/dingo-e2e:/e2e \
  --entrypoint dingo dingo-local-test \
  database snapshot --config /e2e/mysql-src.yaml --data-dir /e2e/mysql-src-data --dir /e2e/mysql-snap

docker run --rm --network dingo-test-net -v /tmp/dingo-e2e:/e2e \
  --entrypoint dingo dingo-local-test \
  database restore /e2e/mysql-snap --config /e2e/mysql-dst.yaml --data-dir /e2e/mysql-dst-data

docker exec dingo-test-mysql mysql -uroot -pmysql dingo_e2e_restore -e "SELECT count(*) FROM node_settings; SHOW TABLES;" | head
```

### S3 (MinIO) round trip

```shell
cat > /tmp/dingo-e2e/s3-src.yaml << 'EOF'
network: preview
plugins:
  storage:
    blob:
      provider: "s3"
      config:
        bucket: "dingo-test"
        region: "us-east-1"
        endpoint: "http://dingo-test-minio:9000/"
        prefix: "e2e-src/"
EOF
cat > /tmp/dingo-e2e/s3-dst.yaml << 'EOF'
network: preview
plugins:
  storage:
    blob:
      provider: "s3"
      config:
        bucket: "dingo-test"
        region: "us-east-1"
        endpoint: "http://dingo-test-minio:9000/"
        prefix: "e2e-dst/"
EOF

docker run --rm --network dingo-test-net -v /tmp/dingo-e2e:/e2e \
  -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin \
  --entrypoint dingo dingo-local-test \
  database snapshot --config /e2e/s3-src.yaml --data-dir /e2e/s3-src-data --dir /e2e/s3-snap

docker run --rm --network dingo-test-net -v /tmp/dingo-e2e:/e2e \
  -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin \
  --entrypoint dingo dingo-local-test \
  database restore /e2e/s3-snap --config /e2e/s3-dst.yaml --data-dir /e2e/s3-dst-data
```

Expect both commands to print success (`Snapshot written to ...` /
`Database restored to ...`) with no errors.

## Phase 5: Full verification sweep

On the host (no services needed for these):

```shell
go build -tags dingo_extra_plugins ./...
go build ./...
go vet -tags dingo_extra_plugins ./...
golangci-lint run ./...
nilaway ./...
modernize ./...              # expect only pre-existing findings in generated codegen files, none in touched packages
gofmt -l $(git diff --name-only -- '*.go')   # expect no output

go test -tags dingo_extra_plugins ./...
go test -tags dingo_extra_plugins -race \
  ./database/plugin/metadata/postgres/... \
  ./database/plugin/metadata/mysql/... \
  ./database/plugin/metadata/sqlstore/... \
  ./database/plugin/blob/aws/... \
  ./database/plugin/blob/gcs/... \
  ./database/lifecycle/... \
  ./internal/config/... \
  ./internal/dblifecycle/...

make import-boundaries
make docs-parity
```

All should be clean/passing.

## Cleanup

```shell
docker rm -f dingo-test-postgres dingo-test-mysql dingo-test-minio
docker network rm dingo-test-net
docker rmi dingo-test-runner dingo-local-test
docker volume rm dingo-gomod-cache dingo-gobuild-cache
rm -rf /tmp/dingo-testrunner /tmp/dingo-e2e
```

## Known gaps

- **GCS has no local test path.** The GCS plugin uses `storage.NewGRPCClient`
  (native gRPC API), which `fake-gcs-server` (the usual local emulator,
  HTTP/JSON-only) doesn't implement. CI has no GCS coverage either — this
  is a pre-existing gap, not something this change introduces. Only the
  framing-level unit tests (`writeRecord`/`readRecord`, cancellation) run
  without real credentials.
- **`postgresql-client-16` is a point-in-time pin, not a permanent
  guarantee.** It dumps from/restores into any currently-supported
  Postgres server at v16 or older; it cannot dump from a v17+ server
  (pg_dump's own version-mismatch guard). If that becomes a real
  requirement, bump the pin in `Dockerfile` and re-run this whole plan
  against the new target server version — restore compatibility is not
  guaranteed to be monotonic with client version (see below).

## Bugs found and fixed via this testing (for context)

None of these were caught by unit tests alone — all five needed a real
backend:

1. **pg_dump/pg_restore version mismatch.** Debian's bundled
   `postgresql-client` is v15 and refuses to dump from a v16+ server
   (pg_dump's own hard safety check). Fixed by pulling from the official
   PGDG apt repo. Then discovered the *opposite* problem: `pg_restore`
   v17 fails against a v16 server with `unrecognized configuration
   parameter "transaction_timeout"` (a v17-only GUC in its restore
   preamble) — so "always use latest" isn't safe either. Settled on
   `postgresql-client-16`, matching CI's `postgres:16` service.
2. **`pg_restore` needs `--dbname` explicitly** — it doesn't fall back to
   `PGDATABASE` alone; every restore failed with "one of -d/--dbname and
   -f/--file must be specified".
3. **`mysqldump --databases <db>`** embeds `CREATE DATABASE`/`USE <db>`
   naming the *source* database into the dump; restoring into a
   differently-named target silently landed data in a new database
   matching the source's name instead. Switched to the plain
   single-database dump form.
4. **Two pre-existing bugs in the S3 plugin** (not introduced by this
   change, but the first thing to actually exercise them against a real
   S3-compatible backend): `s3StreamIterator.advance()` panicked
   indexing an empty freshly-fetched page (iterating an empty
   prefix/bucket), and `isS3NotFound` didn't recognize `HeadObject`'s
   `NotFound` error type (only `GetObject`'s `NoSuchKey`), making every
   existence probe against a brand-new key fail with a hard error.
5. **The significant one**: `database/lifecycle/restore.go`'s restore
   orchestration briefly starts the metadata plugin to type-check it,
   then "undoes" that by wiping a directory — which does nothing for a
   live server (Postgres/MySQL had already run real migrations against
   the real target database by that point), so every real restore failed
   with "target database already contains tables". Fixed with a new
   `metadata.Resettable` interface (`Reset(ctx) error`) that Postgres/MySQL
   implement to drop their tables before restore proceeds.
