# Dingo Gov Lens

Dingo Gov Lens is a standalone Preview governance dashboard backed by Dingo's
Postgres metadata store. It does not call Dingo's Blockfrost, Mesh, or UTxO RPC
APIs. The app reads governance rows directly from Postgres through a small
read-only Go web server.

The example is intentionally isolated from the root Dingo module. Its
dependencies live in this directory's `go.mod`.

## What It Shows

- Chain freshness from `tip`, `epoch`, and `node_settings`
- Governance actions from `governance_proposal`
- Vote breakdowns from `governance_vote`
- Active DReps from `drep`
- DRep vote, update, registration, and delegation views
- Stake credential lookup from `account`, either pasted manually or derived
  locally from a CIP-30 wallet reward address, including the immutable slot at
  which Dingo first observed the account
- Links out to Preview GovTool for proposal and voting workflows

## Bootstrap Dingo Quickly

Run Dingo on Preview with `storageMode: api`, Postgres metadata, and Mithril
snapshot import. In API mode, `dingo mithril sync` imports the snapshot and then
backfills historical metadata, including governance votes and certificate
history.

Mithril makes the node usable at tip quickly, but vote rows come from
historical transaction replay. Until the metadata backfill reaches the
Conway-era proposal slots, Gov Lens shows a vote-backfill notice and reports
zero rows from `governance_vote`.

### Docker Compose

The consolidated examples Compose stack runs Gov Lens together with the other
example apps on one shared Preview Dingo node:

- `postgres`: Dingo metadata database
- `dingo-sync`: one-shot `dingo mithril sync` job
- `dingo`: Preview node using the same Postgres metadata DB
- `gov-lens`: Gov Lens web server using the read-only Postgres role

By default Compose builds Dingo from this checkout as
`dingo-examples-dingo:local`, so changes to Dingo's Postgres storage code are
covered by the E2E run. Set `DINGO_IMAGE=ghcr.io/blinklabs-io/dingo:<tag>` in
the environment if you explicitly want to test a published image instead.

```sh
cd examples
cp .env.example .env

# Change the default credentials (POSTGRES_PASSWORD,
# DINGO_GOV_LENS_PASSWORD, API keys, etc.) before any production deployment.
# The shipped values are weak local-development defaults only.

# This is the full bootstrap path; Mithril gets to tip quickly, then API-mode
# historical metadata backfill continues from the snapshot/checkpoint.
#
# dingo-sync is a one-shot `dingo mithril sync` job. The dingo service waits
# for it to complete successfully (depends_on service_completed_successfully)
# before running `dingo serve`, so a single `up` orchestrates the full order.
docker compose up -d
```

Open `http://127.0.0.1:8088`.

The app port is bound to `127.0.0.1` by default. Set
`GOV_LENS_BIND_ADDR=0.0.0.0` only when you intentionally want to expose the
dashboard outside the local machine.

For a one-command Gov Lens-only validation using the shared examples Compose
stack:

```sh
cd examples
cp .env.example .env
./dingo-gov-lens/scripts/e2e-compose.sh
```

If you change Postgres init credentials after the first run, reset the Compose
volumes before rerunning:

```sh
cd examples
docker compose down -v
```

### Local Dingo Binary

```sh
cd examples/dingo-gov-lens

export DATABASE_URL='host=127.0.0.1 port=5432 user=dingo password=change-me dbname=dingo_metadata sslmode=disable TimeZone=UTC'
export DINGO_BIN=/path/to/dingo

./scripts/mithril-sync.sh
./scripts/run-dingo.sh
```

The scripts set:

```sh
DINGO_STORAGE_MODE=api
DINGO_DATABASE_METADATA_PLUGIN=postgres
DINGO_DATABASE_METADATA_POSTGRES_DSN="$DATABASE_URL"
DINGO_DATABASE_BLOB_PLUGIN=badger
DINGO_UTXORPC_PORT=0
DINGO_BLOCKFROST_PORT=0
DINGO_MESH_PORT=0
```

Compose defaults `DINGO_DATABASE_WORKERS=16`, `DINGO_DATABASE_QUEUE_SIZE=500`,
and `DINGO_BACKFILL_BATCH_SIZE=1000` so Preview API backfill resumes faster than
the Dingo defaults. Lower them through environment variables or an `examples/.env`
file on constrained machines.

For Kubernetes, start from `k8s/dingo-values.yaml`. Replace the Postgres DSN
with your cluster-local database service and mount the configured CA certificate
from a Secret in your own chart overlay.

## Read-Only Database User

Create an application role that can only read the Dingo metadata schema:

```sh
psql "$ADMIN_DATABASE_URL" -f sql/create-readonly-user.sql
```

Edit the SQL first so the database name, Dingo owner role, and password match
your environment.
Use the read-only role in the app's `DATABASE_URL`.

## Run The App

```sh
cd examples/dingo-gov-lens

export DATABASE_URL='host=127.0.0.1 port=5432 user=dingo_gov_lens password=change-me dbname=dingo_metadata sslmode=disable TimeZone=UTC'
export ADDR=127.0.0.1:8088

./scripts/run-app.sh
```

Open `http://127.0.0.1:8088`.

## API

- `GET /api/status`
- `GET /api/proposals?lifecycle=active&action_type=6`
- `GET /api/proposals/{txHash}/{actionIndex}`
- `GET /api/dreps?active=true`
- `GET /api/dreps/{credentialHex}?credential_tag=0`
- `GET /api/stake/{stakeCredentialHex}?credential_tag=0`

All query handlers use direct SQL against Dingo metadata tables. The browser
never connects to Postgres directly.

## Local Checks

```sh
go test ./...
go build .
```

This example does not require a Dingo API port. GovTool remains the transaction
surface for signing, voting, and proposing; Gov Lens is the independent SQL
view over what Dingo has indexed on Preview.
