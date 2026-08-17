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
- Active DReps from `drep`, plus how many registered DReps have expired by
  inactivity and are therefore excluded from the voting-power denominator
- DRep vote, update, registration, and delegation views
- Per-DRep inactivity expiry: the epochs left before `expiry_epoch` is reached,
  a filter for expired vs unexpired DReps, the credential's first on-chain
  appearance, and the slot of its most recent real registration certificate
- Retained epoch and reward state per epoch: ADA pots, leader-election and
  reward-basis stake totals, pool and delegator counts, whether the boundary
  snapshot was captured authoritatively, and how many per-pool reward results
  exist
- Stake credential lookup from `account`, either pasted manually or derived
  locally from a CIP-30 wallet reward address, including the immutable slot at
  which Dingo first observed the account, its CIP-0163 inactivity expiry, its
  per-epoch reward outputs, and its reward-withdrawal witnesses
- Links out to Preview GovTool for proposal and voting workflows

### Inactivity And Expiry

Two separate expiry mechanisms show up in the UI, and neither is the same thing
as a registration being active.

A DRep's `active` flag only clears on deregistration. Inactivity expiry is
separate: a DRep vote, registration, or update certificate sets
`last_activity_epoch` and pushes `expiry_epoch` out by the protocol's DRep
inactivity period, and the Conway tally drops a DRep whose `expiry_epoch` has
been reached. The DReps table shows both, so a DRep can read active and expired
at the same time. The expiry filter uses the same test the tally uses: a zero
`expiry_epoch` means no activity has been recorded yet and counts as unexpired.

Reward-account inactivity is the CIP-0163 equivalent for stake credentials, held
behind a node flag. With the flag off, every `account.expiration_epoch` is 0 and
the stake lookup reports the expiry as not enabled. Enabling it also requires a
from-genesis sync, because the column cannot be reconstructed from a Mithril
snapshot, so the Compose stack in this directory always reports it as not
enabled.

The withdrawal history below it depends on that same flag. Dingo only records a
witness row (`account_withdrawal_witness`) for a withdrawal when the flag is
on -- with it off, as on this Compose stack, that write is skipped entirely, so
a withdrawal that moved no reward leaves no trace anywhere and cannot appear.
Withdrawals that moved a nonzero reward still show up either way: the UI falls
back to the reward journal (`account_reward_delta`, written regardless of the
flag) for any withdrawal the witness table didn't record. So on this Compose
stack you will see every nonzero withdrawal but never a zero-amount one; with
the flag on, both appear, and a zero-amount entry is marked as such.

### Retention

Epoch and reward history is retained asymmetrically, which decides how far back
each panel can look:

- Retained for the life of the database: `epoch`, `epoch_summary`,
  `reward_ada_pots`, `reward_snapshot`, `reward_pool_input`, and
  `reward_pool_output`. These are one row per epoch, or roughly one row per pool
  per epoch, and they are what the Epochs panel reads.
- Pruned at every epoch transition to the last four epochs:
  `pool_stake_snapshot`, `reward_stake_input`, and `reward_account_output`.
  These scale with delegator count.

So the Epochs panel reaches back to the first boundary the node captured, while
the per-epoch reward outputs in the stake lookup only cover the current epoch and
the three before it. An older epoch showing no reward rows for an account is
retention, not a missing calculation.

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
DINGO_PLUGINS_STORAGE_METADATA_PROVIDER=postgres
DINGO_PLUGINS_STORAGE_METADATA_CONFIG_DSN="$DATABASE_URL"
DINGO_PLUGINS_STORAGE_BLOB_PROVIDER=badger
DINGO_PLUGINS_API_UTXORPC_CONFIG_PORT=0
DINGO_PLUGINS_API_BLOCKFROST_CONFIG_PORT=0
DINGO_PLUGINS_API_MESH_CONFIG_PORT=0
```

Compose defaults `DINGO_DATABASE_WORKERS=16`, `DINGO_DATABASE_QUEUE_SIZE=500`,
and `DINGO_BACKFILL_BATCH_SIZE=1000` so Preview API backfill resumes faster than
the Dingo defaults. Lower them through environment variables or an `examples/.env`
file on constrained machines.

For Kubernetes, start from `k8s/dingo-values.yaml`. Replace the Postgres DSN
with your cluster-local database service and mount the configured CA certificate
from a Secret in your own chart overlay.

### Dingo version

Use 0.68.0 or newer. DRep activity renewal from certificates landed in 0.68.0,
and without it `drep.expiry_epoch` is not maintained from vote and update
certificates, so the expiry column and the `expiry` filter report stale
answers rather than wrong-looking ones. CIP-0163 account expiry columns need
0.67.0, and the reward metadata tables need 0.65.0.

The Epochs tab reaches only the last four epochs on a released node. Retaining
`epoch_summary` and the per-epoch reward tables for the life of the database is
newer than 0.68.0 and not in a tagged release yet; before it, those rows were
pruned to `epoch < current-3` at every epoch transition. The tab works either
way, it just shows a short window until you run a node built from `main`.

## Read-Only Database User

Create an application role that can only read the Dingo metadata schema:

```sh
psql "$ADMIN_DATABASE_URL" -f sql/create-readonly-user.sql
```

Edit the SQL first so the database name, Dingo owner role, and password match
your environment.
Use the read-only role in the app's `DATABASE_URL`.

The grants are schema-wide rather than a table list: `GRANT SELECT ON ALL TABLES
IN SCHEMA public` covers whatever exists when the script runs, and the
`ALTER DEFAULT PRIVILEGES FOR ROLE <dingo owner>` line covers every table Dingo
creates later. Tables added by a Dingo upgrade, including the reward and
inactivity tables this app reads, are therefore readable without editing the
script. The same is true of `docker/postgres-init/001-create-readonly-user.sh`,
which runs at cluster init before Dingo has created any table.

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
- `GET /api/dreps?active=true&expiry=expired`
- `GET /api/dreps/{credentialHex}?credential_tag=0`
- `GET /api/stake/{stakeCredentialHex}?credential_tag=0`
- `GET /api/epochs?limit=50`

`expiry` accepts `expired` or `active` and can be combined with `active`, which
filters on registration instead.

`/api/status` reports `expiredDrepCount`, `latestRewardEpoch` (the newest epoch
with captured ADA pots), and `accountInactivity`, which says whether the one-time
CIP-0163 activation has run and in which epoch.

`/api/stake/{credential}` adds `expirationEpoch`, `inactivityActivated`,
`rewards` (per-epoch reward outputs within the retained window), and
`withdrawals` (reward withdrawals, `zeroAmount` set when the withdrawal moved
nothing -- see "Inactivity And Expiry" above for when zero-amount withdrawals
are visible at all).

All query handlers use direct SQL against Dingo metadata tables. The browser
never connects to Postgres directly.

Tables read: `node_settings`, `commit_timestamp`, `tip`, `epoch`,
`epoch_summary`, `backfill_checkpoint`, `sync_state`, `governance_proposal`,
`governance_vote`, `drep`, `registration_drep`, `update_drep`,
`deregistration_drep`, `vote_delegation`, `stake_vote_delegation`,
`vote_registration_delegation`, `stake_vote_registration_delegation`, `certs`,
`transaction`, `account`, `account_inactivity_activation`,
`account_withdrawal_witness`, `account_reward_delta`, `reward_ada_pots`,
`reward_snapshot`, `reward_pool_output`, and `reward_account_output`.

## Local Checks

```sh
go test ./...
go build .
```

This example does not require a Dingo API port. GovTool remains the transaction
surface for signing, voting, and proposing; Gov Lens is the independent SQL
view over what Dingo has indexed on Preview.
