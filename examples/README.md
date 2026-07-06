# Dingo Examples

The examples can share one Preview Dingo node, one Postgres metadata store, and
one Mithril bootstrap job.

## Docker Compose

Run every example app together:

```sh
cd examples
cp .env.example .env
docker compose up -d
```

Open:

- Gov Lens: `http://127.0.0.1:8088`
- Blockfrost Explorer: `http://127.0.0.1:5173`
- Sundae Preview: `http://127.0.0.1:5174`

The shared stack runs:

- `postgres`: Dingo metadata database with a read-only Gov Lens role
- `dingo-sync`: one-shot `dingo mithril sync` job
- `dingo`: Preview node using Postgres metadata, Badger blob storage, and API mode
- `gov-lens`: Go web app that reads governance rows from Postgres
- `blockfrost-explorer`: Vite app proxying to Dingo's Blockfrost API
- `sundae-preview`: Vite app proxying to Dingo's UTxO RPC API

By default Compose builds Dingo from this checkout as
`dingo-examples-dingo:local`. Set `DINGO_IMAGE=ghcr.io/blinklabs-io/dingo:<tag>`
if you explicitly want to use a published image.

The shipped database credentials are local-development defaults. Change
`POSTGRES_PASSWORD` and `DINGO_GOV_LENS_PASSWORD` before exposing the stack
outside a trusted development machine.

Useful port overrides:

```sh
GOV_LENS_PORT=18088 \
BLOCKFROST_EXPLORER_PORT=15173 \
SUNDAE_PREVIEW_PORT=15174 \
docker compose up -d
```

Set `DINGO_BIND_ADDR=127.0.0.1` if Dingo API ports should be local-only.

Reset the shared Dingo/Postgres state:

```sh
docker compose down -v
```
