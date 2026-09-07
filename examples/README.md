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

Building from the checkout is the recommended path when exercising the current
source tree. Published images are compatible with the full shared stack from
0.70.0 onward. The explorer's address summary, DRep list, exact-address UTxO
matching, retiring-pools, and pool-metadata views are available from 0.69.0
onward.

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

`DINGO_BIND_ADDR` is the *host* address the Dingo ports are published on;
set it to `127.0.0.1` if they should be local-only. It is not the address
Dingo binds inside its container. The API listeners bind `127.0.0.1` by
default and a container's loopback is reachable only from inside that
container, so the stack sets `DINGO_API_BIND_ADDR=0.0.0.0` in the shared
Dingo environment — without it neither the published ports nor the
`blockfrost-explorer` and `sundae-preview` services could reach the API.

Reset the shared Dingo/Postgres state:

```sh
docker compose down -v
```
