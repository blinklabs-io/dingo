# Dingo Blockfrost Explorer

This is a local blockchain explorer for Dingo's Blockfrost-compatible REST API.
The browser never calls hosted Blockfrost or any other chain data service. The
Vite dev server proxies `/health` and `/api/v0/*` to Dingo, and the app blocks
browser `fetch` calls to non-origin URLs.

## Dingo

Run Dingo on Preview with `storageMode: api` and the Blockfrost API enabled:

```sh
cd examples/dingo-blockfrost-explorer
DINGO_BIN=/path/to/dingo ./scripts/run-dingo.sh
```

The script sets:

```sh
CARDANO_NETWORK=preview
DINGO_STORAGE_MODE=api
DINGO_PLUGINS_API_BLOCKFROST_CONFIG_PORT=3000
DINGO_PLUGINS_API_UTXORPC_CONFIG_PORT=9090
DINGO_PLUGINS_API_MESH_CONFIG_PORT=8080
DINGO_PLUGINS_STORAGE_METADATA_PROVIDER=sqlite
DINGO_PLUGINS_STORAGE_BLOB_PROVIDER=badger
```

Equivalent YAML:

```yaml
storageMode: "api"
plugins:
  api:
    blockfrost:
      provider: builtin
      config: {port: 3000}
    utxorpc:
      provider: builtin
      config: {port: 9090}
    mesh:
      provider: builtin
      config: {port: 8080}
```

`storageMode: api` is required because the explorer reads transaction, address,
asset, metadata, account, DRep, and pool data from Dingo's API indexes.

### Dingo version

Use 0.69.0 or newer. The address summary, DRep list, and aligned DRep response
landed in 0.68.0. Pending Retirements (`GET /api/v0/pools/retiring`) and Pool
Metadata (`GET /api/v0/pools/{pool_id}/metadata`) landed in 0.69.0. Against an
older node, those views can return 404 or render unavailable fields.

### Kubernetes

The example Helm values use `ghcr.io/blinklabs-io/dingo:0.70.0`, enable all
API ports, and keep the chart-created Dingo Service internal to the cluster.
Apply the separate API Service when the frontend proxy needs a Kubernetes
LoadBalancer:

```sh
helm upgrade --install dingo-blockfrost-explorer blinklabs/dingo -f k8s/dingo-values.yaml
kubectl apply -f k8s/blockfrost-loadbalancer.yaml
```

The LoadBalancer manifest exposes Blockfrost `3000`, UTxO RPC `9090`, Mesh
`8080`, and metrics `12798`. The values set `DINGO_API_BIND_ADDR=0.0.0.0`
because a Service reaches the pod's own IP rather than its loopback, and the
API listeners bind `127.0.0.1` by default. A Dingo release that refuses an
unauthenticated non-loopback API bind additionally needs `api.auth`
configured against a mounted token file, which the pinned `0.70.0` image
predates.

### Docker Compose

The consolidated examples Compose stack runs the explorer together with the
other example apps on one shared Preview Dingo node. It runs a one-shot
`dingo mithril sync`, starts `dingo serve` after the sync job succeeds, and
serves the Vite explorer.

```sh
cd examples
cp .env.example .env
docker compose up -d
```

Open `http://127.0.0.1:5173`. `DINGO_BIND_ADDR` and
`BLOCKFROST_EXPLORER_BIND_ADDR` are the *host* addresses the ports are
published on; set either to `127.0.0.1` before `docker compose up` for
local-only bindings. Neither is the address Dingo binds inside its
container: the API listeners bind `127.0.0.1` by default and a container's
loopback is reachable only from inside that container, so the stack sets
`DINGO_API_BIND_ADDR=0.0.0.0` in the shared Dingo environment. That bind is
refused without authentication, so the stack also sets `DINGO_API_TOKEN`; the
Vite dev server attaches it as `Authorization: Bearer` on proxied requests
and the browser never sees it.

## Frontend

```sh
cd examples/dingo-blockfrost-explorer
npm install
DINGO_BLOCKFROST_URL=http://127.0.0.1:3000 \
DINGO_METRICS_URL=http://127.0.0.1:12798 \
  npm run dev -- --host 0.0.0.0
```

Open `http://127.0.0.1:5173`.

For a reachable Kubernetes service or another host, set `DINGO_BLOCKFROST_URL`
to that Dingo Blockfrost endpoint. The browser still talks only to the frontend
origin; Vite performs the proxy hop. `DINGO_METRICS_URL` is used only as a
local fallback to recover the current block height when `/api/v0/blocks/latest`
is temporarily unavailable.

## Explorer Views

- Dashboard landing view with latest chain status, charted recent block
  activity, block tempo, epoch activity, protocol snapshot, and recent blocks;
  slower supply and pool data load from their dedicated detail tabs
- Dedicated search results for block hashes/heights, transaction hashes,
  payment addresses, stake accounts, stake pools, asset IDs, DReps, `epoch:NN`,
  and `metadata:NN`; single-result searches open the matching detail view
- Latest block transaction hashes and transaction details
- Detail views for blocks, transactions, payment addresses, stake accounts,
  assets, epochs, network data, DReps, metadata labels, and stake pools
- Block details by height or hash, including block VRF, operational
  certificate, and operational certificate counter
- Transaction lookup with UTxOs, metadata, certificates, withdrawals, redeemers,
  pool updates/retires, metadata CBOR, transaction CBOR, required signers, and
  treasury donation; inputs are labelled spend, collateral, or reference
- Payment address summary with total balance, address type, script flag, and
  linked stake address, alongside the UTxO page, native assets, and transaction
  history
- Native asset details with CIP-25 metadata, aggregate mint/burn activity, and
  paginated current holder addresses
- Datum hashes, inline datums, and reference script hashes on address and
  transaction UTxOs
- Stake account summary, payment addresses, delegations, registrations, rewards,
  registration state, and the delegated DRep
- DRep list ordered by delegated voting power, with retired and expired state
  plus resolved CIP-119 anchor documents; a DRep ID or credential hex opens the
  single DRep view
- Stake pool list with the pending retirement schedule, and pool details with
  registered off-chain metadata, retirement epoch, and relays. A pool that has
  left the active set still resolves from its registered metadata

Views separate a Blockfrost 404 from a real failure, so a missing resource or a
route this Dingo build does not serve reads as "Not found" rather than a request
error.

The UI deliberately marks cexplorer-style aggregate views that Dingo's current
public Blockfrost surface does not expose yet, such as individual asset mint
transactions, pool delegators, arbitrary block transaction lists, script
indexes, and historical epoch block lists.

## Local Checks

```sh
npm run check
npm run build
```
