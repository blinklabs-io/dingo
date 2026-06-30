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
DINGO_BLOCKFROST_PORT=3000
DINGO_UTXORPC_PORT=9090
DINGO_MESH_PORT=8080
DINGO_DATABASE_METADATA_PLUGIN=sqlite
DINGO_DATABASE_BLOB_PLUGIN=badger
```

Equivalent YAML:

```yaml
storageMode: "api"
blockfrostPort: 3000
utxorpcPort: 9090
meshPort: 8080
```

`storageMode: api` is required because the explorer reads transaction, address,
asset, metadata, account, DRep, and pool data from Dingo's API indexes.

### Kubernetes

The example Helm values use `ghcr.io/blinklabs-io/dingo:0.52.0`, enable all
API ports, and keep the chart-created Dingo Service internal to the cluster.
Apply the separate API Service when the frontend proxy needs a Kubernetes
LoadBalancer:

```sh
helm upgrade --install dingo-blockfrost-explorer blinklabs/dingo -f k8s/dingo-values.yaml
kubectl apply -f k8s/blockfrost-loadbalancer.yaml
```

The LoadBalancer manifest exposes Blockfrost `3000`, UTxO RPC `9090`, Mesh
`8080`, and metrics `12798`.

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

Open `http://127.0.0.1:5173`. Set `DINGO_BIND_ADDR=127.0.0.1` or
`BLOCKFROST_EXPLORER_BIND_ADDR=127.0.0.1` before `docker compose up` if you
want local-only bindings.

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
- Block details by height or hash
- Transaction lookup with UTxOs, metadata, certificates, withdrawals, redeemers,
  pool updates/retires, metadata CBOR, transaction CBOR, and required signers
- Payment address visible balance, UTxOs, native assets, and transaction history
- Stake account summary, payment addresses, delegations, registrations, rewards

The UI deliberately marks cexplorer-style aggregate views that Dingo's current
public Blockfrost surface does not expose yet, such as asset holder lists,
asset mint history, pool delegators, arbitrary block transaction lists, script
indexes, and historical epoch block lists.

## Local Checks

```sh
npm run check
npm run build
```
