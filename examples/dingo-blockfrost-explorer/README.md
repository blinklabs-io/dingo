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
DINGO_UTXORPC_PORT=0
DINGO_MESH_PORT=0
DINGO_DATABASE_METADATA_PLUGIN=sqlite
DINGO_DATABASE_BLOB_PLUGIN=badger
```

Equivalent YAML:

```yaml
storageMode: "api"
blockfrostPort: 3000
utxorpcPort: 0
meshPort: 0
```

`storageMode: api` is required because the explorer reads transaction, address,
asset, metadata, account, DRep, and pool data from Dingo's API indexes.

### Kubernetes

The example Helm values keep the chart-created Dingo Service internal to the
cluster. Apply the separate Blockfrost Service when the frontend proxy needs a
Kubernetes LoadBalancer:

```sh
helm upgrade --install dingo-blockfrost-explorer blinklabs/dingo -f k8s/dingo-values.yaml
kubectl apply -f k8s/blockfrost-loadbalancer.yaml
```

The LoadBalancer manifest exposes only port `3000`.

## Frontend

```sh
cd examples/dingo-blockfrost-explorer
npm install
DINGO_BLOCKFROST_URL=http://127.0.0.1:3000 npm run dev -- --host 0.0.0.0
```

Open `http://127.0.0.1:5173`.

For a reachable Kubernetes service or another host, set `DINGO_BLOCKFROST_URL`
to that Dingo Blockfrost endpoint. The browser still talks only to the frontend
origin; Vite performs the proxy hop.

## Explorer Views

- Latest chain status, epoch, network supply, genesis timing, and sampled pools
- Latest block transaction hashes and transaction details
- Block lookup by height or hash
- Transaction lookup with UTxOs, metadata, certificates, withdrawals, redeemers,
  and required signers
- Payment address UTxOs and transaction history
- Stake account summary, payment addresses, delegations, registrations, rewards
- Asset, DRep, metadata label, and stake pool lookups

## Local Checks

```sh
npm run check
npm run build
```
