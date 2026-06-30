# DingoSwap

This is a local-only frontend example for building a real SundaeSwap V3 Preview
order transaction with a CIP-30 wallet. Runtime chain access goes through Dingo
UTxO RPC only. The app blocks browser `fetch` calls to non-origin URLs to catch
accidental hosted API usage.

## Dingo

The consolidated examples Compose stack runs Dingo on Preview with UTxO RPC
enabled and serves this app together with the other example apps:

```sh
cd examples
cp .env.example .env
docker compose up -d
```

Open `http://127.0.0.1:5174`.

For Kubernetes, deploy Dingo on Preview:

```sh
cd examples/dingo-sundae-preview
./scripts/deploy-dingo.sh
./scripts/port-forward-dingo.sh
```

The chart uses a `LoadBalancer` service and enables only `DINGO_UTXORPC_PORT`.
If the load balancer is not reachable from the dev host, keep the port-forward
open while running the frontend.

## Frontend

Install and run the app:

```sh
npm install
DINGO_UTXORPC_URL=http://127.0.0.1:9090 npm run dev -- --host 0.0.0.0
```

For a reachable LoadBalancer service, replace `DINGO_UTXORPC_URL` with that
service URL, for example `http://192.168.4.211:9090`.

The Vite dev server proxies UTxO RPC paths to Dingo, so the browser talks to the
frontend origin while Dingo remains the only chain API. When Dingo is ready, the
app scans Sundae V3 pool UTxOs through UTxO RPC, fills the pool dropdown, and
shows reserve-derived spot prices and a price-impact chart. Until then it keeps
a curated Preview pool fallback list. Connect a Preview-capable CIP-30 wallet,
load a pool, choose either ADA-to-token or token-to-ADA, build/evaluate the
order, then sign and submit through Dingo.
