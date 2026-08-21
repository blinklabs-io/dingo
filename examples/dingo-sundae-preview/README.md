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

The example values pin Dingo `0.70.0`. The chart uses a `LoadBalancer` service
and enables only `DINGO_PLUGINS_API_UTXORPC_CONFIG_PORT`.
If the load balancer is not reachable from the dev host, keep the port-forward
open while running the frontend.

Dingo `0.68.0` or newer is required. Earlier releases decoded the UTxO RPC
`exact_address` search field as CBOR, so the raw address bytes that Blaze and
the UTxO RPC SDK send were rejected, and the `payment_part` and
`delegation_part` fields were decoded as whole addresses instead of 28-byte
credentials. The address-scoped queries described below only work against
`0.68.0` or newer.

The app talks plain HTTP to Dingo through the Vite dev proxy. If you put Dingo
behind its own TLS certificate instead, note that its UTxO RPC listener now
requires TLS 1.2 or newer.

## Frontend

Install and run the app:

```sh
npm install
DINGO_UTXORPC_URL=http://127.0.0.1:9090 npm run dev -- --host 0.0.0.0
```

For a reachable LoadBalancer service, replace `DINGO_UTXORPC_URL` with that
service URL, for example `http://192.168.4.211:9090`.

The Vite dev server proxies UTxO RPC paths to Dingo, so the browser talks to the
frontend origin while Dingo remains the only chain API. On startup the app asks
Dingo for its genesis and refuses to continue unless the endpoint reports the
Preview network magic, because the Sundae V3 script hashes it uses are Preview
only. It then scans Sundae V3 pool UTxOs through UTxO RPC, fills the pool
dropdown, and shows reserve-derived spot prices and a price-impact chart. Until
then it keeps a curated Preview pool fallback list. Connect a Preview-capable
CIP-30 wallet, load a pool, choose either ADA-to-token or token-to-ADA,
build/evaluate the order, then sign and submit through Dingo.

## Wallet view from Dingo

The wallet dropdown shows what Dingo itself holds for the connected wallet,
next to what CIP-30 reports, so the two can be compared:

- `Dingo UTxOs` is an exact-address search. Dingo compares the complete
  serialized address bytes, so a base address does not pick up enterprise UTxOs
  that merely share its payment credential.
- `Stake cred` is a delegation-part search on the address stake credential. It
  covers every address form that shares that credential, so it spans the whole
  wallet rather than one address. It resolves at most the first 200 UTxOs and
  says so when it truncates.

Both refresh on connect and after a submitted order confirms.

## What UTxO RPC does not answer

Live staking state is not part of the UTxO RPC surface. Dingo answers
stake-address-info and stake-snapshot queries over node-to-client
LocalStateQuery only, so this app cannot show delegated stake, the delegated
pool, available rewards, or DRep delegation. The stake-credential row above is a
UTxO view of a stake credential, not a stake-address-info answer. A client that
needs those values has to speak node-to-client to Dingo instead.

The UTxO RPC surface Dingo exposes today is `ReadParams`, `ReadEraSummary`,
`ReadUtxos`, `SearchUtxos`, `ReadData`, `ReadTx`, and `ReadGenesis` on the query
service; `SubmitTx`, `WaitForTx`, `EvalTx`, `ReadMempool`, and `WatchMempool` on
the submit service; `FetchBlock`, `DumpHistory`, `FollowTip`, and `ReadTip` on
the sync service; and `WatchTx` on the watch service. This app uses the query
and submit services.

Order submission goes to Dingo's mempool, which now runs a configurable FIFO
backend with double-buffered revalidation on the node side. That is a node
configuration concern with no client-visible RPC change: `SubmitTx` still
returns the transaction hash and `WaitForTx` still streams to the confirmed
stage.

## Dependency note

`@blaze-cardano/sdk` is held at 0.2.48 on purpose. Both `@sundaeswap/core`
2.12.0 and `@utxorpc/blaze-provider` 0.3.9 depend on 0.2.48, so pinning the
direct dependency to the same version leaves exactly one copy of the SDK in
the tree.

Do not bump this to 0.3.x on its own. A 0.3.x direct dependency makes npm
install a second copy of the SDK, and the `Blaze` instance this app builds
then becomes a different nominal type from the one `TxBuilderV3` expects,
which breaks `npm run build`. Casting past that error compiles but is worse
than the error: it hands a 0.3.x object to code built against 0.2.48 inside
the swap path, where a mismatch would surface as a malformed transaction
rather than a type failure.

Bump it when `@sundaeswap/core` and `@utxorpc/blaze-provider` move up, so all
three stay on one version.
