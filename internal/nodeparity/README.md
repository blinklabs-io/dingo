# Node parity

The live parity tests compare a running Dingo with a reference: a running
cardano-node's ledger state (protocol parameters, stake distribution, whole
UTxO set) over node-to-client LocalStateQuery for the check and watch tests,
or Koios for the from-genesis test. They are `go test` runs in this package,
each skipped unless its `DINGO_NODE_PARITY_*` settings are present; the
settings are documented on their constants in `live_parity_test.go`.

**Usage:** neither node is started or managed by the tests -- point them at
two already-running, already-synced NtC listeners. Each test runs until it
finishes or until go test's `-timeout` less one minute, so pass a `-timeout`
as long as the watch or replay should last.

Dingo's TCP node-to-client listener is disabled by default. The
`localhost:3002` examples below need `privatePort: 3002` (or
`CARDANO_PRIVATE_PORT=3002`); otherwise pass the path of `dingo.socket`.

```shell
# One comparison cycle; fails on divergence or a discarded cycle.
DINGO_NODE_PARITY_NETWORK=preview \
DINGO_NODE_PARITY_DINGO_ADDR=localhost:3002 \
DINGO_NODE_PARITY_CARDANO_ADDR=/path/to/cardano-node.socket \
  go test -count=1 -run '^TestLiveNodeParityCheck$' -v ./internal/nodeparity

# The same at an explicit historical block instead of the live tip.
DINGO_NODE_PARITY_AT_SLOT=122256283 \
DINGO_NODE_PARITY_AT_HASH=4c160b6b0f131bbba0845924b0db3f94ded5e3f806c8adac11482e41a0162a54 \
  ... go test -count=1 -run '^TestLiveNodeParityCheck$' -v ./internal/nodeparity

# DINGO_NODE_PARITY_WATCH is required for the watch test and selects the mode;
# unset, the test skips. A watch fails when it completes no comparison.
#
# Incremental watch for a day: one block at a time in chain order, with a
# full checkpoint every DINGO_NODE_PARITY_FULL_CHECK_INTERVAL blocks (1000)
# and a persisted cursor so a rerun resumes rather than re-baselining.
DINGO_NODE_PARITY_WATCH=incremental \
DINGO_NODE_PARITY_CURSOR_FILE=/var/lib/node-parity/preprod-cursor.json \
  ... go test -count=1 -timeout 24h -run '^TestLiveNodeParityWatch$' -v ./internal/nodeparity

# Full watch: a whole-ledger comparison on each tip change, with a fallback
# check every DINGO_NODE_PARITY_FALLBACK_INTERVAL (2m) and each cycle bounded
# by DINGO_NODE_PARITY_CHECK_TIMEOUT (20m).
DINGO_NODE_PARITY_WATCH=full \
  ... go test -count=1 -timeout 24h -run '^TestLiveNodeParityWatch$' -v ./internal/nodeparity

# From genesis against Koios. Optional: DINGO_NODE_PARITY_KOIOS_API_KEY or
# _KOIOS_API_KEY_FILE, _KOIOS_BASE_URL, _KOIOS_CACHE_PATH, _SKIP_UTXO,
# _BOUNDARY_CAPTURE_DIR, _VERBOSE, and _AT_SLOT/_AT_HASH to resume from a
# validated point.
DINGO_NODE_PARITY_FROM_GENESIS=true \
DINGO_NODE_PARITY_NETWORK=preview \
DINGO_NODE_PARITY_DINGO_ADDR=localhost:3002 \
  go test -count=1 -timeout 72h -run '^TestLiveNodeParityFromGenesis$' -v ./internal/nodeparity
```

A from-genesis run fails when it compares no epoch, for example when the Dingo
address is unreachable or the `-timeout` ends before the first epoch boundary.

Addresses accept either a `host:port` TCP address or a leading-`/` Unix
socket path (a real cardano-node's own NtC endpoint is normally a socket).
`DINGO_NODE_PARITY_NETWORK` is `preview`, `preprod` or `mainnet`. With
`DINGO_NODE_PARITY_DINGO_ADDR` set, `TestMain` keeps the production retry and
reconnect waits instead of the shortened ones the unit tests use.
