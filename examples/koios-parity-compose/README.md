# Koios-Parity Compose Stack

A local dev stack for running a Dingo Preview-network sync with live
observability, and an easy on/off toggle for the in-process Koios
reward-parity validator.

Builds `dingo` from this checkout's own `Dockerfile` (no external image), plus
Prometheus and Grafana, both provisioned via file rather than manual
click-ops.

## Run it

```sh
cd examples/koios-parity-compose
docker compose up -d
```

Open:

- Grafana: `http://127.0.0.1:13930` (`admin` / `admin`, per `GF_SECURITY_ADMIN_USER`/
  `GF_SECURITY_ADMIN_PASSWORD` below -- change on first login before using this
  outside a trusted local machine)
- Prometheus: `http://127.0.0.1:13900` (targets: `http://127.0.0.1:13900/targets`)

The Grafana instance is pre-provisioned with a Prometheus datasource and a
"Dingo Preview Sync Progress" dashboard (folder: **Dingo**) showing:

- Chain tip slot number and block number over time
- Block apply rate (derived from block-height growth; see the panel
  description for why this doesn't use `dingo_ledger_block_pipeline_*`)
- Slots behind wall clock (`dingo_tip_gap_slots`), which trends toward 0 as
  sync completes
- Current epoch, chainsync header-cache size, and on-disk database size

Tear down (including synced chain data):

```sh
docker compose down -v
```

## Koios-parity toggle

`entrypoint-wrapper.sh` wraps the image's own `/bin/entrypoint.sh` (unchanged)
and decides which `--koios-parity-*` CLI flags to hand it, based on one
environment variable:

```sh
# On: validates closed-epoch reward state against Koios preview reference
# data as the node advances. Passes:
#   --koios-parity-enabled --koios-parity-strict=false
#   --koios-parity-base-url https://preview-koios.tosidrop.me/api/v1
KOIOS_PARITY_ENABLED=true docker compose up -d

# Off (default): no --koios-parity-* flags are passed at all.
docker compose up -d
```

`--koios-parity-strict=false` is deliberate here: this stack is for local
observability, not enforcement, so a Koios mismatch or transient API error is
logged rather than stopping the node. `dingo`'s own `--koios-parity-strict`
flag defaults to `true`.

Override the reference API with `KOIOS_PARITY_BASE_URL` if needed; it is only
read when the toggle is on.

**Verifying the toggle**: `docker compose logs dingo | grep "koios parity"`.
With the toggle on, dingo logs `koios parity observer enabled` (with
`network`, `strict`, `accounts` fields) at startup. With it off, that line
never appears and the container's process arguments carry no
`--koios-parity-*` flags (`docker compose exec dingo ps -o args= 1` or
`docker inspect` on the running container).

Reward parity is only checked at closed-epoch boundaries, so a short
validation run from genesis will show the observer enabled in the logs
without necessarily reaching its first check -- Preview epochs are ~5 days.

## Ports

All ports are overridable and distinct from the repo's root
`docker-compose.yml` and `examples/docker-compose.yml` defaults, so this stack
can run alongside either without conflict.

| Env var | Default | Service | Purpose |
|---|---|---|---|
| `DINGO_RELAY_PORT` | 13701 | dingo | Ouroboros NtN (relay) |
| `DINGO_METRICS_PORT` | 13798 | dingo | Prometheus metrics (`/metrics`), localhost only |
| `PROMETHEUS_PORT` | 13900 | prometheus | Prometheus UI |
| `GRAFANA_PORT` | 13930 | grafana | Grafana UI |

Prometheus reaches dingo over the compose network at `dingo:12798` regardless
of `DINGO_METRICS_PORT`; that mapping is only for a human operator to query
`/metrics` directly from the host.

## Files

```
docker-compose.yml            # dingo + prometheus + grafana
entrypoint-wrapper.sh          # koios-parity CLI flag toggle (see above)
prometheus/prometheus.yml      # scrape config (targets dingo:12798)
grafana/provisioning/          # datasource + dashboard-provider config
grafana/dashboards/            # the starter dashboard JSON
```
