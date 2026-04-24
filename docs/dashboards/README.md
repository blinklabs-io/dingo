# Dingo Grafana Dashboards

Monitoring dashboards for [Dingo](https://github.com/blinklabs-io/dingo), the Go Cardano node by Blink Labs.

## Dashboards

| Dashboard | File | Description |
|-----------|------|-------------|
| **Node Overview** | `node-overview.json` | At-a-glance header (epoch, slot, tip gap, density, blockfetch, mempool, KES, peers, storage, runtime), block height, epoch progress, forging stats |
| **Block Production** | `block-production.json` | Forge latency heatmap, block delay CDFs, forge rates, KES lifecycle |
| **Peer Health** | `peer-health.json` | Hot/warm/cold peers, connection types, peer state timeseries, promotions/demotions, network I/O |
| **Mempool** | `mempool.json` | Pending TXs, mempool size, TX lifecycle, CBOR cache, event bus |
| **Resource Usage** | `resources.json` | CPU, memory, GC heatmap, disk I/O, IOPS, PSI pressure, Badger, file descriptors |

`full-panel.json` contains the standalone Business Text header panel for embedding into custom dashboards.

## Prerequisites

- **Grafana** 10+ (tested on 12.4)
- **Prometheus** datasource with UID `prometheus`
- **Dingo** node with metrics enabled (default port `12798`)
- **[Business Text](https://grafana.com/grafana/plugins/marcusolsson-dynamictext-panel/)** plugin — required by all dashboards for header panels
- **[prometheus-node-exporter](https://github.com/prometheus/node_exporter)** — required by Resource Usage dashboard for host-level CPU, memory, and disk metrics

## Quick start

### 1. Install the Business Text plugin

```bash
grafana-cli plugins install marcusolsson-dynamictext-panel
sudo systemctl restart grafana-server
```

Or via `GF_INSTALL_PLUGINS` environment variable (Docker/Kubernetes):

```
GF_INSTALL_PLUGINS=marcusolsson-dynamictext-panel
```

### 2. Configure Prometheus scrape targets

Add to your `prometheus.yml` under `scrape_configs`:

```yaml
- job_name: dingo
  scrape_interval: 15s
  static_configs:
    - targets: ['localhost:12798']

# Optional: required for Resource Usage dashboard
- job_name: node
  scrape_interval: 15s
  static_configs:
    - targets: ['localhost:9100']
```

```bash
sudo systemctl reload prometheus
```

### 3. Import dashboards

**Option A: Provisioning (recommended)**

```bash
sudo cp docs/dashboards/provisioning.yaml /etc/grafana/provisioning/dashboards/dingo.yaml
sudo mkdir -p /var/lib/grafana/dashboards/dingo
sudo cp docs/dashboards/*.json /var/lib/grafana/dashboards/dingo/
sudo chown -R grafana:grafana /var/lib/grafana/dashboards/dingo/
sudo systemctl restart grafana-server
```

Dashboards appear in a **Dingo** folder automatically.

**Option B: Manual import**

1. Open Grafana
2. Go to **Dashboards > New > Import**
3. Upload each JSON file
4. Select your Prometheus datasource when prompted

### 4. Alert rules (optional)

```bash
sudo cp docs/dashboards/alerts.yaml /etc/prometheus/rules/dingo.yml
```

Add to `prometheus.yml`:

```yaml
rule_files:
  - /etc/prometheus/rules/dingo.yml
```

## Datasource

All dashboards expect a Prometheus datasource with UID `prometheus`. If your datasource has a different UID, update it before importing or recreate the datasource with UID `prometheus`.

## Template variables

All dashboards include two template variables that auto-discover dingo instances:

| Variable | Query | Purpose |
|----------|-------|---------|
| `$network` | `label_values(dingo_build_info, network)` | Selects the Cardano network (`preview`, `preprod`, `mainnet`). |
| `$instance` | `label_values(dingo_build_info{network="$network"}, instance)` | Selects specific dingo instance(s). Multi-select with "All" default. |

`dingo_build_info` is a metric only dingo exposes (includes `network`, `version`, `revision` labels). This ensures dropdowns show only dingo targets, never Haskell cardano-node.

All panel queries use `{network="$network", instance=~"$instance"}` as their selector.

## Navigation

Dashboard-level links at the top of each dashboard connect all 5 dashboards. Links preserve the selected time range and template variables.

## Dormant metrics

Some metrics only emit when their feature is active. Panels display "No data" until activated:

| Metric | Activates when |
|--------|---------------|
| `cardano_node_metrics_Forge_node_is_leader_int` | Forging is enabled |
| `cardano_node_metrics_Forge_adopted_int` | Block is forged and adopted |
| `cardano_node_metrics_blockForgingLatency_seconds_bucket` | Block is forged |
| `cardano_node_metrics_remainingKESPeriods_int` | KES key is configured |
| `dingo_forge_tip_gap_slots` | Forging is enabled |
| `dingo_database_size_bytes` | Database stores are initialized |
| `database_blob_*` | Badger blob store is active |

## File listing

```
docs/dashboards/
  node-overview.json       Node Overview dashboard
  block-production.json    Block Production dashboard
  peer-health.json         Peer Health dashboard
  mempool.json             Mempool dashboard
  resources.json           Resource Usage dashboard
  full-panel.json          Business Text header panel (standalone)
  provisioning.yaml        Grafana provisioning config
  prometheus.yaml          Prometheus scrape config snippet
  alerts.yaml              Prometheus alert rules
  README.md                This file
```
