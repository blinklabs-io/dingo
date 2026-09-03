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

Before you begin, make sure you have:

- **Grafana 12+** (tested on 12.4) — [install guide](https://grafana.com/docs/grafana/latest/setup-grafana/installation/)
- **Prometheus** scraping your Dingo node on port `12798` (configured in step 2 below)
- **Dingo** node with metrics enabled (default port `12798`)
- **[Business Text plugin](https://grafana.com/grafana/plugins/marcusolsson-dynamictext-panel/)** — required by all dashboards for the header panel
- **[prometheus-node-exporter](https://github.com/prometheus/node_exporter)** — required only by the Resource Usage dashboard for host-level CPU, memory, and disk metrics

## Quick start

### 1. Install the Business Text plugin

Open a terminal on the machine running Grafana and run:

```bash
grafana-cli plugins install marcusolsson-dynamictext-panel
sudo systemctl restart grafana-server
```

If you are running Grafana in Docker or Kubernetes, set the environment variable instead:

```text
GF_INSTALL_PLUGINS=marcusolsson-dynamictext-panel
```

### 2. Configure Prometheus scrape targets

Prometheus needs to know where to find your Dingo node. Add the following to your `prometheus.yml` under `scrape_configs`:

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

Then reload Prometheus to apply the change:

```bash
sudo systemctl reload prometheus
```

### 3. Import dashboards

There are two ways to get the dashboards into Grafana.

**Option A: Provisioning (recommended)**

Provisioning automatically loads dashboards from files on disk. Grafana will pick them up on startup or within about 30 seconds if already running.

```bash
sudo cp docs/dashboards/provisioning.yaml /etc/grafana/provisioning/dashboards/dingo.yaml
sudo mkdir -p /var/lib/grafana/dashboards/dingo
sudo cp docs/dashboards/*.json /var/lib/grafana/dashboards/dingo/
sudo chown -R grafana:grafana /var/lib/grafana/dashboards/dingo/
sudo systemctl restart grafana-server
```

After restarting, go to **Dashboards → Browse** in the Grafana UI and search for "Dingo". All five dashboards appear in a **Dingo** folder automatically.

**Note:** `allowUiUpdates: true` lets you edit dashboards in the Grafana UI, but changes will be overwritten on the next provision cycle (every 30 seconds). To make permanent edits, modify the JSON files directly.

**Option B: Manual import**

1. Open Grafana in your browser
2. Go to **Dashboards → New → Import**
3. Upload each JSON file
4. Select your Prometheus datasource when prompted

### 4. Alert rules (optional)

The alert rules file is ready to use as-is — no editing required.

Copy the file to your Prometheus rules directory:

```bash
sudo cp docs/dashboards/alerts.yaml /etc/prometheus/rules/dingo.yaml
```

Then add it to `prometheus.yml`:

```yaml
rule_files:
  - /etc/prometheus/rules/dingo.yaml
```

Reload Prometheus:

```bash
sudo systemctl reload prometheus
```

## Datasource

All dashboards expect a Prometheus datasource with UID `prometheus`. If your datasource has a different UID, update it before importing or recreate the datasource with UID `prometheus`.

## Template variables

All dashboards include two template variables that auto-discover dingo instances:

| Variable | Query | Purpose |
|----------|-------|---------|
| `$network` | `label_values(dingo_build_info, network)` | Selects the Cardano network (`preview`, `preprod`, `mainnet`). |
| `$instance` | `label_values(dingo_build_info{network="$network"}, instance)` | Selects specific dingo instance(s). Multi-select with "All" default. |

`dingo_build_info` is a metric only dingo exposes (includes `network`, `version`, `commit` labels). This ensures dropdowns show only dingo targets, never Haskell cardano-node.

All panel queries use `{network="$network", instance=~"$instance"}` as their selector.

The **Resource Usage** dashboard adds a third variable:

| Variable | Query | Purpose |
|----------|-------|---------|
| `$node_instance` | `label_values(node_uname_info, instance)` | Selects the node_exporter target for host-level metrics (CPU, memory, disk, network). node_exporter runs on a different port (default `9100`) from the dingo metrics port, so it needs its own instance dropdown. |

Set `$node_instance` to the hostname/port reported by your node_exporter scrape target (typically `localhost:9100` or the host IP).

## Navigation

Dashboard-level links at the top of each dashboard connect all 5 dashboards. Links preserve the selected time range and template variables.

## Dormant metrics

Some metrics only emit when their feature is active. Panels display "No data" until activated:

| Metric | Activates when |
|--------|---------------|
| `cardano_node_metrics_Forge_node_is_leader_int` | Forging is enabled |
| `cardano_node_metrics_Forge_adopted_int` | Block is forged and adopted |
| `dingo_metrics_blockForgingLatency_seconds_bucket` | Block is forged |
| `cardano_node_metrics_remainingKESPeriods_int` | KES key is configured |
| `dingo_forge_tip_gap_slots` | Forging is enabled |
| `dingo_database_size_bytes` | Database stores are initialized |
| `database_blob_*` | Badger blob store is active |

## File listing

```text
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

## Troubleshooting

**Business Text plugin not found / panels show "Panel plugin not found: marcusolsson-dynamictext-panel"**
Install the plugin (step 1 above) and restart Grafana. If running in a managed environment, add `marcusolsson-dynamictext-panel` to your `GF_INSTALL_PLUGINS` list.

**Panels show "No data"**
Check that Prometheus is scraping your Dingo node: open Prometheus in the browser, go to **Status → Targets**, and confirm the `dingo` job is up. Also verify the `$network` and `$instance` template variables at the top of the dashboard are set to the correct values.

**Datasource UID mismatch ("datasource not found")**
All JSON files use the datasource UID `prometheus`. If your Prometheus datasource has a different UID, either rename it to `prometheus` in **Connections → Data sources**, or do a find-and-replace of `"prometheus"` with your actual UID in the JSON files before importing.

**Dashboards not appearing after provisioning**
Confirm `/etc/grafana/provisioning/dashboards/dingo.yaml` references the correct path. Grafana auto-reloads provisioned dashboards every 30 seconds; if it hasn't appeared after a minute, check the Grafana log (`journalctl -u grafana-server`) for errors.
