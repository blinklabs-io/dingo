#!/usr/bin/env bash
# Import Dingo dashboards to Grafana Cloud under the Dingo folder.
#
# Usage:
#   GRAFANA_TOKEN=<service-account-token> ./import-to-cloud.sh
#
# Optional overrides:
#   GRAFANA_URL=https://blinklabsio.grafana.net   (default)
#   FOLDER_UID=fe24y2dswncowc                      (default — the Blink Labs > Dingo folder)
#   DS_UID=grafanacloud-blinklabsio-prom           (find at Connections → Data Sources → your Prometheus → UID)

set -euo pipefail

GRAFANA_URL="${GRAFANA_URL:-https://blinklabsio.grafana.net}"
FOLDER_UID="${FOLDER_UID:-fe24y2dswncowc}"
DS_UID="${DS_UID:-}"

if [[ -z "${GRAFANA_TOKEN:-}" ]]; then
  echo "ERROR: set GRAFANA_TOKEN to a Grafana service account token with Editor role" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Discover actual datasource UID if not provided
if [[ -z "$DS_UID" ]]; then
  echo "DS_UID not set — querying Grafana for a Prometheus datasource..."
  DS_UID=$(curl -sf \
    -H "Authorization: Bearer ${GRAFANA_TOKEN}" \
    "${GRAFANA_URL}/api/datasources" \
    | python3 -c "
import sys, json
sources = json.load(sys.stdin)
prom = [s for s in sources if s.get('type') == 'prometheus']
if not prom:
    print('ERROR: no Prometheus datasource found', file=sys.stderr)
    sys.exit(1)
print(prom[0]['uid'])
")
  echo "Found datasource UID: ${DS_UID}"
fi

DASHBOARDS=(
  block-production.json
  mempool.json
  node-overview.json
  peer-health.json
  resources.json
)

for f in "${DASHBOARDS[@]}"; do
  path="${SCRIPT_DIR}/${f}"
  echo -n "Importing ${f} ... "

  # Replace the hardcoded datasource UID "prometheus" with the real one, then
  # wrap in the import envelope Grafana expects.
  # Note: panels use "prometheus" as the UID so file provisioning works on
  # self-hosted Grafana (where the default Prometheus datasource UID is
  # "prometheus"). On Grafana Cloud the UID is different, hence this swap.
  payload=$(python3 - <<PYEOF
import json, sys

with open("${path}") as fh:
    dash = json.load(fh)

def fix(obj):
    if isinstance(obj, dict):
        if obj.get("uid") == "prometheus" and obj.get("type") in (None, "prometheus", "datasource"):
            obj["uid"] = "${DS_UID}"
        for v in obj.values():
            fix(v)
    elif isinstance(obj, list):
        for item in obj:
            fix(item)

fix(dash)
dash.pop("__inputs", None)
dash.pop("id", None)

envelope = {
    "dashboard": dash,
    "folderUid": "${FOLDER_UID}",
    "overwrite": True,
    "message": "imported from blinklabs-io/dingo repo"
}
print(json.dumps(envelope))
PYEOF
)

  result=$(curl -sf \
    -X POST \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer ${GRAFANA_TOKEN}" \
    --data-binary "${payload}" \
    "${GRAFANA_URL}/api/dashboards/db")

  url=$(echo "${result}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('url',''))" 2>/dev/null || true)
  echo "OK — ${GRAFANA_URL}${url}"
done

echo ""
echo "All dashboards imported to folder ${FOLDER_UID}."
echo "If panels still show 'No data', confirm Prometheus is scraping dingo on port 12798."
