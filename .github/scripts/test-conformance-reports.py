#!/usr/bin/env python3
"""Check that generate-conformance-reports.sh produces reports with go test."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / ".github" / "scripts" / "generate-conformance-reports.sh"

# Valid reports for the scenarios the script can run through the stub below.
STUB_GO = """#!/bin/sh
echo "$*" >> "$STUB_LOG"
[ "$1" = test ] || exit 1
[ -n "${DINGO_CONFORMANCE_OUTPUT:-}" ] || exit 1
for pkg; do :; done
case "$pkg" in
*/cborconformance)
  [ -n "${DINGO_CONFORMANCE_INPUT:-}" ] && [ -n "${DINGO_CONFORMANCE_REVISION:-}" ] || exit 1
  cat >"$DINGO_CONFORMANCE_OUTPUT" <<'JSON'
{"version":"1","scenario":"cbor-decoding","generated_at":"2026-01-01T00:00:00Z",
 "corpus":"c","dataset_commit":"d","protocol_version":"10.0","successful":true,
 "totals":{},"rules":{},"failures":[]}
JSON
  ;;
*/ledgerrulesconformance)
  [ -n "${DINGO_CONFORMANCE_INPUT:-}" ] || exit 1
  cat >"$DINGO_CONFORMANCE_OUTPUT" <<'JSON'
{"version":"1","scenario":"ledger-rules","generated_at":"2026-01-01T00:00:00Z",
 "implementation":{"name":"dingo"},"scenarios":[]}
JSON
  ;;
*) exit 1 ;;
esac
"""


class ConformanceReportsTest(unittest.TestCase):
    def test_cbor_and_ledger_reports_come_from_go_test(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            bin_dir = tmp / "bin"
            bin_dir.mkdir()
            for name, body in (("go", STUB_GO), ("curl", "#!/bin/sh\nexit 1\n")):
                stub = bin_dir / name
                stub.write_text(body)
                stub.chmod(0o755)

            dataset = tmp / "dataset"
            dataset.mkdir()
            git = ["git", "-C", str(dataset)]
            subprocess.run([*git, "init", "-q"], check=True)
            subprocess.run(
                [*git, "-c", "user.name=t", "-c", "user.email=t@example.com",
                 "commit", "-q", "--allow-empty", "-m", "x"],
                check=True,
            )
            revision = subprocess.run(
                [*git, "rev-parse", "HEAD"], check=True, capture_output=True, text=True
            ).stdout.strip()

            dist = tmp / "dist"
            log = tmp / "go.log"
            env = {
                **os.environ,
                "PATH": f"{bin_dir}:{os.environ['PATH']}",
                "STUB_LOG": str(log),
                "RELEASE_TAG": "v0.0.1",
                "DIST_DIR": str(dist),
                "CONFORMANCE_DIR": str(tmp / "conformance"),
                "CBOR_DATASET_DIR": str(dataset),
                "CBOR_DATASET_REVISION": revision,
                "UPLC_CORPUS_SHA256": "0" * 64,
                "TURBO_CORPUS_SHA256": "0" * 64,
            }
            subprocess.run(
                ["bash", str(SCRIPT)], cwd=ROOT, env=env, check=True,
                capture_output=True, text=True,
            )

            for suffix, scenario in (
                ("cbor-conformance", "cbor-decoding"),
                ("ledger-rules", "ledger-rules"),
            ):
                reports = list(dist.glob(f"dingo-*-{suffix}.json.gz"))
                self.assertEqual(len(reports), 1, reports)
                raw = subprocess.run(
                    ["gzip", "-dc", str(reports[0])], check=True, capture_output=True
                ).stdout
                report = json.loads(raw)
                self.assertEqual(report["scenario"], scenario)
                self.assertNotIn("error", report)

            self.assertNotIn("go run", log.read_text())


if __name__ == "__main__":
    shutil.which("jq") or exit("jq is required")
    unittest.main()
