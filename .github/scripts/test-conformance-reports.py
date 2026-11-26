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

# Stands in for go test: it runs in the package directory, as the test binary
# does, prints test failures on stdout, and writes valid reports for the
# scenarios the script can run without downloading a corpus.
STUB_GO = """#!/bin/sh
echo "$*" >> "$STUB_LOG"
[ "$1" = test ] || exit 1
for pkg; do :; done
cd "$pkg" || exit 1
fail() {
  echo "--- FAIL: TestWriteConformanceReport (0.00s)"
  echo "    report_output_test.go:1: $1"
  echo "FAIL"
  exit 1
}
[ -n "${DINGO_CONFORMANCE_OUTPUT:-}" ] || fail "DINGO_CONFORMANCE_OUTPUT is not set"
[ -d "${DINGO_CONFORMANCE_INPUT:-}" ] ||
  fail "generate report: missing input ${DINGO_CONFORMANCE_INPUT:-}"
case "$pkg" in
*/cborconformance)
  [ -n "${DINGO_CONFORMANCE_REVISION:-}" ] || exit 1
  cat >"$DINGO_CONFORMANCE_OUTPUT" <<'JSON' || exit 1
{"version":"1","scenario":"cbor-decoding","generated_at":"2026-01-01T00:00:00Z",
 "corpus":"c","dataset_commit":"d","protocol_version":"10.0","successful":true,
 "totals":{},"rules":{},"failures":[]}
JSON
  ;;
*/ledgerrulesconformance)
  [ -n "${DINGO_CONFORMANCE_REVISION:-}" ] || exit 1
  cat >"$DINGO_CONFORMANCE_OUTPUT" <<JSON || exit 1
{"version":"1","scenario":"ledger-rules","generated_at":"2026-01-01T00:00:00Z",
 "dataset_commit":"$DINGO_CONFORMANCE_REVISION",
 "implementation":{"name":"dingo"},"scenarios":[]}
JSON
  ;;
*) exit 1 ;;
esac
"""

LEDGER_FIXTURES = Path(
    ".conformance/amaru/crates/amaru-ledger/tests/data/transaction"
)


class ConformanceReportsTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        bin_dir = self.tmp / "bin"
        bin_dir.mkdir()
        for name, body in (("go", STUB_GO), ("curl", "#!/bin/sh\nexit 1\n")):
            stub = bin_dir / name
            stub.write_text(body)
            stub.chmod(0o755)

        # The release workflow runs the script from the repository root with
        # relative input and output directories.
        self.work = self.tmp / "work"
        for pkg in ("cborconformance", "ledgerrulesconformance", "uplcconformance"):
            (self.work / "internal" / pkg).mkdir(parents=True)
        dataset = self.work / ".conformance" / "cardano-cbor-dataset"
        (dataset / "dataset" / "conway").mkdir(parents=True)
        (self.work / LEDGER_FIXTURES).mkdir(parents=True)
        revision = self.init_checkout(dataset)
        amaru_revision = self.init_checkout(self.work / ".conformance" / "amaru")

        self.log = self.tmp / "go.log"
        self.env = {
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "STUB_LOG": str(self.log),
            "RELEASE_TAG": "v0.0.1",
            "CBOR_DATASET_DIR": ".conformance/cardano-cbor-dataset",
            "CBOR_DATASET_REVISION": revision,
            "AMARU_LEDGER_REVISION": amaru_revision,
            "UPLC_CORPUS_SHA256": "0" * 64,
            "TURBO_CORPUS_SHA256": "0" * 64,
        }
        for name in ("DIST_DIR", "CONFORMANCE_DIR"):
            self.env.pop(name, None)

    @staticmethod
    def init_checkout(path):
        """Make path a one-commit git checkout and return its commit."""
        git = ["git", "-C", str(path)]
        subprocess.run([*git, "init", "-q"], check=True)
        subprocess.run(
            [*git, "-c", "user.name=t", "-c", "user.email=t@example.com",
             "-c", "commit.gpgsign=false",
             "commit", "-q", "--allow-empty", "-m", "x"],
            check=True,
        )
        return subprocess.run(
            [*git, "rev-parse", "HEAD"], check=True, capture_output=True, text=True
        ).stdout.strip()

    def run_script(self):
        subprocess.run(
            ["bash", str(SCRIPT)], cwd=self.work, env=self.env, check=True,
            capture_output=True, text=True,
        )

    def read_report(self, suffix):
        reports = list((self.work / "dist").glob(f"dingo-*-{suffix}.json.gz"))
        self.assertEqual(len(reports), 1, reports)
        raw = subprocess.run(
            ["gzip", "-dc", str(reports[0])], check=True, capture_output=True
        ).stdout
        return json.loads(raw)

    def test_cbor_and_ledger_reports_come_from_go_test(self):
        self.run_script()

        for suffix, scenario in (
            ("cbor-conformance", "cbor-decoding"),
            ("ledger-rules", "ledger-rules"),
        ):
            report = self.read_report(suffix)
            self.assertEqual(report["scenario"], scenario)
            self.assertNotIn("error", report)

        self.assertNotIn("go run", self.log.read_text())

    def test_ledger_report_names_the_amaru_commit_it_ran_against(self):
        self.run_script()

        report = self.read_report("ledger-rules")
        self.assertEqual(report["dataset_commit"], self.env["AMARU_LEDGER_REVISION"])

    def test_ledger_error_report_when_the_checkout_is_not_the_pinned_revision(self):
        self.env["AMARU_LEDGER_REVISION"] = "0" * 40
        self.run_script()

        report = self.read_report("ledger-rules")
        self.assertIn("does not match the pinned revision", report["error"])

    def test_error_report_carries_the_test_failure(self):
        (self.work / LEDGER_FIXTURES).rmdir()
        self.run_script()

        report = self.read_report("ledger-rules")
        self.assertIn("generate report: missing input", report["error"])


if __name__ == "__main__":
    shutil.which("jq") or exit("jq is required")
    unittest.main()
