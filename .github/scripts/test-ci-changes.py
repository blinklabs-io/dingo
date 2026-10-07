#!/usr/bin/env python3
"""Exercise CI selection against real Git histories without GitHub services."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("ci-changes.sh").resolve()


class CIChangesTest(unittest.TestCase):
    def test_change_selection(self):
        cases = [
            ("readme", {"README.md": "new"}, "pull_request", False),
            ("nested docs", {"docs/guide/setup.md": "new"}, "pull_request", False),
            ("go", {"node.go": "new"}, "pull_request", True),
            ("package docs", {"doc.go": "new"}, "pull_request", True),
            ("mixed", {"README.md": "new", "node.go": "new"}, "pull_request", True),
            ("config", {"config/cardano/preview/README.md": "new"}, "pull_request", True),
            ("fixture", {"testdata/vector.md": "new"}, "pull_request", True),
            ("workflow", {".github/workflows/go-test.yml": "new"}, "pull_request", True),
            ("unknown", {"new-file": "new"}, "pull_request", True),
            ("empty", {}, "pull_request", True),
            ("manual", {"README.md": "new"}, "workflow_dispatch", True),
            ("push", {"README.md": "new"}, "push", True),
            ("delete docs", {"README.md": None}, "pull_request", False),
            ("delete code", {"node.go": None}, "pull_request", True),
            ("rename code to docs", {"node.go": None, "docs/node.md": "old"}, "pull_request", True),
            ("unusual code name", {"source\nfile.go": "new"}, "pull_request", True),
            ("unusual docs name", {"docs/a\nb.md": "new"}, "pull_request", False),
        ]
        for name, changes, event, expected in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)

                def git(*args):
                    return subprocess.check_output(
                        ["git", *args], cwd=root, stderr=subprocess.DEVNULL, text=True
                    ).strip()

                git("init", "-q")
                git("config", "user.email", "ci@example.invalid")
                git("config", "user.name", "CI selection test")
                for path in ("README.md", "node.go"):
                    (root / path).write_text("old")
                git("add", ".")
                git("commit", "-qm", "base")
                base = git("rev-parse", "HEAD")
                for path, content in changes.items():
                    target = root / path
                    if content is None:
                        target.unlink()
                    else:
                        target.parent.mkdir(parents=True, exist_ok=True)
                        target.write_text(content)
                git("add", ".")
                git("commit", "--allow-empty", "-qm", "change")
                head = git("rev-parse", "HEAD")
                # Advance the base branch independently: PR selection must
                # exclude unrelated code added after the common ancestor.
                git("checkout", "-q", "--detach", base)
                (root / "unrelated.go").write_text("base branch change")
                git("add", ".")
                git("commit", "-qm", "advance base")
                base = git("rev-parse", "HEAD")
                output = root / "output"
                env = dict(os.environ, EVENT_NAME=event, BASE_SHA=base,
                           HEAD_SHA=head, GITHUB_OUTPUT=str(output))
                subprocess.run(["bash", str(SCRIPT)], cwd=root, env=env,
                               check=True, stdout=subprocess.DEVNULL)
                self.assertEqual(output.read_text(), f"run-ci={str(expected).lower()}\n")
                output.unlink()
                env["BASE_SHA"] = "invalid"
                subprocess.run(["bash", str(SCRIPT)], cwd=root, env=env,
                               check=True, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
                self.assertEqual(output.read_text(), "run-ci=true\n")


if __name__ == "__main__":
    unittest.main()
