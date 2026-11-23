#!/usr/bin/env python3
"""Check that `make build` hands dingo's PGO profile to every cmd/ binary."""

from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
PROFILE = ROOT / "cmd" / "dingo" / "default.pgo"
NAMES = sorted(
    d.name for d in (ROOT / "cmd").iterdir()
    if d.is_dir() and not d.name.startswith("common")
)


def go_build_lines(root):
    # -B forces every binary rule to print its recipe regardless of mtimes.
    out = subprocess.run(
        ["make", "-n", "-B", *NAMES],
        cwd=root, check=True, capture_output=True, text=True,
    ).stdout.replace("\\\n", " ")
    lines = {}
    for name in NAMES:
        matching = [l for l in out.splitlines() if "go build" in l and l.split()[-1] == f"./cmd/{name}"]
        lines[name] = matching
    return lines


class MakefilePGOTest(unittest.TestCase):
    def test_every_binary_uses_dingo_profile(self):
        self.assertTrue(PROFILE.is_file())
        flag = f"-pgo={PROFILE}"
        for name, lines in go_build_lines(ROOT).items():
            with self.subTest(binary=name):
                self.assertEqual(len(lines), 1, lines)
                self.assertIn(flag, lines[0].split())

    def test_absent_profile_passes_no_pgo_flag(self):
        # A tree without cmd/dingo/default.pgo must still build, so the
        # recipe may not name the missing file.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for f in ("Makefile", "go.mod", "go.sum"):
                shutil.copy(ROOT / f, root / f)
            for name in NAMES:
                (root / "cmd" / name).mkdir(parents=True)
            for name, lines in go_build_lines(root).items():
                with self.subTest(binary=name):
                    self.assertEqual(len(lines), 1, lines)
                    self.assertFalse(
                        [t for t in lines[0].split() if t.startswith("-pgo")],
                        lines[0],
                    )


if __name__ == "__main__":
    unittest.main()
