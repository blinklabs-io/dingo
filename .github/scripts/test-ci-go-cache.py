#!/usr/bin/env python3
"""Check the ci workflow's Go cache and PGO wiring without GitHub services."""

import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest

import yaml


SCRIPTS = Path(__file__).resolve().parent
WORKFLOWS = SCRIPTS.parent / "workflows"
PRUNE = SCRIPTS / "prune-go-cache.sh"

TEST_JOBS = ["check", "race", "services", "macos"]
# job in go-test.yml -> job in go-cache.yml that saves its build cache
WARM_JOBS = {"check": "check", "race": "race", "services": "services", "macos": "macos"}


def load(name):
    with open(WORKFLOWS / name, encoding="utf-8") as f:
        return yaml.safe_load(f)


def cache_steps(job, action):
    return [
        s
        for s in job["steps"]
        if str(s.get("uses", "")).startswith(action + "@")
    ]


def build_cache_step(job, action):
    steps = [
        s
        for s in cache_steps(job, action)
        if "go-build" in s["with"]["path"] or "Caches" in s["with"]["path"]
    ]
    return steps[0] if steps else None


def build_prefix(key):
    """The part of a build-cache key before its content-derived suffix."""
    return key.split("${{ hashFiles")[0]


class GoTestWorkflowTest(unittest.TestCase):
    def setUp(self):
        self.jobs = load("go-test.yml")["jobs"]

    def test_test_jobs_build_without_pgo(self):
        for name in TEST_JOBS:
            with self.subTest(job=name):
                env = self.jobs[name].get("env", {})
                self.assertIn("-pgo=off", str(env.get("GOFLAGS", "")))

    def test_release_build_keeps_pgo(self):
        env = self.jobs["build"].get("env", {})
        self.assertNotIn("-pgo", str(env.get("GOFLAGS", "")))

    def test_pull_request_and_queue_jobs_never_save_caches(self):
        for name, job in self.jobs.items():
            with self.subTest(job=name):
                self.assertEqual([], cache_steps(job, "actions/cache"))
                self.assertEqual([], cache_steps(job, "actions/cache/save"))

    def test_test_jobs_restore_distinct_build_caches(self):
        prefixes = {}
        for name in TEST_JOBS:
            step = build_cache_step(self.jobs[name], "actions/cache/restore")
            self.assertIsNotNone(step, name)
            key = step["with"]["key"]
            prefixes[name] = build_prefix(key)
            self.assertIn("restore-keys", step["with"], name)
        self.assertEqual(len(prefixes), len(set(prefixes.values())), prefixes)

    def test_module_cache_is_restored_by_every_job(self):
        for name in TEST_JOBS + ["build"]:
            with self.subTest(job=name):
                mods = [
                    s
                    for s in cache_steps(self.jobs[name], "actions/cache/restore")
                    if s["with"]["path"] == "~/go/pkg/mod"
                ]
                self.assertEqual(1, len(mods))


class GoCacheWorkflowTest(unittest.TestCase):
    def setUp(self):
        self.wf = load("go-cache.yml")
        self.jobs = self.wf["jobs"]
        self.ci = load("go-test.yml")["jobs"]

    def test_runs_on_main_pushes(self):
        # PyYAML parses the bare key `on` as True.
        triggers = self.wf.get("on", self.wf.get(True))
        self.assertEqual(["main"], triggers["push"]["branches"])

    def test_jobs_build_without_pgo(self):
        for name, job in self.jobs.items():
            with self.subTest(job=name):
                self.assertIn("-pgo=off", str(job.get("env", {}).get("GOFLAGS", "")))

    def test_saved_keys_match_restored_prefixes(self):
        for ci_name, warm_name in WARM_JOBS.items():
            with self.subTest(job=ci_name):
                restore = build_cache_step(self.ci[ci_name], "actions/cache/restore")
                save = build_cache_step(self.jobs[warm_name], "actions/cache/save")
                self.assertIsNotNone(save)
                saved_key = save["with"]["key"]
                self.assertIn("github.sha", saved_key)
                prefix = build_prefix(restore["with"]["key"])
                self.assertTrue(saved_key.startswith(prefix), (saved_key, prefix))

    def test_warm_jobs_prune_after_saving(self):
        for name, job in self.jobs.items():
            with self.subTest(job=name):
                uses = [str(s.get("uses", "")) for s in job["steps"]]
                runs = [str(s.get("run", "")) for s in job["steps"]]
                save = max(i for i, u in enumerate(uses) if u.startswith("actions/cache/save@"))
                prune = [i for i, r in enumerate(runs) if "prune-go-cache.sh" in r]
                self.assertTrue(prune)
                self.assertGreater(min(prune), save)
                self.assertEqual("write", job["permissions"]["actions"])


class PruneGoCacheTest(unittest.TestCase):
    def run_prune(self, listing, *args):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            log = tmp / "calls"
            gh = tmp / "gh"
            gh.write_text(
                "#!/usr/bin/env bash\n"
                'echo "$*" >> "$GH_LOG"\n'
                'if [ "$1 $2" = "cache list" ]; then printf "%b" "$GH_LISTING"; fi\n'
            )
            gh.chmod(gh.stat().st_mode | stat.S_IXUSR)
            env = dict(
                os.environ,
                PATH=f"{tmp}:{os.environ['PATH']}",
                GH_LOG=str(log),
                GH_LISTING=listing,
                GITHUB_REPOSITORY="o/r",
            )
            proc = subprocess.run(
                ["bash", str(PRUNE), *args],
                env=env,
                capture_output=True,
                text=True,
            )
            calls = log.read_text().splitlines() if log.exists() else []
            deletes = [c for c in calls if c.startswith("cache delete")]
            return proc, calls, deletes

    def test_deletes_older_entries_and_keeps_current(self):
        proc, calls, deletes = self.run_prune(
            "1\\tLinux-go-build-check-aaa\\n2\\tLinux-go-build-check-bbb\\n3\\tLinux-go-build-check-ccc\\n",
            "Linux-go-build-check-",
            "Linux-go-build-check-bbb",
        )
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertEqual(
            ["cache delete 1 --repo o/r", "cache delete 3 --repo o/r"], deletes
        )
        self.assertTrue(any("--ref refs/heads/main" in c for c in calls), calls)

    def test_ignores_keys_outside_the_prefix(self):
        proc, _, deletes = self.run_prune(
            "7\\tLinux-go-build-race-aaa\\n8\\tLinux-go-build-check-old\\n",
            "Linux-go-build-check-",
            "Linux-go-build-check-new",
        )
        self.assertEqual(0, proc.returncode, proc.stderr)
        self.assertEqual(["cache delete 8 --repo o/r"], deletes)

    def test_refuses_missing_arguments(self):
        proc, _, deletes = self.run_prune("", "Linux-go-build-check-")
        self.assertNotEqual(0, proc.returncode)
        self.assertEqual([], deletes)

    def test_refuses_a_keep_key_outside_the_prefix(self):
        proc, _, deletes = self.run_prune("1\\tx-a\\n", "x-", "y-b")
        self.assertNotEqual(0, proc.returncode)
        self.assertEqual([], deletes)


if __name__ == "__main__":
    unittest.main()
