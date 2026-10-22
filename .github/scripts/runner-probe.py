#!/usr/bin/env python3
"""Log what kind of runner a test job got, so a slow or red run explains itself.

Runner speed varies several-fold between hosts of the same label, and storage
latency is what varies most: the same suite has run I/O-bound steps three to
five times slower on some hosts while CPU-bound steps ran unchanged. A job log
that records the host's region, CPU, memory and fsync latency lets a slow run
be attributed to the host or to the change without re-running anything.

Usage: runner-probe.py start|end

`start` also launches a background sampler that records the lowest available
memory seen during the job; `end` reports it, then stops the sampler. Every
probe is best effort: a value that cannot be read is reported as unknown, and
the script always exits 0 so it can never fail the job it describes.
"""

import json
import os
import platform
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.request

FSYNC_ROUNDS = 100
FSYNC_BYTES = 4096
SAMPLE_SECONDS = 5
IMDS_LOCATION = (
    "http://169.254.169.254/metadata/instance/compute/location"
    "?api-version=2021-02-01&format=text"
)


def state_dir():
    return os.environ.get("RUNNER_TEMP") or tempfile.gettempdir()


def sampler_paths():
    base = os.path.join(state_dir(), "runner-probe")
    return base + ".pid", base + ".minavail"


def run(cmd):
    try:
        return subprocess.run(
            cmd, capture_output=True, text=True, timeout=5, check=False
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def region():
    request = urllib.request.Request(IMDS_LOCATION, headers={"Metadata": "true"})
    try:
        with urllib.request.urlopen(request, timeout=2) as response:
            return response.read().decode().strip() or "unknown"
    except OSError:
        return "unknown"


def cpu_model():
    if sys.platform == "darwin":
        return run(["sysctl", "-n", "machdep.cpu.brand_string"]) or "unknown"
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as cpuinfo:
            for line in cpuinfo:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    for line in run(["lscpu"]).splitlines():
        if line.startswith("Model name:"):
            return line.split(":", 1)[1].strip()
    return platform.processor() or "unknown"


def memory_mib():
    """Return (total, available) in MiB, either None when unreadable."""
    if sys.platform == "darwin":
        total = run(["sysctl", "-n", "hw.memsize"])
        page = run(["sysctl", "-n", "hw.pagesize"])
        free = None
        for line in run(["vm_stat"]).splitlines():
            if line.startswith(("Pages free", "Pages inactive")):
                pages = int(line.split(":")[1].strip().rstrip("."))
                free = (free or 0) + pages
        total_mib = int(total) // 2**20 if total.isdigit() else None
        avail_mib = (
            free * int(page) // 2**20 if free is not None and page.isdigit()
            else None
        )
        return total_mib, avail_mib
    fields = {}
    try:
        with open("/proc/meminfo", encoding="utf-8") as meminfo:
            for line in meminfo:
                key, value = line.split(":", 1)
                fields[key] = int(value.split()[0]) // 1024
    except (OSError, ValueError):
        pass
    return fields.get("MemTotal"), fields.get("MemAvailable")


def fsync_latency_ms(directory):
    """Time FSYNC_ROUNDS create-write-fsync-close rounds in directory."""
    samples = []
    payload = os.urandom(FSYNC_BYTES)
    with tempfile.TemporaryDirectory(dir=directory) as scratch:
        for i in range(FSYNC_ROUNDS):
            path = os.path.join(scratch, str(i))
            began = time.perf_counter()
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            try:
                os.write(fd, payload)
                os.fsync(fd)
            finally:
                os.close(fd)
            samples.append((time.perf_counter() - began) * 1000)
    samples.sort()
    return {
        "median": round(statistics.median(samples), 2),
        "p90": round(samples[int(0.9 * (len(samples) - 1))], 2),
        "max": round(samples[-1], 2),
    }


def sample_forever(path):
    lowest = None
    while True:
        _, available = memory_mib()
        if available is not None and (lowest is None or available < lowest):
            lowest = available
            with open(path, "w", encoding="utf-8") as out:
                out.write(str(lowest))
        time.sleep(SAMPLE_SECONDS)


def start_sampler():
    pid_path, min_path = sampler_paths()
    child = subprocess.Popen(
        [sys.executable, os.path.abspath(__file__), "sample", min_path],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    with open(pid_path, "w", encoding="utf-8") as out:
        out.write(str(child.pid))


def stop_sampler():
    """Stop the sampler and return the lowest available memory it saw."""
    pid_path, min_path = sampler_paths()
    try:
        with open(pid_path, encoding="utf-8") as pid_file:
            os.kill(int(pid_file.read()), signal.SIGTERM)
    except (OSError, ValueError):
        pass
    try:
        with open(min_path, encoding="utf-8") as min_file:
            return int(min_file.read())
    except (OSError, ValueError):
        return None


def probe(phase):
    directory = state_dir()
    total, available = memory_mib()
    fsync_ms = None
    try:
        fsync_ms = fsync_latency_ms(directory)
    except OSError as err:
        fsync_error = repr(err)
    else:
        fsync_error = None
    report = {
        "phase": phase,
        "runner": os.environ.get("RUNNER_NAME", "unknown"),
        "image": os.environ.get("ImageOS", "unknown")
        + "/"
        + os.environ.get("ImageVersion", "unknown"),
        "region": region(),
        "cpu": cpu_model(),
        "cpus": os.cpu_count(),
        "load": [round(v, 2) for v in os.getloadavg()],
        "mem_total_mib": total,
        "mem_available_mib": available,
        "disk_free_gib": round(shutil.disk_usage(directory).free / 2**30, 1),
        "fsync_ms": fsync_ms,
    }
    if fsync_error is not None:
        report["fsync_error"] = fsync_error
    if phase == "start":
        start_sampler()
    else:
        lowest = stop_sampler()
        report["mem_min_available_mib"] = lowest
        if total is not None and lowest is not None:
            report["mem_peak_used_mib"] = total - lowest
    print("runner-probe " + json.dumps(report, sort_keys=True))


def main(argv):
    if len(argv) == 3 and argv[1] == "sample":
        sample_forever(argv[2])
        return
    if len(argv) != 2 or argv[1] not in ("start", "end"):
        print("usage: runner-probe.py start|end", file=sys.stderr)
        return
    try:
        probe(argv[1])
    except Exception as err:  # noqa: BLE001 - a probe must never fail the job
        print(f"runner-probe {argv[1]}: failed: {err!r}")


if __name__ == "__main__":
    main(sys.argv)
    sys.exit(0)
