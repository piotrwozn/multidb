"""Low-overhead process diagnostics and aggregate container cgroup-v2 counters.

Container memory.current includes page cache/kernel memory and is not process
RSS. Missing cgroup access stays explicitly unavailable; a parent PID is never
silently presented as the service's aggregate memory. Heterogeneous native,
container and embedded deployments do not qualify for an engine RAM ranking.
"""
from __future__ import annotations

import json
from pathlib import Path, PurePosixPath
import shutil
import threading
import time

from compare import require_idle, resource_sample


class ContainerResources:
    def __init__(self, container, pid, *, cgroup_root=Path("/sys/fs/cgroup"), proc_root=Path("/proc")):
        self.container, self.pid, self.path, self.error = container, pid, None, None
        try:
            if type(pid) is not int or pid <= 0:
                raise ValueError("invalid container PID")
            lines = (Path(proc_root) / str(pid) / "cgroup").read_text().splitlines()
            entries = [line.split(":", 2)[2] for line in lines if line.startswith("0::")]
            if len(entries) != 1:
                raise ValueError("container cgroup-v2 membership is unavailable")
            relative = PurePosixPath(entries[0])
            if not relative.is_absolute() or ".." in relative.parts:
                raise ValueError("invalid container cgroup path")
            root = Path(cgroup_root).resolve()
            candidate = root.joinpath(*relative.parts[1:]).resolve()
            if not candidate.is_relative_to(root):
                raise ValueError("container cgroup escapes the mounted hierarchy")
            # A namespaced / path would charge the entire benchmark host, not
            # this container, unless a distinct container hierarchy is visible.
            if candidate == root:
                raise ValueError("container-specific cgroup is hidden by the cgroup namespace")
            self.path = candidate
        except (OSError, ValueError, IndexError) as error:
            self.error = f"{type(error).__name__}: {error}"

    def sample(self):
        result = {
            "time_monotonic": time.monotonic(), "container": self.container,
            "scope": "container_cgroup_v2_including_descendants", "aggregate_available": False,
            "aggregate_rss_bytes": None, "ram_rank_eligible": False,
            "memory_semantics": "cgroup charged memory includes page cache/kernel; not RSS; deployment limits differ",
        }
        if self.path is None:
            return {**result, "unavailable_reason": self.error}
        try:
            memory = int((self.path / "memory.current").read_text().strip())
            cpu = dict(line.split() for line in (self.path / "cpu.stat").read_text().splitlines())
            memory_stats = dict(line.split() for line in (self.path / "memory.stat").read_text().splitlines())
            usage = int(cpu["usage_usec"])
            if min(memory, usage) < 0:
                raise ValueError("negative cgroup resource counter")
            result.update(aggregate_available=True, memory_current_bytes=memory,
                          cpu_seconds=usage / 1_000_000, cgroup_path=str(self.path))
            for field, key in (("memory_anonymous_bytes", "anon"), ("memory_file_bytes", "file"),
                               ("memory_kernel_bytes", "kernel")):
                result[field] = int(memory_stats[key]) if key in memory_stats else None
            peak = self.path / "memory.peak"
            result["memory_peak_bytes"] = int(peak.read_text().strip()) if peak.exists() else None
        except (OSError, ValueError, KeyError) as error:
            result.update(aggregate_available=False, unavailable_reason=f"{type(error).__name__}: {error}")
        return result


def process_sample(pid):
    return {**resource_sample(pid), "scope": "one_native_process_including_threads",
            "aggregate_service": False, "ram_rank_eligible": False,
            "memory_semantics": "process RSS; embedded engines share the benchmark client; not comparable to cgroup charged memory"}


class ResourceSampler:
    def __init__(self, output, targets):
        self.path = Path(output) / "resources.jsonl"
        self.targets = {}
        self.target_lock = threading.Lock()
        self.errors = []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._run, daemon=True)
        for name, pid in targets.items():
            self.add_target(name, pid)

    def _run(self):
        try:
            with self.path.open("x") as stream:
                while not self.stop.is_set():
                    require_idle()
                    with self.target_lock:
                        targets = dict(self.targets)
                    samples = {name: collect() for name, collect in targets.items()}
                    space = shutil.disk_usage(self.path.parent)
                    samples["output_filesystem"] = {
                        "scope": "output_filesystem", "time_monotonic": time.monotonic(),
                        "free_bytes": space.free, "total_bytes": space.total,
                    }
                    stream.write(json.dumps(samples) + "\n")
                    stream.flush()
                    self.stop.wait(.5)
        except Exception as failure:
            self.errors.append(f"{type(failure).__name__}: {failure}")

    def start(self):
        self.thread.start()

    def add_target(self, name, pid):
        with self.target_lock:
            self.targets[name] = lambda pid=pid: process_sample(pid)

    def add_container(self, name, container, pid):
        source = ContainerResources(container, pid)
        with self.target_lock:
            self.targets[name] = source.sample

    def remove_target(self, name):
        with self.target_lock:
            self.targets.pop(name, None)

    def close(self):
        self.stop.set()
        self.thread.join(timeout=5)
        if self.thread.is_alive() or self.errors:
            raise RuntimeError(f"resource sampling failed: {self.errors}")
