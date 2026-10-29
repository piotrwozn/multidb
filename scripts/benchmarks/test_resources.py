from pathlib import Path
import tempfile
import unittest

from resources import ContainerResources


class AggregateContainerResources(unittest.TestCase):
    def test_cgroup_aggregate_counters_do_not_use_parent_pid_rss(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            proc, cgroup = root / "proc", root / "cgroup"
            (proc / "123").mkdir(parents=True)
            (proc / "123/cgroup").write_text("0::/docker/container1\n")
            counters = cgroup / "docker/container1"
            counters.mkdir(parents=True)
            (counters / "memory.current").write_text("9000\n")
            (counters / "memory.peak").write_text("12000\n")
            (counters / "memory.stat").write_text("anon 4000\nfile 3000\nkernel 2000\n")
            (counters / "cpu.stat").write_text("usage_usec 2500000\nuser_usec 2000000\n")
            result = ContainerResources("container1", 123, cgroup_root=cgroup, proc_root=proc).sample()
            self.assertTrue(result["aggregate_available"])
            self.assertEqual(result["memory_current_bytes"], 9000)
            self.assertEqual(result["cpu_seconds"], 2.5)
            self.assertEqual(result["memory_anonymous_bytes"], 4000)
            self.assertIsNone(result["aggregate_rss_bytes"])
            self.assertFalse(result["ram_rank_eligible"])
            self.assertNotIn("VmRSS", result)

    def test_hidden_or_missing_cgroup_is_unavailable_never_host_or_pid_substitute(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "123").mkdir()
            (root / "123/cgroup").write_text("0::/\n")
            hidden = ContainerResources("hidden", 123, cgroup_root=root, proc_root=root).sample()
            self.assertFalse(hidden["aggregate_available"])
            self.assertIn("hidden", hidden["unavailable_reason"])
            missing = ContainerResources("missing", 999, cgroup_root=root, proc_root=root).sample()
            self.assertFalse(missing["aggregate_available"])
            self.assertNotIn("memory_current_bytes", missing)


if __name__ == "__main__":
    unittest.main()
