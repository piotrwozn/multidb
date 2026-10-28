import fnmatch
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from domain_common import Recorder
import domain_kv


class RedisFixture:
    def __init__(self):
        self.data, self.transactions = {}, []
        self.settings = {"appendonly": "yes", "appendfsync": "always"}
        self.boot = "boot-1"

    def info(self, _):
        return {"redis_version": "8.2.3", "run_id": self.boot}

    def config_get(self, *_):
        return self.settings

    def set(self, key, value):
        self.data[key] = value
        return True

    def get(self, key):
        return self.data.get(key)

    def mget(self, keys):
        return [self.get(key) for key in keys]

    def scan_iter(self, match, count):
        return iter(key for key in self.data if fnmatch.fnmatchcase(key, match))

    def pipeline(self, transaction):
        self.transactions.append(transaction)
        owner = self

        class Pipeline:
            def __init__(self):
                self.operations = []

            def set(self, key, value):
                self.operations.append((key, value))

            def execute(self):
                return [owner.set(key, value) for key, value in self.operations]

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        return Pipeline()


class HttpFixture:
    def __init__(self):
        self.connection = sqlite3.connect(":memory:")
        self.builders = []

    def json(self, method, path, body):
        if path == "/builder/table":
            self.builders.append(body)
            self.connection.execute(f"CREATE TABLE {body['name']} (id INTEGER PRIMARY KEY, category INTEGER NOT NULL, value INTEGER NOT NULL, payload TEXT NOT NULL)")
            return {"created": True}
        if (method, path) != ("POST", "/sql"):
            raise AssertionError((method, path))
        cursor = self.connection.execute(body["sql"])
        if cursor.description:
            return {"output": {"kind": "rows", "rows": [list(row) for row in cursor.fetchall()]}}
        return {"output": {"kind": "affected_rows", "affected_rows": cursor.rowcount}}


class KeyValueDomainTests(unittest.TestCase):
    def test_full_adapter_exact_records_atomic_batches_and_honest_restart_check(self):
        redis, http = RedisFixture(), HttpFixture()
        with tempfile.TemporaryDirectory() as directory, patch("domain_common.require_idle"):
            args = SimpleNamespace(output=Path(directory), rows=[103], repeats=2, ops=2)
            recorder = Recorder(Path(directory) / "measurements")
            try:
                domain_kv.run(args, recorder, multidb=http, redis_client=redis)
            finally:
                recorder.close()
            self.assertEqual(len(recorder.summaries), 12)
            self.assertTrue(all(result["correctness_status"] == "passed" for result in recorder.summaries))
            self.assertTrue(all(redis.transactions))
            self.assertEqual(http.builders[0]["indexes"], [])
            self.assertEqual(http.builders[0]["schema"]["primary_key"], 0)
            with self.assertRaisesRegex(RuntimeError, "has not restarted"):
                domain_kv.verify_persistence(args, multidb_restart=(10, 20), multidb=http, redis_client=redis)
            redis.boot = "boot-2"
            domain_kv.verify_persistence(args, multidb_restart=(10, 20), multidb=http, redis_client=redis)
            proof = json.loads((Path(directory) / "kv-persistence.json").read_text())
            self.assertTrue(proof["restart_tested"])
            key = next(iter(redis.data))
            del redis.data[key]
            with self.assertRaises(AssertionError):
                domain_kv.verify_persistence(args, multidb_restart=(20, 30), multidb=http, redis_client=redis)
        http.connection.close()

    def test_unsafe_durability_setting_and_coerced_record_types_fail_closed(self):
        redis = RedisFixture()
        redis.settings["appendfsync"] = "everysec"
        with self.assertRaisesRegex(RuntimeError, "appendfsync=always"):
            domain_kv.verify_redis_configuration(redis)
        with self.assertRaises(AssertionError):
            domain_kv.assert_record([1.0, 1, 1, "x"], [1, 1, 1, "x"])
        with self.assertRaises(AssertionError):
            domain_kv.require_redis_batch([True, False], 2)
        self.assertEqual(len(domain_kv.row(1)[3]), 256)


if __name__ == "__main__":
    unittest.main()
