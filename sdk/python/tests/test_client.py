import json
import unittest

from multidb_client import (
    CONTROL_PLANE_API_VERSION,
    DEFAULT_BASE_URL,
    MIN_MULTIDB_VERSION,
    ControlPlaneClient,
    ControlPlaneError,
    GraphEdgeRequest,
    GraphRagRequest,
    TensorColumn,
    TensorPhysicalSpec,
)


class ClientTests(unittest.TestCase):
    def test_default_base_url(self) -> None:
        self.assertEqual(DEFAULT_BASE_URL, "http://127.0.0.1:8080/api")
        self.assertEqual(CONTROL_PLANE_API_VERSION, 1)
        self.assertEqual(MIN_MULTIDB_VERSION, "0.1.0")

    def test_success_and_error_envelopes(self) -> None:
        calls = []

        def transport(method, url, headers, body):
            calls.append((method, url, headers, body))
            if url.endswith("/status"):
                return 200, {}, json.dumps({"ok": True, "data": {"server_version": "test"}}).encode()
            return 401, {}, json.dumps({"ok": False, "error": {"code": "unauthorized", "message": "unauthorized"}}).encode()

        client = ControlPlaneClient(base_url="http://unit.test/api", token="secret", transport=transport)
        self.assertEqual(client.status()["server_version"], "test")
        with self.assertRaises(ControlPlaneError) as raised:
            client.auth_me()
        self.assertEqual(raised.exception.code, "unauthorized")
        self.assertEqual(calls[0][2]["Authorization"], "Bearer secret")

    def test_invalid_json_maps_to_typed_error(self) -> None:
        def transport(method, url, headers, body):
            return 200, {}, b"not-json"

        client = ControlPlaneClient(transport=transport)
        with self.assertRaises(ControlPlaneError) as raised:
            client.status()
        self.assertEqual(raised.exception.code, "invalid_json")

    def test_invalid_envelope_maps_to_typed_error(self) -> None:
        def transport(method, url, headers, body):
            return 200, {}, json.dumps({"status": "not-envelope"}).encode()

        client = ControlPlaneClient(transport=transport)
        with self.assertRaises(ControlPlaneError) as raised:
            client.status()
        self.assertEqual(raised.exception.code, "invalid_envelope")

    def test_provisioning_methods_send_documented_paths_and_bodies(self) -> None:
        calls = []

        def transport(method, url, headers, body):
            calls.append((method, url, None if body is None else json.loads(body.decode())))
            return 200, {}, json.dumps({"ok": True, "data": {}}).encode()

        client = ControlPlaneClient(base_url="http://unit.test/api", token="secret", transport=transport)
        spec = {"version": 2, "name": "shop"}
        options = {"allow_destructive": ["collections.orders"]}
        client.provision_state()
        client.provision_plan(spec)
        client.provision_apply(spec, "prov_0123", options)
        client.provision_drift(spec, {"unmanaged": "fail"})
        client.live_config()
        self.assertEqual(
            calls,
            [
                ("GET", "http://unit.test/api/provision/state", None),
                ("POST", "http://unit.test/api/provision/plan", {"spec": spec}),
                (
                    "POST",
                    "http://unit.test/api/provision/apply",
                    {"spec": spec, "options": options, "confirm": "prov_0123"},
                ),
                (
                    "POST",
                    "http://unit.test/api/provision/drift",
                    {"spec": spec, "options": {"unmanaged": "fail"}},
                ),
                ("GET", "http://unit.test/api/config/live", None),
            ],
        )

    def test_blocked_provision_plan_keeps_the_plan_in_the_error(self) -> None:
        plan = {"plan_id": "prov_0123", "outcome": "blocked"}

        def transport(method, url, headers, body):
            payload = {"ok": False, "error": {"code": "provision_blocked", "message": "blocked"}, "data": plan}
            return 422, {}, json.dumps(payload).encode()

        client = ControlPlaneClient(transport=transport)
        with self.assertRaises(ControlPlaneError) as raised:
            client.provision_plan({"version": 2})
        self.assertEqual(raised.exception.status, 422)
        self.assertEqual(raised.exception.code, "provision_blocked")
        self.assertEqual(raised.exception.body["data"], plan)

    def test_graph_rag_and_edges_preserve_payload_results_auth_and_encoded_paths(self) -> None:
        calls = []
        source, target = "01" * 16, "02" * 16
        result = {
            "context": "A retrieved passage",
            "sources": [{"document_id": source, "text": "A retrieved passage", "source": None,
                         "seed_distance": 0.25, "depth": 0, "seed_id": source}],
            "relationships": [{"source": source, "label": "related", "target": target}],
            "truncated": False,
        }

        def transport(method, url, headers, body):
            calls.append((method, url, headers, json.loads(body.decode())))
            data = result if url.endswith("/graphrag/query") else {"written": True}
            return 200, {}, json.dumps({"ok": True, "data": data}).encode()

        client = ControlPlaneClient(base_url="http://unit.test/api/", token="session-token",
                                    principal="reader", transport=transport)
        minimal = GraphRagRequest(vector_collection="chunks", graph="knowledge", query_vector=[1.0, 0.0, 0.0])
        self.assertEqual(client.graph_rag_query(minimal), result)
        bounded = GraphRagRequest(**minimal, seed_k=1, max_depth=2, max_nodes=4, max_edges=8,
                                  max_context_bytes=1024, labels=["related", "cites"],
                                  text_field=["content", "text"], source_field=["provenance", "url"])
        self.assertEqual(client.graph_rag_query(bounded), result)
        edge = GraphEdgeRequest(source=source, label="related", target=target,
                                props={"confidence": 0.9, "tags": ["demo"], "note": None})
        self.assertEqual(client.add_graph_edge("knowledge /?", edge), {"written": True})
        bare_edge = GraphEdgeRequest(source=source, label="cites", target=target)
        self.assertEqual(client.add_graph_edge("knowledge", bare_edge), {"written": True})
        self.assertEqual([(method, url, body) for method, url, _, body in calls], [
            ("POST", "http://unit.test/api/graphrag/query", minimal),
            ("POST", "http://unit.test/api/graphrag/query", bounded),
            ("POST", "http://unit.test/api/data/graphs/knowledge%20%2F%3F/edges", edge),
            ("POST", "http://unit.test/api/data/graphs/knowledge/edges", bare_edge),
        ])
        for _, _, headers, _ in calls:
            self.assertEqual(headers["Authorization"], "Bearer session-token")
            self.assertEqual(headers["x-multidb-principal"], "reader")
            self.assertEqual(headers["Content-Type"], "application/json")
            self.assertEqual(headers["Accept"], "application/json")

    def test_graph_rag_and_edges_preserve_server_errors(self) -> None:
        for status, code in [(400, "invalid_value"), (403, "forbidden"), (404, "not_found")]:
            payload = {"ok": False, "error": {"code": code, "message": "request refused"},
                       "data": {"reason": "fixture"}}

            def transport(method, url, headers, body):
                return status, {}, json.dumps(payload).encode()

            client = ControlPlaneClient(transport=transport)
            for operation in [
                lambda: client.graph_rag_query({"vector_collection": "chunks", "graph": "knowledge", "query_vector": [1.0]}),
                lambda: client.add_graph_edge("knowledge", {"source": "01" * 16, "label": "related", "target": "02" * 16}),
            ]:
                with self.subTest(status=status, operation=operation), self.assertRaises(ControlPlaneError) as raised:
                    operation()
                self.assertEqual(raised.exception.status, status)
                self.assertEqual(raised.exception.code, code)
                self.assertEqual(str(raised.exception), "request refused")
                self.assertEqual(raised.exception.body, payload)

    def test_tensor_methods_preserve_data_revisions_execution_and_encoded_paths(self) -> None:
        calls = []
        config = {"name": "training /?", "tensors": [
            {"name": "image", "dtype": "u8", "shape": [None, None, 3]},
            {"name": "caption", "dtype": "utf8", "shape": []},
            {"name": "embedding", "dtype": "f32", "shape": [3]},
        ]}
        tensors = {
            "image": [{"shape": [1, 1, 3], "data": {"u8": [255, 0, 0]}}],
            "caption": [{"shape": [], "data": {"utf8": ["red pixel"]}}],
            "embedding": [{"shape": [3], "data": {"f32": [1.0, 0.0, 0.0]}}],
        }
        execution = {"device": "gpu", "adapter": "fixture-adapter", "backend": "Vulkan"}
        info = {"config": dict(config, chunk_samples=64, max_chunk_bytes=4194304), "samples": 1, "revision": 1}
        search_result = {"hits": [{"index": 0, "distance": 0.0}], "scanned_samples": 1, "next_start": None,
                         "total_samples": 1, "revision": 1, "skipped_samples": 0, "execution": execution}
        compute_result = {"start": 0, "end": 1, "revision": 1, "values": [1.0], "execution": execution}

        def transport(method, url, headers, body):
            calls.append((method, url, headers, None if body is None else json.loads(body.decode())))
            if url.endswith("/search"):
                data = search_result
            elif url.endswith("/compute"):
                data = compute_result
            elif url.endswith("/append"):
                data = {"start": 0, "end": 1, "revision": 1}
            elif url.endswith("/read"):
                data = {"start": 0, "end": 1, "total_samples": 1, "revision": 1, "tensors": tensors}
            else:
                data = info
            return 200, {}, json.dumps({"ok": True, "data": data}).encode()

        client = ControlPlaneClient(base_url="http://unit.test/api/", token="secret", principal="trainer", transport=transport)
        self.assertEqual(client.create_tensor_dataset(config), info)
        self.assertEqual(client.tensor_dataset_info(config["name"], 0), info)
        client.tensor_dataset_info(config["name"])
        self.assertEqual(client.append_tensors(config["name"], tensors), {"start": 0, "end": 1, "revision": 1})
        read = {"start": 0, "limit": 1, "columns": ["image", "caption"], "revision": 1}
        self.assertEqual(client.read_tensors(config["name"], read)["tensors"], tensors)
        client.read_tensors(config["name"])
        search = {"tensor": "embedding", "query": [1.0, 0.0, 0.0], "k": 1, "metric": "Cosine",
                  "start": 0, "max_samples": 100, "revision": 1}
        self.assertEqual(client.search_tensors(config["name"], search), search_result)
        compute = {"tensor": "embedding", "operation": "sum", "start": 0, "limit": 1, "revision": 1}
        self.assertEqual(client.compute_tensors(config["name"], compute), compute_result)
        base = "http://unit.test/api/data/tensor-datasets/training%20%2F%3F"
        self.assertEqual([(method, url, body) for method, url, _, body in calls], [
            ("POST", "http://unit.test/api/builder/tensor-dataset", config),
            ("GET", base + "?revision=0", None), ("GET", base, None),
            ("POST", base + "/append", {"tensors": tensors}), ("POST", base + "/read", read),
            ("POST", base + "/read", {}), ("POST", base + "/search", search), ("POST", base + "/compute", compute),
        ])
        for _, _, headers, _ in calls:
            self.assertEqual(headers["Authorization"], "Bearer secret")
            self.assertEqual(headers["x-multidb-principal"], "trainer")

    def test_tensor_gpu_errors_preserve_status_and_never_fall_back(self) -> None:
        for status, code in [(400, "invalid_value"), (403, "forbidden"), (404, "not_found"),
                             (422, "tensor_gpu_unsupported"), (503, "tensor_gpu_unavailable")]:
            calls = []

            def transport(method, url, headers, body):
                calls.append(url)
                return status, {}, json.dumps({"ok": False, "error": {"code": code, "message": "request refused"}}).encode()

            client = ControlPlaneClient(transport=transport)
            with self.assertRaises(ControlPlaneError) as raised:
                client.compute_tensors("training", {"tensor": "embedding", "operation": "mean"})
            self.assertEqual(raised.exception.status, status)
            self.assertEqual(raised.exception.code, code)
            self.assertEqual(len(calls), 1)

    def test_tensor_provisioning_preserves_version_two_physical_fields(self) -> None:
        calls = []

        def transport(method, url, headers, body):
            calls.append(json.loads(body.decode()))
            return 200, {}, json.dumps({"ok": True, "data": {"plan_id": "prov_tensor", "outcome": "ready"}}).encode()

        physical = TensorPhysicalSpec(kind="tensor_dataset", tensors=[
            TensorColumn(name="image", dtype="u8", shape=[None, None, 3]),
            TensorColumn(name="embedding", dtype="f32", shape=[3]),
        ], tensor_chunk_samples=64, tensor_max_chunk_bytes=4194304)
        spec = {"version": 2, "name": "training", "profile": "tensor_dataset", "collections": [
            {"name": "training", "role": "analytics", "domain": "local", "indexes": [], "physical": physical},
        ]}
        client = ControlPlaneClient(transport=transport)
        client.provision_plan(spec)
        self.assertEqual(calls, [{"spec": spec}])
        self.assertEqual(calls[0]["spec"]["collections"][0]["physical"], physical)


if __name__ == "__main__":
    unittest.main()
