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


if __name__ == "__main__":
    unittest.main()
