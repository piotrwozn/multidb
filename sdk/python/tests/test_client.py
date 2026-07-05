import json
import unittest

from multidb_client import (
    CONTROL_PLANE_API_VERSION,
    DEFAULT_BASE_URL,
    MIN_MULTIDB_VERSION,
    ControlPlaneClient,
    ControlPlaneError,
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


if __name__ == "__main__":
    unittest.main()
