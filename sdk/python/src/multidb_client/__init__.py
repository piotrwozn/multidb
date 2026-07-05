"""MultiDB Control Plane API v1 client."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Mapping, MutableMapping, Optional, Tuple

JsonValue = Any
JsonObject = MutableMapping[str, JsonValue]
Transport = Callable[[str, str, Mapping[str, str], Optional[bytes]], Tuple[int, Mapping[str, str], bytes]]

DEFAULT_BASE_URL = "http://127.0.0.1:8080/api"
CONTROL_PLANE_API_VERSION = 1
MIN_MULTIDB_VERSION = "0.1.0"


class ControlPlaneError(Exception):
    """Error raised for Control Plane HTTP, envelope, or decode failures."""

    def __init__(self, message: str, *, status: int, code: str, body: Any = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.body = body


@dataclass(frozen=True)
class ControlPlaneClient:
    """Synchronous client for MultiDB's Control Plane API v1."""

    base_url: str = DEFAULT_BASE_URL
    token: str = ""
    principal: Optional[str] = None
    transport: Optional[Transport] = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "base_url", self.base_url.rstrip("/"))

    def with_token(self, token: str, principal: Optional[str] = None) -> "ControlPlaneClient":
        return ControlPlaneClient(
            base_url=self.base_url,
            token=token,
            principal=self.principal if principal is None else principal,
            transport=self.transport,
        )

    def openapi(self) -> JsonObject:
        return self._raw_json("GET", "/openapi.json", auth=False)

    def health(self) -> JsonObject:
        return self._raw_json("GET", "/health", auth=False)

    def ready(self) -> JsonObject:
        return self._raw_json("GET", "/ready", auth=False)

    def status(self) -> JsonObject:
        return self._request("GET", "/status")

    def metrics(self) -> str:
        status, _, body = self._send("GET", "/metrics", None, auth=True)
        if status >= 400:
            self._raise_http(status, body)
        return body.decode("utf-8")

    def login(self, username: str, password: str) -> JsonObject:
        return self._request("POST", "/auth/login", {"username": username, "password": password}, auth=False)

    def logout(self) -> JsonObject:
        return self._request("POST", "/auth/logout")

    def change_password(self, current_password: str, new_password: str) -> JsonObject:
        return self._request(
            "POST",
            "/auth/change-password",
            {"current_password": current_password, "new_password": new_password},
        )

    def auth_me(self) -> JsonObject:
        return self._request("GET", "/auth/me")

    def catalog(self) -> JsonObject:
        return self._request("GET", "/catalog")

    def sql(self, sql: str) -> JsonObject:
        return self._request("POST", "/sql", {"sql": sql})

    def table_rows(self, table: str, *, offset: int = 0, limit: int = 100) -> JsonObject:
        return self._request("GET", f"/data/tables/{_quote(table)}/rows?offset={offset}&limit={limit}")

    def insert_table_row(self, table: str, row: list[JsonValue]) -> JsonObject:
        return self._request("POST", f"/data/tables/{_quote(table)}/rows", {"row": row})

    def update_table_row(self, table: str, row: list[JsonValue]) -> JsonObject:
        return self._request("PUT", f"/data/tables/{_quote(table)}/rows", {"row": row})

    def delete_table_row(self, table: str, primary_key: JsonValue, confirm: str) -> JsonObject:
        return self._request(
            "DELETE",
            f"/data/tables/{_quote(table)}/rows",
            {"primary_key": primary_key, "confirm": confirm},
        )

    def documents(self, collection: str, *, offset: int = 0, limit: int = 100) -> JsonObject:
        return self._request(
            "GET",
            f"/data/collections/{_quote(collection)}/documents?offset={offset}&limit={limit}",
        )

    def create_document(self, collection: str, document: JsonValue) -> JsonObject:
        return self._request(
            "POST",
            f"/data/collections/{_quote(collection)}/documents",
            {"document": document},
        )

    def update_document(self, collection: str, document_id: str, document: JsonValue) -> JsonObject:
        return self._request(
            "PUT",
            f"/data/collections/{_quote(collection)}/documents/{_quote(document_id)}",
            {"document": document},
        )

    def delete_document(self, collection: str, document_id: str, confirm: str) -> JsonObject:
        return self._request(
            "DELETE",
            f"/data/collections/{_quote(collection)}/documents/{_quote(document_id)}",
            {"confirm": confirm},
        )

    def create_table(self, request: JsonObject) -> JsonObject:
        return self._request("POST", "/builder/table", request)

    def create_collection(self, request: JsonObject) -> JsonObject:
        return self._request("POST", "/builder/collection", request)

    def create_vector(self, request: JsonObject) -> JsonObject:
        return self._request("POST", "/builder/vector", request)

    def create_time_series(self, request: JsonObject) -> JsonObject:
        return self._request("POST", "/builder/time-series", request)

    def create_full_text(self, request: JsonObject) -> JsonObject:
        return self._request("POST", "/builder/full-text", request)

    def create_geo_index(self, request: JsonObject) -> JsonObject:
        return self._request("POST", "/builder/geo", request)

    def create_graph(self, request: JsonObject) -> JsonObject:
        return self._request("POST", "/builder/graph", request)

    def insert_vector(self, collection: str, metadata: JsonValue, vector: list[float]) -> JsonObject:
        return self._request(
            "POST",
            f"/data/vectors/{_quote(collection)}/vectors",
            {"metadata": metadata, "vector": vector},
        )

    def search_vector(self, collection: str, vector: list[float], k: int) -> JsonObject:
        return self._request(
            "POST",
            f"/data/vectors/{_quote(collection)}/search",
            {"vector": vector, "k": k},
        )

    def time_series_points(self, collection: str, series: str, start: int, end: int) -> JsonObject:
        return self._request(
            "GET",
            f"/data/time-series/{_quote(collection)}/points?series={_quote(series)}&start={start}&end={end}",
        )

    def insert_time_series_point(self, collection: str, series: str, point: JsonObject) -> JsonObject:
        return self._request(
            "POST",
            f"/data/time-series/{_quote(collection)}/points",
            {"series": series, "point": point},
        )

    def security(self) -> JsonObject:
        return self._request("GET", "/security")

    def save_security(self, security: JsonObject) -> JsonObject:
        return self._request("POST", "/security", security)

    def audit(self) -> JsonObject:
        return self._request("GET", "/audit")

    def config(self) -> JsonObject:
        return self._request("GET", "/config")

    def validate(self, spec: JsonObject) -> JsonObject:
        return self._request("POST", "/config/validate", spec)

    def plan(self, current: JsonObject, desired: JsonObject) -> JsonObject:
        return self._request("POST", "/config/plan", {"current": current, "desired": desired})

    def apply(self, plan: JsonObject, confirm: str) -> JsonObject:
        return self._request("POST", "/config/apply", {"plan": plan, "confirm": confirm})

    def profiles(self) -> JsonValue:
        return self._request("GET", "/profiles")

    def roles(self) -> JsonValue:
        return self._request("GET", "/roles")

    def domains(self) -> JsonValue:
        return self._request("GET", "/domains")

    def extensions(self) -> JsonValue:
        return self._request("GET", "/extensions")

    def advice(self) -> JsonObject:
        return self._request("GET", "/advice")

    def advice_plan(self, advice_id: str) -> JsonObject:
        return self._request("POST", "/advice/plan", {"advice_id": advice_id})

    def record_advice_decision(self, advice_id: str, status: str, reason: str) -> JsonObject:
        return self._request(
            "POST",
            "/advice/decision",
            {"advice_id": advice_id, "status": status, "reason": reason},
        )

    def studio_manifest(self) -> JsonObject:
        return self._request("GET", "/studio")

    def _request(self, method: str, path: str, body: Any = None, *, auth: bool = True) -> Any:
        status, _, raw = self._send(method, path, body, auth=auth)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ControlPlaneError("Control Plane did not return JSON", status=status, code="invalid_json") from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("ok"), bool):
            raise ControlPlaneError("Control Plane returned an invalid envelope", status=status, code="invalid_envelope", body=payload)
        if not payload["ok"]:
            error = payload.get("error", {})
            raise ControlPlaneError(
                str(error.get("message", "Control Plane request failed")),
                status=status,
                code=str(error.get("code", "unknown_error")),
                body=payload,
            )
        return payload.get("data")

    def _raw_json(self, method: str, path: str, body: Any = None, *, auth: bool = True) -> JsonObject:
        status, _, raw = self._send(method, path, body, auth=auth)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ControlPlaneError("Control Plane did not return JSON", status=status, code="invalid_json") from exc
        if status >= 400 and not (isinstance(payload, dict) and "ok" in payload):
            self._raise_http(status, raw)
        return payload

    def _send(self, method: str, path: str, body: Any, *, auth: bool) -> Tuple[int, Mapping[str, str], bytes]:
        encoded = None if body is None else json.dumps(body).encode("utf-8")
        headers: dict[str, str] = {"Accept": "application/json"}
        if encoded is not None:
            headers["Content-Type"] = "application/json"
        if auth:
            headers["Authorization"] = f"Bearer {self.token}"
            if self.principal:
                headers["x-multidb-principal"] = self.principal
        transport = self.transport or _urllib_transport
        return transport(method, f"{self.base_url}{path}", headers, encoded)

    @staticmethod
    def _raise_http(status: int, body: bytes) -> None:
        raise ControlPlaneError(body.decode("utf-8", errors="replace"), status=status, code=f"http_{status}", body=body)


def _urllib_transport(method: str, url: str, headers: Mapping[str, str], body: Optional[bytes]) -> Tuple[int, Mapping[str, str], bytes]:
    request = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), error.read()


def _quote(value: str) -> str:
    return urllib.parse.quote(value, safe="")


__all__ = [
    "ControlPlaneClient",
    "ControlPlaneError",
    "CONTROL_PLANE_API_VERSION",
    "DEFAULT_BASE_URL",
    "MIN_MULTIDB_VERSION",
]
