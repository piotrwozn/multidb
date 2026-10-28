"""MultiDB Control Plane API v1 client."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Literal, Mapping, MutableMapping, Optional, Tuple, TypedDict, Union

JsonValue = Any
JsonObject = MutableMapping[str, JsonValue]
Transport = Callable[[str, str, Mapping[str, str], Optional[bytes]], Tuple[int, Mapping[str, str], bytes]]

DEFAULT_BASE_URL = "http://127.0.0.1:8080/api"
CONTROL_PLANE_API_VERSION = 1
MIN_MULTIDB_VERSION = "0.1.0"


class _GraphRagOptions(TypedDict, total=False):
    seed_k: int  # Server default: 8.
    max_depth: int  # Server default: 2.
    max_nodes: int  # Server default: 64.
    max_edges: int  # Server default: 256.
    max_context_bytes: int  # Server default: 16384 UTF-8 bytes.
    labels: list[str]  # Server default: ["related"].
    text_field: list[str]  # Metadata path; default: ["text"].
    source_field: list[str]  # Metadata path; default: ["source"].


class GraphRagRequest(_GraphRagOptions):
    """Bounded vector-seeded graph retrieval; optional defaults are supplied by the server."""

    vector_collection: str
    graph: str
    query_vector: list[float]


class GraphRagSource(TypedDict):
    document_id: str
    text: str
    source: Optional[str]
    seed_distance: float
    depth: int
    seed_id: str


class GraphRagRelationship(TypedDict):
    source: str
    label: str
    target: str


class GraphRagResult(TypedDict):
    context: str
    sources: list[GraphRagSource]
    relationships: list[GraphRagRelationship]
    truncated: bool


class _GraphEdgeOptions(TypedDict, total=False):
    props: JsonValue


class GraphEdgeRequest(_GraphEdgeOptions):
    """GraphRAG nodes use lowercase 32-character hex ids returned by insert_vector."""

    source: str
    label: str
    target: str


class GraphEdgeWriteResult(TypedDict):
    written: Literal[True]


class VectorInsertResult(TypedDict):
    id: str


TensorDType = Literal["u8", "i64", "f32", "f64", "bool", "utf8"]


class _TensorU8Data(TypedDict):
    u8: list[int]


class _TensorI64Data(TypedDict):
    i64: list[int]


class _TensorF32Data(TypedDict):
    f32: list[float]


class _TensorF64Data(TypedDict):
    f64: list[float]


class _TensorBoolData(TypedDict):
    bool: list[bool]


class _TensorUtf8Data(TypedDict):
    utf8: list[str]


TensorData = Union[_TensorU8Data, _TensorI64Data, _TensorF32Data, _TensorF64Data, _TensorBoolData, _TensorUtf8Data]


class Tensor(TypedDict):
    """One typed sample. Data has exactly one dtype-tagged array, not an arbitrary JSON blob."""

    shape: list[int]
    data: TensorData


class TensorColumn(TypedDict):
    name: str
    dtype: TensorDType
    shape: list[Optional[int]]


class _TensorPhysicalOptions(TypedDict, total=False):
    kind: Literal["tensor_dataset"]
    tensor_chunk_samples: int  # 1..1024, default 64.
    tensor_max_chunk_bytes: int  # 1024..16777216, default 4194304.


class TensorPhysicalSpec(_TensorPhysicalOptions):
    """Version 2 collections[].physical for native typed tensor provisioning.

    Use an analytics or vector_memory role. The tensor_dataset profile requires
    a real GPU; null shape dimensions denote ragged sample dimensions.
    """

    tensors: list[TensorColumn]


class _TensorDatasetOptions(TypedDict, total=False):
    chunk_samples: int  # Server default: 64.
    max_chunk_bytes: int  # Server default: 4194304; HTTP bodies remain capped at 1 MiB.


class TensorDatasetConfig(_TensorDatasetOptions):
    name: str
    tensors: list[TensorColumn]


TensorBatch = dict[str, list[Tensor]]


class TensorAppendRequest(TypedDict):
    tensors: TensorBatch


class TensorDatasetInfo(TypedDict):
    config: TensorDatasetConfig
    samples: int
    revision: int


class TensorAppendResult(TypedDict):
    start: int
    end: int
    revision: int


class TensorReadRequest(TypedDict, total=False):
    start: int
    limit: int
    columns: Optional[list[str]]
    revision: Optional[int]


class TensorReadBatch(TypedDict):
    start: int
    end: int
    total_samples: int
    revision: int
    tensors: TensorBatch


TensorDevice = Literal["cpu", "gpu"]


class TensorExecution(TypedDict):
    device: TensorDevice
    adapter: Optional[str]
    backend: Optional[str]


class _TensorSearchOptions(TypedDict, total=False):
    k: int
    metric: Literal["Cosine", "L2"]
    start: int
    max_samples: int
    revision: Optional[int]


class TensorSearchRequest(_TensorSearchOptions):
    tensor: str
    query: list[float]


class TensorSearchHit(TypedDict):
    index: int
    distance: float


class TensorSearchResult(TypedDict):
    hits: list[TensorSearchHit]
    scanned_samples: int
    next_start: Optional[int]
    total_samples: int
    revision: int
    skipped_samples: int
    execution: TensorExecution


TensorGpuOperation = Literal["sum", "mean", "l2_norm"]


class _TensorComputeOptions(TypedDict, total=False):
    start: int
    limit: int
    revision: Optional[int]


class TensorComputeRequest(_TensorComputeOptions):
    tensor: str
    operation: TensorGpuOperation


class TensorComputeResult(TypedDict):
    start: int
    end: int
    revision: int
    values: list[float]
    execution: TensorExecution


class ProfileVariantSummary(TypedDict):
    name: str
    storage_profile: str
    canonical: bool


class _ProfileSummaryOptions(TypedDict, total=False):
    requires_gpu: bool
    variants: list[ProfileVariantSummary]
    knobs: JsonObject


class ProfileSummary(_ProfileSummaryOptions):
    slug: str
    aliases: list[str]
    status: str
    description: str
    default_domain: str
    compatible_roles: list[str]


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

    def create_tensor_dataset(self, request: TensorDatasetConfig) -> TensorDatasetInfo:
        return self._request("POST", "/builder/tensor-dataset", request)

    def tensor_dataset_info(self, dataset: str, revision: Optional[int] = None) -> TensorDatasetInfo:
        query = "" if revision is None else f"?revision={revision}"
        return self._request("GET", f"/data/tensor-datasets/{_quote(dataset)}{query}")

    def append_tensors(self, dataset: str, tensors: TensorBatch) -> TensorAppendResult:
        return self._request("POST", f"/data/tensor-datasets/{_quote(dataset)}/append", {"tensors": tensors})

    def read_tensors(self, dataset: str, request: Optional[TensorReadRequest] = None) -> TensorReadBatch:
        return self._request("POST", f"/data/tensor-datasets/{_quote(dataset)}/read", {} if request is None else request)

    def search_tensors(self, dataset: str, request: TensorSearchRequest) -> TensorSearchResult:
        return self._request("POST", f"/data/tensor-datasets/{_quote(dataset)}/search", request)

    def compute_tensors(self, dataset: str, request: TensorComputeRequest) -> TensorComputeResult:
        return self._request("POST", f"/data/tensor-datasets/{_quote(dataset)}/compute", request)

    def create_graph(self, request: JsonObject) -> JsonObject:
        return self._request("POST", "/builder/graph", request)

    def add_graph_edge(self, graph: str, request: GraphEdgeRequest) -> GraphEdgeWriteResult:
        return self._request("POST", f"/data/graphs/{_quote(graph)}/edges", request)

    def graph_rag_query(self, request: GraphRagRequest) -> GraphRagResult:
        """Returns retrieved context and provenance, assembled locally without an LLM."""
        return self._request("POST", "/graphrag/query", request)

    def insert_vector(self, collection: str, metadata: JsonValue, vector: list[float]) -> VectorInsertResult:
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

    def provision_state(self) -> JsonObject:
        """What provisioning manages in the served database (preview)."""
        return self._request("GET", "/provision/state")

    def provision_plan(self, spec: JsonObject, options: Optional[JsonObject] = None) -> JsonObject:
        """Plans ``spec`` against the served database (preview, a dry run).

        A blocked plan is HTTP 422: the raised ``ControlPlaneError`` carries the plan in
        ``body["data"]``.
        """
        return self._request("POST", "/provision/plan", _provision_body(spec, options))

    def provision_apply(
        self, spec: JsonObject, confirm: str, options: Optional[JsonObject] = None
    ) -> JsonObject:
        """Applies ``spec`` when ``confirm`` is the ``prov_`` id of its current plan (preview).

        The server recomputes the plan; pass the options the plan was computed with.
        """
        body = _provision_body(spec, options)
        body["confirm"] = confirm
        return self._request("POST", "/provision/apply", body)

    def provision_drift(self, spec: JsonObject, options: Optional[JsonObject] = None) -> JsonObject:
        """How the served database differs from ``spec`` (preview)."""
        return self._request("POST", "/provision/drift", _provision_body(spec, options))

    def live_config(self) -> JsonObject:
        """The served database described as a spec (preview)."""
        return self._request("GET", "/config/live")

    def profiles(self) -> list[ProfileSummary]:
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
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ControlPlaneError("Control Plane did not return JSON", status=status, code="invalid_json") from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("ok"), bool):
            raise ControlPlaneError("Control Plane returned an invalid envelope", status=status, code="invalid_envelope", body=payload)
        if not payload["ok"]:
            error = payload.get("error", {})
            if not isinstance(error, dict):
                error = {}
            raise ControlPlaneError(
                str(error.get("message", "Control Plane request failed")),
                status=status,
                code=str(error.get("code", "unknown_error")),
                body=payload,
            )
        if not 200 <= status < 300 and not (status == 422 and path in ("/config/validate", "/config/plan", "/config/apply")):
            self._raise_http(status, raw)
        return payload.get("data")

    def _raw_json(self, method: str, path: str, body: Any = None, *, auth: bool = True) -> JsonObject:
        status, _, raw = self._send(method, path, body, auth=auth)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise ControlPlaneError("Control Plane did not return JSON", status=status, code="invalid_json") from exc
        if not 200 <= status < 300 and not (path == "/ready" and status == 503):
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
        result = transport(method, f"{self.base_url}{path}", headers, encoded)
        if 300 <= result[0] < 400:
            self._raise_http(result[0], result[2])
        return result

    @staticmethod
    def _raise_http(status: int, body: bytes) -> None:
        raise ControlPlaneError(body.decode("utf-8", errors="replace"), status=status, code=f"http_{status}", body=body)


class _RefuseRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _urllib_transport(method: str, url: str, headers: Mapping[str, str], body: Optional[bytes]) -> Tuple[int, Mapping[str, str], bytes]:
    request = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
    try:
        with urllib.request.build_opener(_RefuseRedirect).open(request, timeout=30) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), error.read()


def _provision_body(spec: JsonObject, options: Optional[JsonObject]) -> dict[str, JsonValue]:
    body: dict[str, JsonValue] = {"spec": spec}
    if options is not None:
        body["options"] = options
    return body


def _quote(value: str) -> str:
    return urllib.parse.quote(value, safe="")


__all__ = [
    "ControlPlaneClient",
    "ControlPlaneError",
    "GraphRagRequest",
    "GraphRagSource",
    "GraphRagRelationship",
    "GraphRagResult",
    "GraphEdgeRequest",
    "GraphEdgeWriteResult",
    "VectorInsertResult",
    "CONTROL_PLANE_API_VERSION",
    "DEFAULT_BASE_URL",
    "MIN_MULTIDB_VERSION",
]
