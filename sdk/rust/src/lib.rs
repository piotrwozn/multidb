#![forbid(unsafe_code)]

use std::{
    fmt,
    io::{Read, Write},
    net::TcpStream,
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};

pub const DEFAULT_BASE_URL: &str = "http://127.0.0.1:8080/api";
pub const CONTROL_PLANE_API_VERSION: u32 = 1;
pub const MIN_MULTIDB_VERSION: &str = "0.1.0";

pub type JsonValue = Value;

#[derive(Clone)]
pub struct ControlPlaneClient {
    base_url: String,
    token: String,
    principal: Option<String>,
    transport: Arc<dyn Transport>,
}

impl Default for ControlPlaneClient {
    fn default() -> Self {
        Self::new()
    }
}

impl ControlPlaneClient {
    #[must_use]
    pub fn new() -> Self {
        Self::with_base_url(DEFAULT_BASE_URL)
    }

    #[must_use]
    pub fn with_base_url(base_url: impl Into<String>) -> Self {
        Self {
            base_url: trim_trailing_slash(base_url.into()),
            token: String::new(),
            principal: None,
            transport: Arc::new(HttpTransport),
        }
    }

    #[must_use]
    pub fn with_transport(base_url: impl Into<String>, transport: Arc<dyn Transport>) -> Self {
        Self {
            base_url: trim_trailing_slash(base_url.into()),
            token: String::new(),
            principal: None,
            transport,
        }
    }

    #[must_use]
    pub fn with_token(&self, token: impl Into<String>) -> Self {
        let mut next = self.clone();
        next.token = token.into();
        next
    }

    #[must_use]
    pub fn with_principal(&self, principal: impl Into<String>) -> Self {
        let mut next = self.clone();
        next.principal = Some(principal.into());
        next
    }

    /// # Errors
    /// Fails when the transport fails or the server returns invalid JSON.
    pub fn openapi(&self) -> Result<Value, ControlPlaneError> {
        self.raw_json("GET", "/openapi.json", None, false)
    }

    /// # Errors
    /// Fails when the transport fails or the server returns invalid JSON.
    pub fn health(&self) -> Result<HealthResponse, ControlPlaneError> {
        self.raw_json("GET", "/health", None, false)
    }

    /// # Errors
    /// Fails when the transport fails or the server returns invalid JSON.
    pub fn ready(&self) -> Result<HealthResponse, ControlPlaneError> {
        self.raw_json("GET", "/ready", None, false)
    }

    /// # Errors
    /// Fails when the request fails or the response envelope is invalid.
    pub fn status(&self) -> Result<Value, ControlPlaneError> {
        self.request("GET", "/status", None, true)
    }

    /// # Errors
    /// Fails when the request fails.
    pub fn metrics(&self) -> Result<String, ControlPlaneError> {
        let response = self.send("GET", "/metrics", None, true)?;
        Ok(String::from_utf8_lossy(&response.body).into_owned())
    }

    /// # Errors
    /// Fails when login fails.
    pub fn login(
        &self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<LoginResponse, ControlPlaneError> {
        self.request(
            "POST",
            "/auth/login",
            Some(json!({ "username": username.into(), "password": password.into() })),
            false,
        )
    }

    /// # Errors
    /// Fails when logout fails.
    pub fn logout(&self) -> Result<Value, ControlPlaneError> {
        self.request("POST", "/auth/logout", None, true)
    }

    /// # Errors
    /// Fails when password change fails.
    pub fn change_password(
        &self,
        current_password: impl Into<String>,
        new_password: impl Into<String>,
    ) -> Result<Value, ControlPlaneError> {
        self.request(
            "POST",
            "/auth/change-password",
            Some(json!({ "current_password": current_password.into(), "new_password": new_password.into() })),
            true,
        )
    }

    /// # Errors
    /// Fails when auth lookup fails.
    pub fn auth_me(&self) -> Result<AuthMeResponse, ControlPlaneError> {
        self.request("GET", "/auth/me", None, true)
    }

    /// # Errors
    /// Fails when request fails.
    pub fn catalog(&self) -> Result<Value, ControlPlaneError> {
        self.request("GET", "/catalog", None, true)
    }

    /// # Errors
    /// Fails when SQL execution fails.
    pub fn sql(&self, sql: impl Into<String>) -> Result<Value, ControlPlaneError> {
        self.request("POST", "/sql", Some(json!({ "sql": sql.into() })), true)
    }

    /// # Errors
    /// Fails when request fails.
    pub fn table_rows(
        &self,
        table: impl AsRef<str>,
        offset: usize,
        limit: usize,
    ) -> Result<Value, ControlPlaneError> {
        self.request(
            "GET",
            &format!(
                "/data/tables/{}/rows?offset={offset}&limit={limit}",
                path_escape(table.as_ref())
            ),
            None,
            true,
        )
    }

    /// # Errors
    /// Fails when request fails.
    pub fn insert_table_row(
        &self,
        table: impl AsRef<str>,
        row: Vec<Value>,
    ) -> Result<Value, ControlPlaneError> {
        self.request(
            "POST",
            &format!("/data/tables/{}/rows", path_escape(table.as_ref())),
            Some(json!({ "row": row })),
            true,
        )
    }

    /// # Errors
    /// Fails when request fails.
    pub fn update_table_row(
        &self,
        table: impl AsRef<str>,
        row: Vec<Value>,
    ) -> Result<Value, ControlPlaneError> {
        self.request(
            "PUT",
            &format!("/data/tables/{}/rows", path_escape(table.as_ref())),
            Some(json!({ "row": row })),
            true,
        )
    }

    /// # Errors
    /// Fails when request fails.
    pub fn delete_table_row(
        &self,
        table: impl AsRef<str>,
        primary_key: Value,
        confirm: impl Into<String>,
    ) -> Result<Value, ControlPlaneError> {
        self.request(
            "DELETE",
            &format!("/data/tables/{}/rows", path_escape(table.as_ref())),
            Some(json!({ "primary_key": primary_key, "confirm": confirm.into() })),
            true,
        )
    }

    /// # Errors
    /// Fails when request fails.
    pub fn documents(
        &self,
        collection: impl AsRef<str>,
        offset: usize,
        limit: usize,
    ) -> Result<Value, ControlPlaneError> {
        self.request(
            "GET",
            &format!(
                "/data/collections/{}/documents?offset={offset}&limit={limit}",
                path_escape(collection.as_ref())
            ),
            None,
            true,
        )
    }

    /// # Errors
    /// Fails when request fails.
    pub fn create_document(
        &self,
        collection: impl AsRef<str>,
        document: Value,
    ) -> Result<Value, ControlPlaneError> {
        self.request(
            "POST",
            &format!(
                "/data/collections/{}/documents",
                path_escape(collection.as_ref())
            ),
            Some(json!({ "document": document })),
            true,
        )
    }

    /// # Errors
    /// Fails when request fails.
    pub fn update_document(
        &self,
        collection: impl AsRef<str>,
        id: impl AsRef<str>,
        document: Value,
    ) -> Result<Value, ControlPlaneError> {
        self.request(
            "PUT",
            &format!(
                "/data/collections/{}/documents/{}",
                path_escape(collection.as_ref()),
                path_escape(id.as_ref())
            ),
            Some(json!({ "document": document })),
            true,
        )
    }

    /// # Errors
    /// Fails when request fails.
    pub fn delete_document(
        &self,
        collection: impl AsRef<str>,
        id: impl AsRef<str>,
        confirm: impl Into<String>,
    ) -> Result<Value, ControlPlaneError> {
        self.request(
            "DELETE",
            &format!(
                "/data/collections/{}/documents/{}",
                path_escape(collection.as_ref()),
                path_escape(id.as_ref())
            ),
            Some(json!({ "confirm": confirm.into() })),
            true,
        )
    }

    pub fn create_table(&self, body: Value) -> Result<Value, ControlPlaneError> {
        self.request("POST", "/builder/table", Some(body), true)
    }

    pub fn create_collection(&self, body: Value) -> Result<Value, ControlPlaneError> {
        self.request("POST", "/builder/collection", Some(body), true)
    }

    pub fn create_vector(&self, body: Value) -> Result<Value, ControlPlaneError> {
        self.request("POST", "/builder/vector", Some(body), true)
    }

    pub fn create_time_series(&self, body: Value) -> Result<Value, ControlPlaneError> {
        self.request("POST", "/builder/time-series", Some(body), true)
    }

    pub fn create_full_text(&self, body: Value) -> Result<Value, ControlPlaneError> {
        self.request("POST", "/builder/full-text", Some(body), true)
    }

    pub fn create_geo_index(&self, body: Value) -> Result<Value, ControlPlaneError> {
        self.request("POST", "/builder/geo", Some(body), true)
    }

    pub fn create_graph(&self, body: Value) -> Result<Value, ControlPlaneError> {
        self.request("POST", "/builder/graph", Some(body), true)
    }

    pub fn insert_vector(
        &self,
        collection: impl AsRef<str>,
        metadata: Value,
        vector: Vec<f64>,
    ) -> Result<Value, ControlPlaneError> {
        self.request(
            "POST",
            &format!("/data/vectors/{}/vectors", path_escape(collection.as_ref())),
            Some(json!({ "metadata": metadata, "vector": vector })),
            true,
        )
    }

    pub fn search_vector(
        &self,
        collection: impl AsRef<str>,
        vector: Vec<f64>,
        k: usize,
    ) -> Result<Value, ControlPlaneError> {
        self.request(
            "POST",
            &format!("/data/vectors/{}/search", path_escape(collection.as_ref())),
            Some(json!({ "vector": vector, "k": k })),
            true,
        )
    }

    pub fn time_series_points(
        &self,
        collection: impl AsRef<str>,
        series: impl AsRef<str>,
        start: i64,
        end: i64,
    ) -> Result<Value, ControlPlaneError> {
        self.request(
            "GET",
            &format!(
                "/data/time-series/{}/points?series={}&start={start}&end={end}",
                path_escape(collection.as_ref()),
                query_escape(series.as_ref())
            ),
            None,
            true,
        )
    }

    pub fn insert_time_series_point(
        &self,
        collection: impl AsRef<str>,
        series: impl Into<String>,
        point: Value,
    ) -> Result<Value, ControlPlaneError> {
        self.request(
            "POST",
            &format!(
                "/data/time-series/{}/points",
                path_escape(collection.as_ref())
            ),
            Some(json!({ "series": series.into(), "point": point })),
            true,
        )
    }

    pub fn security(&self) -> Result<Value, ControlPlaneError> {
        self.request("GET", "/security", None, true)
    }

    pub fn save_security(&self, security: Value) -> Result<Value, ControlPlaneError> {
        self.request("POST", "/security", Some(security), true)
    }

    pub fn audit(&self) -> Result<Value, ControlPlaneError> {
        self.request("GET", "/audit", None, true)
    }

    pub fn config(&self) -> Result<Value, ControlPlaneError> {
        self.request("GET", "/config", None, true)
    }

    pub fn validate(&self, spec: Value) -> Result<Value, ControlPlaneError> {
        self.request("POST", "/config/validate", Some(spec), true)
    }

    pub fn plan(&self, current: Value, desired: Value) -> Result<Value, ControlPlaneError> {
        self.request(
            "POST",
            "/config/plan",
            Some(json!({ "current": current, "desired": desired })),
            true,
        )
    }

    pub fn apply(
        &self,
        plan: Value,
        confirm: impl Into<String>,
    ) -> Result<Value, ControlPlaneError> {
        self.request(
            "POST",
            "/config/apply",
            Some(json!({ "plan": plan, "confirm": confirm.into() })),
            true,
        )
    }

    pub fn profiles(&self) -> Result<Value, ControlPlaneError> {
        self.request("GET", "/profiles", None, true)
    }

    pub fn roles(&self) -> Result<Value, ControlPlaneError> {
        self.request("GET", "/roles", None, true)
    }

    pub fn domains(&self) -> Result<Value, ControlPlaneError> {
        self.request("GET", "/domains", None, true)
    }

    pub fn extensions(&self) -> Result<Value, ControlPlaneError> {
        self.request("GET", "/extensions", None, true)
    }

    pub fn advice(&self) -> Result<Value, ControlPlaneError> {
        self.request("GET", "/advice", None, true)
    }

    pub fn advice_plan(&self, advice_id: impl Into<String>) -> Result<Value, ControlPlaneError> {
        self.request(
            "POST",
            "/advice/plan",
            Some(json!({ "advice_id": advice_id.into() })),
            true,
        )
    }

    pub fn record_advice_decision(
        &self,
        advice_id: impl Into<String>,
        status: impl Into<String>,
        reason: impl Into<String>,
    ) -> Result<Value, ControlPlaneError> {
        self.request(
            "POST",
            "/advice/decision",
            Some(json!({ "advice_id": advice_id.into(), "status": status.into(), "reason": reason.into() })),
            true,
        )
    }

    pub fn studio_manifest(&self) -> Result<Value, ControlPlaneError> {
        self.request("GET", "/studio", None, true)
    }

    fn request<T: DeserializeOwned>(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        auth: bool,
    ) -> Result<T, ControlPlaneError> {
        let response = self.send(method, path, body, auth)?;
        let payload: Value =
            serde_json::from_slice(&response.body).map_err(|_| ControlPlaneError {
                status: response.status,
                code: "invalid_json".to_owned(),
                message: "Control Plane did not return JSON".to_owned(),
                body: response.body.clone(),
            })?;
        let Some(ok) = payload.get("ok").and_then(Value::as_bool) else {
            return Err(ControlPlaneError {
                status: response.status,
                code: "invalid_envelope".to_owned(),
                message: "Control Plane returned an invalid envelope".to_owned(),
                body: response.body,
            });
        };
        if !ok {
            let error = payload.get("error").cloned().unwrap_or(Value::Null);
            let error =
                serde_json::from_value::<ApiErrorBody>(error).unwrap_or_else(|_| ApiErrorBody {
                    code: "unknown_error".to_owned(),
                    message: "Control Plane request failed".to_owned(),
                });
            return Err(ControlPlaneError {
                status: response.status,
                code: error.code,
                message: error.message,
                body: response.body,
            });
        }
        let data = payload.get("data").cloned().unwrap_or(Value::Null);
        serde_json::from_value(data).map_err(|error| ControlPlaneError {
            status: response.status,
            code: "invalid_json".to_owned(),
            message: error.to_string(),
            body: response.body,
        })
    }

    fn raw_json<T: DeserializeOwned>(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        auth: bool,
    ) -> Result<T, ControlPlaneError> {
        let response = self.send(method, path, body, auth)?;
        serde_json::from_slice(&response.body).map_err(|_| ControlPlaneError {
            status: response.status,
            code: "invalid_json".to_owned(),
            message: "Control Plane did not return JSON".to_owned(),
            body: response.body,
        })
    }

    fn send(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        auth: bool,
    ) -> Result<HttpResponse, ControlPlaneError> {
        let payload = body
            .map(|value| serde_json::to_vec(&value))
            .transpose()
            .map_err(|error| ControlPlaneError {
                status: 0,
                code: "invalid_json".to_owned(),
                message: error.to_string(),
                body: Vec::new(),
            })?;
        let mut headers = vec![("Accept".to_owned(), "application/json".to_owned())];
        if payload.is_some() {
            headers.push(("Content-Type".to_owned(), "application/json".to_owned()));
        }
        if auth {
            headers.push(("Authorization".to_owned(), format!("Bearer {}", self.token)));
            if let Some(principal) = self.principal.as_deref() {
                headers.push(("x-multidb-principal".to_owned(), principal.to_owned()));
            }
        }
        self.transport.send(HttpRequest {
            method: method.to_owned(),
            url: format!("{}{}", self.base_url, path),
            headers,
            body: payload.unwrap_or_default(),
        })
    }
}

pub trait Transport: Send + Sync {
    /// # Errors
    /// Fails when the transport cannot complete the request.
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, ControlPlaneError>;
}

pub struct HttpRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

struct HttpTransport;

impl Transport for HttpTransport {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, ControlPlaneError> {
        let parsed = ParsedHttpUrl::parse(&request.url)?;
        let mut stream = TcpStream::connect(&parsed.host_port).map_err(io_error)?;
        let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
        let _ = stream.set_write_timeout(Some(Duration::from_secs(30)));
        write!(
            stream,
            "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Length: {}\r\n",
            request.method,
            parsed.path,
            parsed.host_port,
            request.body.len()
        )
        .map_err(io_error)?;
        for (name, value) in request.headers {
            write!(stream, "{name}: {value}\r\n").map_err(io_error)?;
        }
        stream.write_all(b"\r\n").map_err(io_error)?;
        stream.write_all(&request.body).map_err(io_error)?;

        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).map_err(io_error)?;
        let Some(split) = find_header_body_split(&raw) else {
            return Err(ControlPlaneError {
                status: 0,
                code: "invalid_http".to_owned(),
                message: "HTTP response did not contain a header/body split".to_owned(),
                body: raw,
            });
        };
        let header = String::from_utf8_lossy(&raw[..split]);
        let status = header
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse::<u16>().ok())
            .unwrap_or(0);
        Ok(HttpResponse {
            status,
            body: raw[split + 4..].to_vec(),
        })
    }
}

#[derive(Debug)]
pub struct ControlPlaneError {
    pub status: u16,
    pub code: String,
    pub message: String,
    pub body: Vec<u8>,
}

impl fmt::Display for ControlPlaneError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "multidb control plane {} ({}): {}",
            self.code, self.status, self.message
        )
    }
}

impl std::error::Error for ControlPlaneError {}

#[derive(Debug, Deserialize)]
struct ApiErrorBody {
    code: String,
    message: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct HealthResponse {
    pub ok: bool,
    pub status: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct LoginResponse {
    pub token: String,
    pub expires_at: String,
    pub expires_at_millis: u64,
    pub principal: String,
    pub roles: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct AuthMeResponse {
    pub principal: String,
    pub roles: Vec<String>,
    pub system_admin: bool,
    pub database_admin: bool,
    pub insecure_local_admin: bool,
}

struct ParsedHttpUrl {
    host_port: String,
    path: String,
}

impl ParsedHttpUrl {
    fn parse(url: &str) -> Result<Self, ControlPlaneError> {
        let Some(rest) = url.strip_prefix("http://") else {
            return Err(ControlPlaneError {
                status: 0,
                code: "unsupported_url".to_owned(),
                message: "only http:// URLs are supported by the default transport".to_owned(),
                body: Vec::new(),
            });
        };
        let (host_port, path) = rest
            .split_once('/')
            .map_or((rest, "/"), |(host, path)| (host, path));
        Ok(Self {
            host_port: host_port.to_owned(),
            path: format!("/{path}"),
        })
    }
}

fn trim_trailing_slash(value: String) -> String {
    value.trim_end_matches('/').to_owned()
}

fn io_error(error: std::io::Error) -> ControlPlaneError {
    ControlPlaneError {
        status: 0,
        code: "transport_error".to_owned(),
        message: error.to_string(),
        body: Vec::new(),
    }
}

fn find_header_body_split(raw: &[u8]) -> Option<usize> {
    raw.windows(4).position(|window| window == b"\r\n\r\n")
}

fn path_escape(value: &str) -> String {
    percent_escape(value)
}

fn query_escape(value: &str) -> String {
    percent_escape(value)
}

fn percent_escape(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            output.push(char::from(byte));
        } else {
            output.push_str(&format!("%{byte:02X}"));
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MockTransport {
        calls: Mutex<Vec<HttpRequest>>,
    }

    impl Transport for MockTransport {
        fn send(&self, request: HttpRequest) -> Result<HttpResponse, ControlPlaneError> {
            let mut calls = self.calls.lock().expect("calls lock");
            let status_request = request.url.ends_with("/status");
            calls.push(request);
            if status_request {
                Ok(HttpResponse {
                    status: 200,
                    body: br#"{"ok":true,"data":{"server_version":"test"}}"#.to_vec(),
                })
            } else {
                Ok(HttpResponse {
                    status: 401,
                    body:
                        br#"{"ok":false,"error":{"code":"unauthorized","message":"unauthorized"}}"#
                            .to_vec(),
                })
            }
        }
    }

    #[test]
    fn maps_success_and_error_envelopes() {
        let transport = Arc::new(MockTransport::default());
        let client = ControlPlaneClient::with_transport("http://unit.test/api", transport.clone())
            .with_token("secret");
        assert_eq!(client.status().unwrap()["server_version"], "test");
        let error = client.auth_me().unwrap_err();
        assert_eq!(error.status, 401);
        assert_eq!(error.code, "unauthorized");
        let calls = transport.calls.lock().expect("calls lock");
        assert!(
            calls[0]
                .headers
                .iter()
                .any(|(name, value)| name == "Authorization" && value == "Bearer secret")
        );
    }

    #[test]
    fn exposes_compatibility_constants() {
        assert_eq!(DEFAULT_BASE_URL, "http://127.0.0.1:8080/api");
        assert_eq!(CONTROL_PLANE_API_VERSION, 1);
        assert_eq!(MIN_MULTIDB_VERSION, "0.1.0");
    }

    #[test]
    fn raw_health_is_not_enveloped() {
        struct HealthTransport;
        impl Transport for HealthTransport {
            fn send(&self, _request: HttpRequest) -> Result<HttpResponse, ControlPlaneError> {
                Ok(HttpResponse {
                    status: 200,
                    body: br#"{"ok":true,"status":"alive"}"#.to_vec(),
                })
            }
        }
        let client =
            ControlPlaneClient::with_transport("http://unit.test/api", Arc::new(HealthTransport));
        assert_eq!(
            client.health().unwrap(),
            HealthResponse {
                ok: true,
                status: "alive".to_owned()
            }
        );
    }
}
