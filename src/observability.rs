use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

use crate::{
    autoscale::{ResourceKind, ResourceSignal},
    security::{Permission, Resource},
};

const DEFAULT_MAX_SERIES: usize = 4_096;
const HISTOGRAM_BUCKETS: [f64; 11] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 10.0,
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetricsConfig {
    pub bind_addr: SocketAddr,
    pub max_series: usize,
    pub bearer_token: Option<String>,
}

#[derive(Clone, Debug)]
pub struct MetricsRegistry {
    inner: Arc<Mutex<RegistryInner>>,
    max_series: usize,
}

pub struct MetricsServer;

#[derive(thiserror::Error, Debug)]
pub enum ObservabilityError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("metrics registry lock poisoned")]
    RegistryLock,

    #[error("metrics series limit exceeded")]
    SeriesLimit,

    #[error("metrics request is not authorized")]
    Unauthorized,

    #[error("tracing subscriber: {0}")]
    Tracing(String),
}

#[derive(Clone, Debug, Default)]
struct RegistryInner {
    counters: BTreeMap<MetricKey, u64>,
    histograms: BTreeMap<MetricKey, HistogramValue>,
}

#[derive(Clone, Debug, Default)]
struct HistogramValue {
    count: u64,
    sum: f64,
    buckets: [u64; HISTOGRAM_BUCKETS.len()],
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct MetricKey {
    name: String,
    labels: BTreeMap<String, String>,
}

static GLOBAL_REGISTRY: OnceLock<MetricsRegistry> = OnceLock::new();

impl MetricsConfig {
    #[must_use]
    pub fn new(bind_addr: SocketAddr) -> Self {
        Self {
            bind_addr,
            max_series: DEFAULT_MAX_SERIES,
            bearer_token: None,
        }
    }

    #[must_use]
    pub fn with_max_series(mut self, max_series: usize) -> Self {
        self.max_series = max_series.max(1);
        self
    }

    #[must_use]
    pub fn with_bearer_token(mut self, bearer_token: impl Into<String>) -> Self {
        self.bearer_token = Some(bearer_token.into());
        self
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::with_max_series(DEFAULT_MAX_SERIES)
    }

    #[must_use]
    pub fn with_max_series(max_series: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RegistryInner::default())),
            max_series: max_series.max(1),
        }
    }

    /// Increments a counter.
    /// # Errors
    /// Fails if the registry lock is poisoned.
    pub fn increment_counter(
        &self,
        name: &str,
        labels: &[(&str, &str)],
        value: u64,
    ) -> Result<(), ObservabilityError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| ObservabilityError::RegistryLock)?;
        let key = MetricKey::new(name, labels);
        if !inner.counters.contains_key(&key) && inner.series_count() >= self.max_series {
            return Err(ObservabilityError::SeriesLimit);
        }
        *inner.counters.entry(key).or_default() += value;
        Ok(())
    }

    /// Records one latency sample.
    /// # Errors
    /// Fails if the registry lock is poisoned.
    pub fn observe_histogram(
        &self,
        name: &str,
        labels: &[(&str, &str)],
        value: f64,
    ) -> Result<(), ObservabilityError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| ObservabilityError::RegistryLock)?;
        let key = MetricKey::new(name, labels);
        if !inner.histograms.contains_key(&key) && inner.series_count() >= self.max_series {
            return Err(ObservabilityError::SeriesLimit);
        }
        let histogram = inner.histograms.entry(key).or_default();
        histogram.count = histogram.count.saturating_add(1);
        histogram.sum += value;
        for (index, bucket) in HISTOGRAM_BUCKETS.iter().enumerate() {
            if value <= *bucket {
                histogram.buckets[index] = histogram.buckets[index].saturating_add(1);
            }
        }
        Ok(())
    }

    /// Renders Prometheus text format.
    /// # Errors
    /// Fails if the registry lock is poisoned.
    pub fn render(&self) -> Result<String, ObservabilityError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| ObservabilityError::RegistryLock)?;
        let mut output = String::new();

        for (key, value) in &inner.counters {
            output.push_str("# TYPE ");
            output.push_str(&key.name);
            output.push_str(" counter\n");
            output.push_str(&key.render());
            output.push(' ');
            output.push_str(&value.to_string());
            output.push('\n');
        }

        for (key, value) in &inner.histograms {
            output.push_str("# TYPE ");
            output.push_str(&key.name);
            output.push_str(" histogram\n");
            for (index, bucket) in HISTOGRAM_BUCKETS.iter().enumerate() {
                output.push_str(&key.render_with_extra_label("_bucket", "le", &bucket.to_string()));
                output.push(' ');
                output.push_str(&value.buckets[index].to_string());
                output.push('\n');
            }
            output.push_str(&key.render_with_extra_label("_bucket", "le", "+Inf"));
            output.push(' ');
            output.push_str(&value.count.to_string());
            output.push('\n');
            output.push_str(&key.render_with_suffix("_count"));
            output.push(' ');
            output.push_str(&value.count.to_string());
            output.push('\n');
            output.push_str(&key.render_with_suffix("_sum"));
            output.push(' ');
            output.push_str(&value.sum.to_string());
            output.push('\n');
            for (quantile, estimate) in value.quantiles() {
                output.push_str(&key.render_with_extra_label("_quantile", "quantile", quantile));
                output.push(' ');
                output.push_str(&estimate.to_string());
                output.push('\n');
            }
        }

        Ok(output)
    }
}

impl MetricsServer {
    /// Serves `/metrics` as Prometheus text.
    /// # Errors
    /// Fails when the listener cannot accept a connection.
    pub async fn serve(
        listener: TcpListener,
        registry: MetricsRegistry,
    ) -> Result<(), ObservabilityError> {
        Self::serve_with_auth(listener, registry, None).await
    }

    /// Serves `/metrics` as Prometheus text with optional bearer-token authorization.
    /// # Errors
    /// Fails when the listener cannot accept a connection.
    pub async fn serve_with_auth(
        listener: TcpListener,
        registry: MetricsRegistry,
        bearer_token: Option<String>,
    ) -> Result<(), ObservabilityError> {
        loop {
            let (mut socket, _) = listener.accept().await?;
            let registry = registry.clone();
            let bearer_token = bearer_token.clone();
            tokio::spawn(async move {
                let mut request = vec![0_u8; 2048];
                let read = match socket.read(&mut request).await {
                    Ok(read) => read,
                    Err(error) => {
                        tracing::debug!(?error, "metrics request read failed");
                        return;
                    }
                };
                let request = String::from_utf8_lossy(&request[..read]);
                let response = if let Some(token) = bearer_token
                    && !request
                        .lines()
                        .any(|line| line.trim() == format!("Authorization: Bearer {token}"))
                {
                    http_response("401 Unauthorized", "text/plain", "unauthorized")
                } else {
                    match registry.render() {
                        Ok(body) => http_response("200 OK", "text/plain; version=0.0.4", &body),
                        Err(error) => http_response(
                            "500 Internal Server Error",
                            "text/plain",
                            &format!("metrics error: {error}"),
                        ),
                    }
                };
                if socket.write_all(response.as_bytes()).await.is_err() {
                    tracing::debug!("metrics client disconnected before response");
                }
            });
        }
    }
}

fn http_response(status: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    )
}

impl RegistryInner {
    fn series_count(&self) -> usize {
        self.counters.len().saturating_add(self.histograms.len())
    }
}

impl HistogramValue {
    fn quantiles(&self) -> [(&'static str, f64); 3] {
        [
            ("0.5", self.estimate_quantile_ratio(1, 2)),
            ("0.95", self.estimate_quantile_ratio(95, 100)),
            ("0.99", self.estimate_quantile_ratio(99, 100)),
        ]
    }

    fn estimate_quantile_ratio(&self, numerator: u64, denominator: u64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let target = self
            .count
            .saturating_mul(numerator)
            .saturating_add(denominator.saturating_sub(1))
            / denominator.max(1);
        for (index, count) in self.buckets.iter().enumerate() {
            if *count >= target {
                return HISTOGRAM_BUCKETS[index];
            }
        }
        HISTOGRAM_BUCKETS[HISTOGRAM_BUCKETS.len() - 1]
    }
}

impl MetricKey {
    fn new(name: &str, labels: &[(&str, &str)]) -> Self {
        Self {
            name: sanitize_metric_name(name),
            labels: labels
                .iter()
                .map(|(key, value)| (sanitize_label_name(key), sanitize_label_value(value)))
                .collect(),
        }
    }

    fn render(&self) -> String {
        self.render_with_suffix("")
    }

    fn render_with_suffix(&self, suffix: &str) -> String {
        self.render_with_extra(suffix, None)
    }

    fn render_with_extra_label(&self, suffix: &str, label: &str, value: &str) -> String {
        self.render_with_extra(suffix, Some((label, value)))
    }

    fn render_with_extra(&self, suffix: &str, extra: Option<(&str, &str)>) -> String {
        let mut rendered = self.name.clone();
        rendered.push_str(suffix);
        if self.labels.is_empty() && extra.is_none() {
            return rendered;
        }

        rendered.push('{');
        for (index, (key, value)) in self.labels.iter().enumerate() {
            if index > 0 {
                rendered.push(',');
            }
            rendered.push_str(key);
            rendered.push_str("=\"");
            rendered.push_str(value);
            rendered.push('"');
        }
        if let Some((key, value)) = extra {
            if !self.labels.is_empty() {
                rendered.push(',');
            }
            rendered.push_str(&sanitize_label_name(key));
            rendered.push_str("=\"");
            rendered.push_str(&sanitize_label_value(value));
            rendered.push('"');
        }
        rendered.push('}');
        rendered
    }
}

#[must_use]
pub fn global_registry() -> &'static MetricsRegistry {
    GLOBAL_REGISTRY.get_or_init(MetricsRegistry::new)
}

/// Installs a basic tracing subscriber. Repeated calls are accepted.
/// # Errors
/// Fails only when another incompatible subscriber rejects initialization.
pub fn init_tracing() -> Result<(), ObservabilityError> {
    tracing_subscriber::fmt()
        .with_target(false)
        .try_init()
        .map_err(|error| ObservabilityError::Tracing(error.to_string()))
        .or(Ok(()))
}

pub fn record_query(start: Instant, outcome: &str) {
    let elapsed = duration_seconds(start.elapsed());
    let registry = global_registry();
    if registry
        .increment_counter("multidb_query_total", &[("outcome", outcome)], 1)
        .is_err()
    {
        tracing::debug!("failed to record query counter");
    }
    if registry
        .observe_histogram(
            "multidb_query_latency_seconds",
            &[("outcome", outcome)],
            elapsed,
        )
        .is_err()
    {
        tracing::debug!("failed to record query latency");
    }
}

pub fn record_network_query(start: Instant, user: &str, outcome: &str) {
    let elapsed = duration_seconds(start.elapsed());
    let registry = global_registry();
    if registry
        .increment_counter(
            "multidb_network_query_total",
            &[("user", user), ("outcome", outcome)],
            1,
        )
        .is_err()
    {
        tracing::debug!("failed to record network query counter");
    }
    if registry
        .observe_histogram(
            "multidb_network_query_latency_seconds",
            &[("user", user), ("outcome", outcome)],
            elapsed,
        )
        .is_err()
    {
        tracing::debug!("failed to record network query latency");
    }
}

pub fn record_replication_operation(operation: &str, outcome: &str) {
    if global_registry()
        .increment_counter(
            "multidb_replication_operations_total",
            &[("operation", operation), ("outcome", outcome)],
            1,
        )
        .is_err()
    {
        tracing::debug!("failed to record replication operation");
    }
}

pub fn record_replication_error(kind: &str) {
    if global_registry()
        .increment_counter("multidb_replication_errors_total", &[("kind", kind)], 1)
        .is_err()
    {
        tracing::debug!("failed to record replication error");
    }
}

pub fn record_shard_operation(shard: u32, operation: &str) {
    let shard_label = shard.to_string();
    if global_registry()
        .increment_counter(
            "multidb_shard_operations_total",
            &[("shard", &shard_label), ("operation", operation)],
            1,
        )
        .is_err()
    {
        tracing::debug!("failed to record shard operation");
    }
}

pub fn record_authz_denied(action: &str, resource: &Resource, permission: Permission) {
    let resource_label = resource_label(resource);
    let permission_label = format!("{permission:?}");
    if global_registry()
        .increment_counter(
            "multidb_authz_denied_total",
            &[
                ("action", action),
                ("resource", &resource_label),
                ("permission", &permission_label),
            ],
            1,
        )
        .is_err()
    {
        tracing::debug!("failed to record authz denied");
    }
}

pub fn record_audit_write(outcome: &str) {
    if global_registry()
        .increment_counter("multidb_audit_writes_total", &[("outcome", outcome)], 1)
        .is_err()
    {
        tracing::debug!("failed to record audit write");
    }
}

pub fn record_resource_signal(signal: &ResourceSignal) {
    let (signal_label, kind_label) = resource_signal_labels(signal);
    if global_registry()
        .increment_counter(
            "multidb_resource_signal_total",
            &[("signal", signal_label), ("kind", kind_label)],
            1,
        )
        .is_err()
    {
        tracing::debug!("failed to record resource signal");
    }
}

pub fn record_workload_fingerprint(_fingerprint: &str) {
    if global_registry()
        .increment_counter("multidb_workload_fingerprint_total", &[], 1)
        .is_err()
    {
        tracing::debug!("failed to record workload fingerprint");
    }
}

pub fn record_tuning_decision(parameter: &str, outcome: &str) {
    if global_registry()
        .increment_counter(
            "multidb_tuning_decision_total",
            &[("parameter", parameter), ("outcome", outcome)],
            1,
        )
        .is_err()
    {
        tracing::debug!("failed to record tuning decision");
    }
}

pub fn record_tuning_rollback(parameter: &str) {
    if global_registry()
        .increment_counter(
            "multidb_tuning_rollback_total",
            &[("parameter", parameter)],
            1,
        )
        .is_err()
    {
        tracing::debug!("failed to record tuning rollback");
    }
}

pub fn record_reprofile_lag(seconds: f64) {
    if global_registry()
        .observe_histogram("multidb_reprofile_lag_seconds", &[], seconds.max(0.0))
        .is_err()
    {
        tracing::debug!("failed to record reprofile lag");
    }
}

pub fn record_cache_access(cache: &str, hit: bool) {
    let outcome = if hit { "hit" } else { "miss" };
    if global_registry()
        .increment_counter(
            "multidb_cache_access_total",
            &[("cache", cache), ("outcome", outcome)],
            1,
        )
        .is_err()
    {
        tracing::debug!("failed to record cache access");
    }
}

pub fn record_cloud_object(operation: &str, outcome: &str, bytes: u64) {
    let registry = global_registry();
    if registry
        .increment_counter(
            "multidb_cloud_object_operations_total",
            &[("operation", operation), ("outcome", outcome)],
            1,
        )
        .is_err()
    {
        tracing::debug!("failed to record cloud object operation");
    }
    if registry
        .increment_counter(
            "multidb_cloud_object_bytes_total",
            &[("operation", operation), ("outcome", outcome)],
            bytes,
        )
        .is_err()
    {
        tracing::debug!("failed to record cloud object bytes");
    }
}

pub fn record_cloud_tier_read(location: &str, bytes: u64) {
    let registry = global_registry();
    if registry
        .increment_counter(
            "multidb_cloud_tier_read_total",
            &[("location", location)],
            1,
        )
        .is_err()
    {
        tracing::debug!("failed to record cloud tier read");
    }
    if registry
        .increment_counter(
            "multidb_cloud_tier_read_bytes_total",
            &[("location", location)],
            bytes,
        )
        .is_err()
    {
        tracing::debug!("failed to record cloud tier read bytes");
    }
}

pub fn record_compression(algorithm: &str, input_bytes: usize, output_bytes: usize) {
    let registry = global_registry();
    if registry
        .increment_counter(
            "multidb_compression_operations_total",
            &[("algorithm", algorithm)],
            1,
        )
        .is_err()
    {
        tracing::debug!("failed to record compression operation");
    }
    if registry
        .increment_counter(
            "multidb_compression_bytes_total",
            &[("algorithm", algorithm), ("kind", "input")],
            u64::try_from(input_bytes).unwrap_or(u64::MAX),
        )
        .is_err()
    {
        tracing::debug!("failed to record compression input bytes");
    }
    if registry
        .increment_counter(
            "multidb_compression_bytes_total",
            &[("algorithm", algorithm), ("kind", "output")],
            u64::try_from(output_bytes).unwrap_or(u64::MAX),
        )
        .is_err()
    {
        tracing::debug!("failed to record compression output bytes");
    }
}

pub fn record_group_commit(batch_size: usize, latency: Duration) {
    let registry = global_registry();
    if registry
        .increment_counter("multidb_group_commit_total", &[], 1)
        .is_err()
    {
        tracing::debug!("failed to record group commit counter");
    }
    if registry
        .observe_histogram(
            "multidb_group_commit_batch_size",
            &[],
            usize_to_f64(batch_size),
        )
        .is_err()
    {
        tracing::debug!("failed to record group commit batch size");
    }
    if registry
        .observe_histogram(
            "multidb_group_commit_latency_seconds",
            &[],
            duration_seconds(latency),
        )
        .is_err()
    {
        tracing::debug!("failed to record group commit latency");
    }
}

pub fn record_parallel_scan(table: &str, partitions: usize) {
    let registry = global_registry();
    if registry
        .increment_counter("multidb_parallel_scan_total", &[("table", table)], 1)
        .is_err()
    {
        tracing::debug!("failed to record parallel scan");
    }
    if registry
        .observe_histogram(
            "multidb_parallel_scan_partitions",
            &[("table", table)],
            usize_to_f64(partitions),
        )
        .is_err()
    {
        tracing::debug!("failed to record parallel scan partitions");
    }
}

pub fn record_backup(kind: &str, outcome: &str, latency: Duration) {
    let registry = global_registry();
    if registry
        .increment_counter(
            "multidb_backup_total",
            &[("kind", kind), ("outcome", outcome)],
            1,
        )
        .is_err()
    {
        tracing::debug!("failed to record backup counter");
    }
    if registry
        .observe_histogram(
            "multidb_backup_latency_seconds",
            &[("kind", kind), ("outcome", outcome)],
            duration_seconds(latency),
        )
        .is_err()
    {
        tracing::debug!("failed to record backup latency");
    }
}

pub fn record_restore(outcome: &str, latency: Duration) {
    let registry = global_registry();
    if registry
        .increment_counter("multidb_restore_total", &[("outcome", outcome)], 1)
        .is_err()
    {
        tracing::debug!("failed to record restore counter");
    }
    if registry
        .observe_histogram(
            "multidb_restore_latency_seconds",
            &[("outcome", outcome)],
            duration_seconds(latency),
        )
        .is_err()
    {
        tracing::debug!("failed to record restore latency");
    }
}

pub fn record_backup_verify(outcome: &str, latency: Duration) {
    let registry = global_registry();
    if registry
        .increment_counter("multidb_backup_verify_total", &[("outcome", outcome)], 1)
        .is_err()
    {
        tracing::debug!("failed to record backup verify counter");
    }
    if registry
        .observe_histogram(
            "multidb_backup_verify_latency_seconds",
            &[("outcome", outcome)],
            duration_seconds(latency),
        )
        .is_err()
    {
        tracing::debug!("failed to record backup verify latency");
    }
}

pub fn record_cdc_events(count: usize) {
    if global_registry()
        .increment_counter(
            "multidb_cdc_events_total",
            &[],
            u64::try_from(count).unwrap_or(u64::MAX),
        )
        .is_err()
    {
        tracing::debug!("failed to record cdc events");
    }
}

pub fn record_subscription_push(name: &str, lsn: u64) {
    let registry = global_registry();
    if registry
        .increment_counter("multidb_subscription_push_total", &[("name", name)], 1)
        .is_err()
    {
        tracing::debug!("failed to record subscription push");
    }
    if registry
        .observe_histogram(
            "multidb_subscription_push_lsn",
            &[("name", name)],
            u64_to_f64(lsn),
        )
        .is_err()
    {
        tracing::debug!("failed to record subscription push lsn");
    }
}

pub fn record_subscription_disconnect(reason: &str) {
    if global_registry()
        .increment_counter(
            "multidb_subscription_disconnects_total",
            &[("reason", reason)],
            1,
        )
        .is_err()
    {
        tracing::debug!("failed to record subscription disconnect");
    }
}

pub fn record_internal_transport_connection(outcome: &str) {
    if global_registry()
        .increment_counter(
            "multidb_internal_transport_connections_total",
            &[("outcome", outcome)],
            1,
        )
        .is_err()
    {
        tracing::debug!("failed to record internal transport connection");
    }
}

pub fn record_internal_transport_flow_control(peer: u64, outcome: &str) {
    let peer = peer.to_string();
    if global_registry()
        .increment_counter(
            "multidb_internal_transport_flow_control_total",
            &[("peer", &peer), ("outcome", outcome)],
            1,
        )
        .is_err()
    {
        tracing::debug!("failed to record internal transport flow-control");
    }
}

pub fn record_materialized_view_refresh(name: &str, fresh_to_lsn: u64) {
    let registry = global_registry();
    if registry
        .increment_counter(
            "multidb_materialized_view_refresh_total",
            &[("name", name)],
            1,
        )
        .is_err()
    {
        tracing::debug!("failed to record materialized view refresh");
    }
    if registry
        .observe_histogram(
            "multidb_materialized_view_fresh_to_lsn",
            &[("name", name)],
            u64_to_f64(fresh_to_lsn),
        )
        .is_err()
    {
        tracing::debug!("failed to record materialized view fresh lsn");
    }
}

pub fn record_vector_bruteforce_fallback() {
    if global_registry()
        .increment_counter("multidb_vector_bruteforce_fallback_total", &[], 1)
        .is_err()
    {
        tracing::debug!("failed to record vector brute-force fallback");
    }
}

pub fn record_hook_failure(kind: &str) {
    if global_registry()
        .increment_counter("multidb_hook_failures_total", &[("kind", kind)], 1)
        .is_err()
    {
        tracing::debug!("failed to record hook failure");
    }
}

fn duration_seconds(duration: Duration) -> f64 {
    duration.as_secs_f64()
}

fn usize_to_f64(value: usize) -> f64 {
    value.to_string().parse::<f64>().unwrap_or(f64::MAX)
}

fn u64_to_f64(value: u64) -> f64 {
    value.to_string().parse::<f64>().unwrap_or(f64::MAX)
}

fn resource_label(resource: &Resource) -> String {
    match resource {
        Resource::Database => "database".to_owned(),
        Resource::Table(name) => format!("table:{name}"),
        Resource::Collection(name) => format!("collection:{name}"),
        Resource::VectorCollection(name) => format!("vector:{name}"),
        Resource::FullTextIndex(name) => format!("full_text:{name}"),
        Resource::TimeSeries(name) => format!("time_series:{name}"),
        Resource::Graph(name) => format!("graph:{name}"),
        Resource::GeoIndex(name) => format!("geo:{name}"),
        Resource::System => "system".to_owned(),
    }
}

fn resource_signal_labels(signal: &ResourceSignal) -> (&'static str, &'static str) {
    match signal {
        ResourceSignal::Ok => ("ok", "none"),
        ResourceSignal::RecoverLocal { .. } => ("recover_local", "unknown"),
        ResourceSignal::NeedMore { want, .. } => ("need_more", resource_kind_label(want.kind)),
        ResourceSignal::LimitReached { .. } => ("limit_reached", "unknown"),
    }
}

const fn resource_kind_label(kind: ResourceKind) -> &'static str {
    match kind {
        ResourceKind::Disk => "disk",
        ResourceKind::Memory => "memory",
    }
}

fn sanitize_metric_name(name: &str) -> String {
    sanitize_with(name, '_')
}

fn sanitize_label_name(name: &str) -> String {
    sanitize_with(name, '_')
}

fn sanitize_label_value(value: &str) -> String {
    value
        .chars()
        .filter(|ch| match ch {
            '"' | '\\' | '\n' | '\r' | '\t' => false,
            _ if ch.is_control() => false,
            _ => true,
        })
        .take(120)
        .collect()
}

fn sanitize_with(value: &str, fallback: char) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == ':' {
                ch
            } else {
                fallback
            }
        })
        .collect::<String>();

    if sanitized.is_empty() {
        fallback.to_string()
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::{
        MetricsRegistry, record_audit_write, record_authz_denied, record_backup,
        record_backup_verify, record_cache_access, record_compression, record_group_commit,
        record_network_query, record_parallel_scan, record_replication_operation, record_restore,
    };
    use crate::security::{Permission, Resource};

    #[test]
    fn prometheus_render_contains_counters_and_histograms() {
        let registry = MetricsRegistry::new();
        registry
            .increment_counter("multidb_query_total", &[("outcome", "ok")], 2)
            .unwrap_or_else(|error| panic!("{error}"));
        registry
            .observe_histogram("multidb_query_latency_seconds", &[("outcome", "ok")], 0.25)
            .unwrap_or_else(|error| panic!("{error}"));

        let output = registry.render().unwrap_or_else(|error| panic!("{error}"));

        assert!(output.contains("multidb_query_total{outcome=\"ok\"} 2"));
        assert!(
            output.contains("multidb_query_latency_seconds_bucket{outcome=\"ok\",le=\"0.25\"} 1")
        );
        assert!(output.contains("multidb_query_latency_seconds_count{outcome=\"ok\"} 1"));
        assert!(output.contains("multidb_query_latency_seconds_sum{outcome=\"ok\"} 0.25"));
        assert!(
            output.contains(
                "multidb_query_latency_seconds_quantile{outcome=\"ok\",quantile=\"0.95\"}"
            )
        );
    }

    #[test]
    fn render_sanitizes_label_values() {
        let registry = MetricsRegistry::new();
        registry
            .increment_counter("secret_metric", &[("value", "abc\nsecret\tvalue")], 1)
            .unwrap_or_else(|error| panic!("{error}"));

        let output = registry.render().unwrap_or_else(|error| panic!("{error}"));

        assert!(!output.contains("abc\nsecret"));
        assert!(!output.contains('\t'));
    }

    #[test]
    fn registry_limits_metric_series() {
        let registry = MetricsRegistry::with_max_series(2);
        registry
            .increment_counter("bounded_metric", &[("value", "one")], 1)
            .unwrap_or_else(|error| panic!("{error}"));
        registry
            .increment_counter("bounded_metric", &[("value", "two")], 1)
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(
            registry
                .increment_counter("bounded_metric", &[("value", "three")], 1)
                .is_err()
        );
        registry
            .increment_counter("bounded_metric", &[("value", "one")], 1)
            .unwrap_or_else(|error| panic!("{error}"));
    }

    #[test]
    fn operational_recorders_emit_expected_metric_names() {
        record_network_query(Instant::now(), "alice", "ok");
        record_authz_denied(
            "query",
            &Resource::Table("users".to_owned()),
            Permission::Write,
        );
        record_audit_write("ok");
        record_replication_operation("read", "ok");

        let output = super::global_registry()
            .render()
            .unwrap_or_else(|error| panic!("{error}"));

        assert!(output.contains("multidb_network_query_total"));
        assert!(output.contains("multidb_authz_denied_total"));
        assert!(output.contains("multidb_audit_writes_total"));
        assert!(output.contains("multidb_replication_operations_total"));
    }

    #[test]
    fn performance_recorders_emit_expected_metric_names() {
        record_cache_access("decoded", true);
        record_compression("lz4", 100, 50);
        record_group_commit(3, std::time::Duration::from_millis(2));
        record_parallel_scan("sales", 4);

        let output = super::global_registry()
            .render()
            .unwrap_or_else(|error| panic!("{error}"));

        assert!(output.contains("multidb_cache_access_total"));
        assert!(output.contains("multidb_compression_operations_total"));
        assert!(output.contains("multidb_group_commit_total"));
        assert!(output.contains("multidb_parallel_scan_total"));
    }

    #[test]
    fn backup_recorders_emit_expected_metric_names() {
        record_backup("full", "ok", std::time::Duration::from_millis(2));
        record_restore("ok", std::time::Duration::from_millis(3));
        record_backup_verify("ok", std::time::Duration::from_millis(4));

        let output = super::global_registry()
            .render()
            .unwrap_or_else(|error| panic!("{error}"));

        assert!(output.contains("multidb_backup_total"));
        assert!(output.contains("multidb_restore_total"));
        assert!(output.contains("multidb_backup_verify_total"));
    }
}
