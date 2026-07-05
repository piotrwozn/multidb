use std::{
    collections::{BTreeMap, VecDeque},
    hash::{Hash, Hasher},
    sync::Arc,
    time::Duration,
};

use moka::sync::Cache;
use serde::{Deserialize, Serialize};

use crate::{db::Profile, storage::Bytes};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub benchmark: BenchmarkConfig,
    pub cache: CacheConfig,
    pub compression: CompressionConfig,
    pub io: IoConfig,
    pub parallelism: ParallelismConfig,
    pub query: QueryExecutionConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    pub regression_threshold_percent: u8,
    pub ycsb_rows: usize,
    pub analytical_rows: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheConfig {
    pub enabled: bool,
    pub max_capacity: u64,
    pub scan_bypass_threshold_rows: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompressionConfig {
    pub algorithm: CompressionAlgorithm,
    pub min_bytes: usize,
    pub zstd_level: i32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IoConfig {
    pub group_commit_window_ms: u64,
    pub read_ahead: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParallelismConfig {
    pub max_threads: usize,
    pub target_partitions: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryExecutionConfig {
    pub max_sql_bytes: usize,
    pub max_statements: usize,
    pub max_values_rows: usize,
    pub parser_recursion_limit: usize,
    pub batch_rows: usize,
    pub max_concurrent_queries: usize,
    pub queue_timeout_ms: u64,
    pub timeout_ms: u64,
    pub memory_limit_bytes: usize,
    pub spill_limit_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionAlgorithm {
    #[default]
    None,
    Lz4,
    Zstd,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub name: String,
    pub throughput_ops_per_sec: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PerformanceTruthProfile {
    #[default]
    LocalSmoke,
    CiGate,
    ReleaseBaseline,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerformanceThresholds {
    pub throughput_regression_percent: u8,
    pub latency_regression_percent: u8,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PerformanceReportEnvelope {
    pub schema_version: u8,
    pub profile: PerformanceTruthProfile,
    pub generated_at_utc: String,
    pub git: BTreeMap<String, String>,
    pub environment: BTreeMap<String, String>,
    pub thresholds: PerformanceThresholds,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub calibration_status: Option<String>,
    pub benchmarks: Vec<BenchmarkReport>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct RegressionGate {
    pub threshold_percent: u8,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RegressionResult {
    pub failed: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct PerformanceCache {
    inner: Option<Cache<CacheKey, Arc<Bytes>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheMetrics {
    pub enabled: bool,
    pub entries: u64,
    pub weighted_size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CacheKey {
    table: String,
    key_hash: u64,
    content_hash: u64,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self::for_profile(Profile::Balanced)
    }
}

impl PerformanceConfig {
    #[must_use]
    pub fn for_profile(profile: Profile) -> Self {
        let threads = std::thread::available_parallelism().map_or(1, usize::from);
        let target_partitions = threads.max(1);
        let common_benchmark = BenchmarkConfig::default();
        let query = QueryExecutionConfig::for_profile(profile, threads);

        match profile {
            Profile::InMemory => Self {
                benchmark: common_benchmark,
                cache: CacheConfig::disabled(),
                compression: CompressionConfig::disabled(),
                io: IoConfig::direct(),
                parallelism: ParallelismConfig {
                    max_threads: threads,
                    target_partitions,
                },
                query,
            },
            Profile::Analytical => Self {
                benchmark: common_benchmark,
                cache: CacheConfig {
                    enabled: true,
                    max_capacity: 4_096,
                    scan_bypass_threshold_rows: 512,
                },
                compression: CompressionConfig {
                    algorithm: CompressionAlgorithm::Zstd,
                    min_bytes: 1_024,
                    zstd_level: 3,
                },
                io: IoConfig {
                    group_commit_window_ms: 0,
                    read_ahead: true,
                },
                parallelism: ParallelismConfig {
                    max_threads: threads,
                    target_partitions: target_partitions.max(2),
                },
                query,
            },
            Profile::HighDurability => Self {
                benchmark: common_benchmark,
                cache: CacheConfig::default(),
                compression: CompressionConfig {
                    algorithm: CompressionAlgorithm::Lz4,
                    min_bytes: 256,
                    zstd_level: 0,
                },
                io: IoConfig::direct(),
                parallelism: ParallelismConfig {
                    max_threads: threads,
                    target_partitions,
                },
                query,
            },
            Profile::Vector => Self {
                benchmark: common_benchmark,
                cache: CacheConfig {
                    enabled: true,
                    max_capacity: 8_192,
                    scan_bypass_threshold_rows: 256,
                },
                compression: CompressionConfig::disabled(),
                io: IoConfig {
                    group_commit_window_ms: 1,
                    read_ahead: true,
                },
                parallelism: ParallelismConfig {
                    max_threads: threads,
                    target_partitions,
                },
                query,
            },
            Profile::Transactional
            | Profile::Document
            | Profile::TimeSeries
            | Profile::Balanced => Self {
                benchmark: common_benchmark,
                cache: CacheConfig::default(),
                compression: CompressionConfig {
                    algorithm: CompressionAlgorithm::Lz4,
                    min_bytes: 256,
                    zstd_level: 0,
                },
                io: IoConfig {
                    group_commit_window_ms: 2,
                    read_ahead: false,
                },
                parallelism: ParallelismConfig {
                    max_threads: threads,
                    target_partitions,
                },
                query,
            },
        }
    }

    #[must_use]
    pub const fn group_commit_window(&self) -> Duration {
        Duration::from_millis(self.io.group_commit_window_ms)
    }
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            regression_threshold_percent: 10,
            ycsb_rows: 10_000,
            analytical_rows: 10_000,
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_capacity: 2_048,
            scan_bypass_threshold_rows: 256,
        }
    }
}

impl CacheConfig {
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            max_capacity: 0,
            scan_bypass_threshold_rows: usize::MAX,
        }
    }
}

impl CompressionConfig {
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            algorithm: CompressionAlgorithm::None,
            min_bytes: usize::MAX,
            zstd_level: 0,
        }
    }
}

impl QueryExecutionConfig {
    pub const DEFAULT_MAX_SQL_BYTES: usize = 1024 * 1024;
    pub const DEFAULT_MAX_STATEMENTS: usize = 32;
    pub const DEFAULT_MAX_VALUES_ROWS: usize = 10_000;
    pub const DEFAULT_PARSER_RECURSION_LIMIT: usize = 128;
    pub const DEFAULT_BATCH_ROWS: usize = 1_024;
    pub const DEFAULT_QUEUE_TIMEOUT_MS: u64 = 100;
    pub const DEFAULT_TIMEOUT_MS: u64 = 30_000;
    pub const DEFAULT_SPILL_LIMIT_BYTES: u64 = 2 * 1024 * 1024 * 1024;

    #[must_use]
    pub fn for_profile(profile: Profile, threads: usize) -> Self {
        let memory_limit_bytes = match profile {
            Profile::InMemory => 512 * 1024 * 1024,
            Profile::Analytical | Profile::Vector => 2 * 1024 * 1024 * 1024,
            Profile::HighDurability => 768 * 1024 * 1024,
            Profile::Transactional
            | Profile::Document
            | Profile::TimeSeries
            | Profile::Balanced => 1024 * 1024 * 1024,
        };

        Self {
            max_sql_bytes: Self::DEFAULT_MAX_SQL_BYTES,
            max_statements: Self::DEFAULT_MAX_STATEMENTS,
            max_values_rows: Self::DEFAULT_MAX_VALUES_ROWS,
            parser_recursion_limit: Self::DEFAULT_PARSER_RECURSION_LIMIT,
            batch_rows: Self::DEFAULT_BATCH_ROWS,
            max_concurrent_queries: threads.max(1).saturating_mul(4),
            queue_timeout_ms: Self::DEFAULT_QUEUE_TIMEOUT_MS,
            timeout_ms: Self::DEFAULT_TIMEOUT_MS,
            memory_limit_bytes,
            spill_limit_bytes: Self::DEFAULT_SPILL_LIMIT_BYTES,
        }
    }
}

impl Default for QueryExecutionConfig {
    fn default() -> Self {
        let threads = std::thread::available_parallelism().map_or(1, usize::from);
        Self::for_profile(Profile::Balanced, threads)
    }
}

impl PerformanceTruthProfile {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalSmoke => "local-smoke",
            Self::CiGate => "ci-gate",
            Self::ReleaseBaseline => "release-baseline",
        }
    }

    #[must_use]
    pub const fn default_thresholds(self) -> PerformanceThresholds {
        match self {
            Self::LocalSmoke | Self::CiGate => PerformanceThresholds::new(10, 10),
            Self::ReleaseBaseline => PerformanceThresholds::new(20, 20),
        }
    }
}

impl PerformanceThresholds {
    #[must_use]
    pub const fn new(throughput_regression_percent: u8, latency_regression_percent: u8) -> Self {
        Self {
            throughput_regression_percent,
            latency_regression_percent,
        }
    }
}

impl PerformanceReportEnvelope {
    pub const SCHEMA_VERSION: u8 = 1;

    #[must_use]
    pub fn new(
        profile: PerformanceTruthProfile,
        generated_at_utc: String,
        git: BTreeMap<String, String>,
        environment: BTreeMap<String, String>,
        benchmarks: Vec<BenchmarkReport>,
    ) -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION,
            profile,
            generated_at_utc,
            git,
            environment,
            thresholds: profile.default_thresholds(),
            calibration_status: None,
            benchmarks,
        }
    }
}

impl IoConfig {
    #[must_use]
    pub const fn direct() -> Self {
        Self {
            group_commit_window_ms: 0,
            read_ahead: false,
        }
    }
}

impl RegressionGate {
    #[must_use]
    pub const fn new(threshold_percent: u8) -> Self {
        Self { threshold_percent }
    }

    #[must_use]
    pub fn compare(
        &self,
        baseline: &[BenchmarkReport],
        candidate: &[BenchmarkReport],
    ) -> RegressionResult {
        let mut failed = Vec::new();
        let candidate_by_name = candidate
            .iter()
            .map(|report| (report.name.as_str(), report))
            .collect::<BTreeMap<_, _>>();

        for base in baseline {
            let Some(current) = candidate_by_name.get(base.name.as_str()) else {
                failed.push(format!("missing benchmark {}", base.name));
                continue;
            };
            if is_regressed(
                base.throughput_ops_per_sec,
                current.throughput_ops_per_sec,
                self.threshold_percent,
            ) {
                failed.push(format!("{} throughput regressed", base.name));
            }
            if is_latency_regressed(base.p95_ms, current.p95_ms, self.threshold_percent) {
                failed.push(format!("{} p95 regressed", base.name));
            }
        }

        RegressionResult { failed }
    }
}

impl RegressionResult {
    #[must_use]
    pub const fn is_ok(&self) -> bool {
        self.failed.is_empty()
    }
}

impl PerformanceCache {
    #[must_use]
    pub fn new(config: &CacheConfig) -> Self {
        if !config.enabled || config.max_capacity == 0 {
            return Self { inner: None };
        }

        Self {
            inner: Some(Cache::new(config.max_capacity)),
        }
    }

    #[must_use]
    pub fn get(&self, table: &str, key: &[u8], content: &[u8]) -> Option<Bytes> {
        self.inner
            .as_ref()?
            .get(&CacheKey::new(table, key, content))
            .map(|bytes| bytes.as_ref().clone())
    }

    pub fn insert(&self, table: &str, key: &[u8], content: &[u8], value: Bytes) {
        if let Some(cache) = &self.inner {
            cache.insert(CacheKey::new(table, key, content), Arc::new(value));
        }
    }

    pub fn invalidate_all(&self) {
        if let Some(cache) = &self.inner {
            cache.invalidate_all();
        }
    }

    #[must_use]
    pub fn metrics(&self) -> CacheMetrics {
        let Some(cache) = &self.inner else {
            return CacheMetrics {
                enabled: false,
                entries: 0,
                weighted_size: 0,
            };
        };

        CacheMetrics {
            enabled: true,
            entries: cache.entry_count(),
            weighted_size: cache.weighted_size(),
        }
    }
}

impl CacheKey {
    fn new(table: &str, key: &[u8], content: &[u8]) -> Self {
        Self {
            table: table.to_owned(),
            key_hash: stable_hash(key),
            content_hash: stable_hash(content),
        }
    }
}

impl Hash for CacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table.hash(state);
        self.key_hash.hash(state);
        self.content_hash.hash(state);
    }
}

#[must_use]
pub fn stable_hash(bytes: &[u8]) -> u64 {
    let mut hash = blake3::Hasher::new();
    hash.update(bytes);
    let digest = hash.finalize();
    u64::from_be_bytes(digest.as_bytes()[..8].try_into().unwrap_or([0; 8]))
}

#[must_use]
pub fn split_into_partitions<T>(items: Vec<T>, target_partitions: usize) -> Vec<Vec<T>> {
    if items.is_empty() {
        return Vec::new();
    }
    let partitions = target_partitions.max(1).min(items.len());
    let mut buckets = (0..partitions).map(|_| Vec::new()).collect::<VecDeque<_>>();
    for (index, item) in items.into_iter().enumerate() {
        let bucket = index % partitions;
        if let Some(values) = buckets.get_mut(bucket) {
            values.push(item);
        }
    }
    buckets
        .into_iter()
        .filter(|bucket| !bucket.is_empty())
        .collect()
}

fn is_regressed(baseline: f64, candidate: f64, threshold_percent: u8) -> bool {
    if baseline <= 0.0 {
        return false;
    }
    let allowed = baseline * (1.0 - f64::from(threshold_percent) / 100.0);
    candidate < allowed
}

fn is_latency_regressed(baseline: f64, candidate: f64, threshold_percent: u8) -> bool {
    if baseline <= 0.0 {
        return false;
    }
    let allowed = baseline * (1.0 + f64::from(threshold_percent) / 100.0);
    candidate > allowed
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        BenchmarkReport, CacheConfig, PerformanceCache, PerformanceConfig,
        PerformanceReportEnvelope, PerformanceThresholds, PerformanceTruthProfile, RegressionGate,
        split_into_partitions,
    };
    use crate::db::Profile;

    #[test]
    fn profile_defaults_match_phase_sixteen_intent() {
        assert!(
            !PerformanceConfig::for_profile(Profile::InMemory)
                .cache
                .enabled
        );
        assert!(
            PerformanceConfig::for_profile(Profile::Analytical)
                .io
                .read_ahead
        );
        assert_eq!(
            PerformanceConfig::for_profile(Profile::HighDurability)
                .io
                .group_commit_window_ms,
            0
        );
    }

    #[test]
    fn cache_keys_include_content_hash() {
        let cache = PerformanceCache::new(&CacheConfig::default());
        cache.insert("t", b"k", b"old", b"value-old".to_vec());

        assert_eq!(cache.get("t", b"k", b"old"), Some(b"value-old".to_vec()));
        assert_eq!(cache.get("t", b"k", b"new"), None);
    }

    #[test]
    fn regression_gate_detects_throughput_and_latency_regressions() {
        let gate = RegressionGate::new(10);
        let baseline = vec![BenchmarkReport {
            name: "put".to_owned(),
            throughput_ops_per_sec: 100.0,
            p50_ms: 1.0,
            p95_ms: 10.0,
            p99_ms: 20.0,
            metadata: BTreeMap::default(),
        }];
        let candidate = vec![BenchmarkReport {
            name: "put".to_owned(),
            throughput_ops_per_sec: 70.0,
            p50_ms: 1.0,
            p95_ms: 12.0,
            p99_ms: 22.0,
            metadata: BTreeMap::default(),
        }];

        let result = gate.compare(&baseline, &candidate);

        assert!(!result.is_ok());
        assert_eq!(result.failed.len(), 2);
    }

    #[test]
    fn phase46_truth_profiles_have_release_thresholds() {
        assert_eq!(PerformanceTruthProfile::LocalSmoke.as_str(), "local-smoke");
        assert_eq!(
            PerformanceTruthProfile::CiGate.default_thresholds(),
            PerformanceThresholds::new(10, 10)
        );
        assert_eq!(
            PerformanceTruthProfile::ReleaseBaseline.default_thresholds(),
            PerformanceThresholds::new(20, 20)
        );
    }

    #[test]
    fn phase46_report_envelope_serializes_profile_and_benchmarks()
    -> Result<(), Box<dyn std::error::Error>> {
        let benchmark = BenchmarkReport {
            name: "performance_micro_wall_clock".to_owned(),
            throughput_ops_per_sec: 1.0,
            p50_ms: 1.0,
            p95_ms: 1.0,
            p99_ms: 1.0,
            metadata: BTreeMap::default(),
        };
        let report = PerformanceReportEnvelope::new(
            PerformanceTruthProfile::CiGate,
            "2026-07-04T00:00:00Z".to_owned(),
            BTreeMap::from([("sha".to_owned(), "abc123".to_owned())]),
            BTreeMap::from([("os".to_owned(), "test".to_owned())]),
            vec![benchmark],
        );

        let json = serde_json::to_string(&report)?;

        assert!(json.contains("\"schema_version\":1"));
        assert!(json.contains("\"profile\":\"ci-gate\""));
        assert!(json.contains("performance_micro_wall_clock"));
        Ok(())
    }

    #[test]
    fn partitions_are_non_empty_and_bounded() {
        let partitions = split_into_partitions(vec![1, 2, 3, 4, 5], 3);

        assert_eq!(partitions.len(), 3);
        assert_eq!(partitions.iter().map(Vec::len).sum::<usize>(), 5);
    }
}
