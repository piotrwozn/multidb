use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{ControlPlaneClient, ControlPlaneError, path_escape};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TensorDType {
    U8,
    I64,
    F32,
    F64,
    Bool,
    Utf8,
}

/// Externally tagged JSON arrays, matching the server's native tensor wire format.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TensorData {
    U8(Vec<u8>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Bool(Vec<bool>),
    Utf8(Vec<String>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Tensor {
    pub shape: Vec<usize>,
    pub data: TensorData,
}

/// A shape describes one sample: an empty shape is scalar, None is a ragged dimension.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TensorColumn {
    pub name: String,
    pub dtype: TensorDType,
    pub shape: Vec<Option<usize>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TensorDatasetConfig {
    pub name: String,
    pub tensors: Vec<TensorColumn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_samples: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_chunk_bytes: Option<usize>,
}

impl TensorDatasetConfig {
    /// Omits physical limits so the server uses 64 samples and 4194304 bytes per chunk.
    #[must_use]
    pub fn new(name: impl Into<String>, tensors: Vec<TensorColumn>) -> Self {
        Self {
            name: name.into(),
            tensors,
            chunk_samples: None,
            max_chunk_bytes: None,
        }
    }
}

pub type TensorBatch = BTreeMap<String, Vec<Tensor>>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TensorDatasetInfo {
    pub config: TensorDatasetConfig,
    pub samples: u64,
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TensorAppendResult {
    pub start: u64,
    pub end: u64,
    pub revision: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TensorReadRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TensorReadBatch {
    pub start: u64,
    pub end: u64,
    pub total_samples: u64,
    pub revision: u64,
    pub tensors: TensorBatch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TensorDevice {
    Cpu,
    Gpu,
}

/// The actual server execution device. The client performs no local fallback.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TensorExecution {
    pub device: TensorDevice,
    pub adapter: Option<String>,
    pub backend: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TensorMetric {
    Cosine,
    L2,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TensorSearchRequest {
    pub tensor: String,
    pub query: Vec<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub k: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metric: Option<TensorMetric>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_samples: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<u64>,
}

impl TensorSearchRequest {
    #[must_use]
    pub fn new(tensor: impl Into<String>, query: Vec<f64>) -> Self {
        Self {
            tensor: tensor.into(),
            query,
            k: None,
            metric: None,
            start: None,
            max_samples: None,
            revision: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TensorSearchHit {
    pub index: u64,
    pub distance: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TensorSearchResult {
    pub hits: Vec<TensorSearchHit>,
    pub scanned_samples: u64,
    pub next_start: Option<u64>,
    pub total_samples: u64,
    pub revision: u64,
    pub skipped_samples: u64,
    pub execution: TensorExecution,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TensorGpuOperation {
    Sum,
    Mean,
    L2Norm,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TensorComputeRequest {
    pub tensor: String,
    pub operation: TensorGpuOperation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision: Option<u64>,
}

impl TensorComputeRequest {
    #[must_use]
    pub fn new(tensor: impl Into<String>, operation: TensorGpuOperation) -> Self {
        Self {
            tensor: tensor.into(),
            operation,
            start: None,
            limit: None,
            revision: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TensorComputeResult {
    pub start: u64,
    pub end: u64,
    pub revision: u64,
    pub values: Vec<f32>,
    pub execution: TensorExecution,
}

impl ControlPlaneClient {
    /// Creates a native typed tensor dataset, not a document or SQL table.
    /// # Errors
    /// Fails on transport, authorization, invalid schema or unsupported storage.
    pub fn create_tensor_dataset(
        &self,
        config: &TensorDatasetConfig,
    ) -> Result<TensorDatasetInfo, ControlPlaneError> {
        self.request(
            "POST",
            "/builder/tensor-dataset",
            Some(tensor_body(config)?),
            true,
        )
    }

    /// Reads metadata for the current revision, or an explicit revision including zero.
    /// # Errors
    /// Fails on transport, authorization or missing dataset/revision.
    pub fn tensor_dataset_info(
        &self,
        dataset: impl AsRef<str>,
        revision: Option<u64>,
    ) -> Result<TensorDatasetInfo, ControlPlaneError> {
        let mut path = tensor_path(dataset.as_ref());
        if let Some(revision) = revision {
            path.push_str(&format!("?revision={revision}"));
        }
        self.request("GET", &path, None, true)
    }

    /// Appends one aligned batch of typed tensor samples.
    /// # Errors
    /// Fails on transport, authorization, invalid tensors or storage failure.
    pub fn append_tensors(
        &self,
        dataset: impl AsRef<str>,
        tensors: &TensorBatch,
    ) -> Result<TensorAppendResult, ControlPlaneError> {
        self.request(
            "POST",
            &format!("{}/append", tensor_path(dataset.as_ref())),
            Some(json!({"tensors": tensors})),
            true,
        )
    }

    /// Reads a bounded batch and selected columns, optionally at an immutable revision.
    /// # Errors
    /// Fails on transport, authorization, invalid request or corrupt data.
    pub fn read_tensors(
        &self,
        dataset: impl AsRef<str>,
        request: &TensorReadRequest,
    ) -> Result<TensorReadBatch, ControlPlaneError> {
        self.request(
            "POST",
            &format!("{}/read", tensor_path(dataset.as_ref())),
            Some(tensor_body(request)?),
            true,
        )
    }

    /// Returns nearest tensor samples and the actual server execution device.
    /// # Errors
    /// Fails on transport, authorization, invalid query or GPU/backend errors.
    pub fn search_tensors(
        &self,
        dataset: impl AsRef<str>,
        request: &TensorSearchRequest,
    ) -> Result<TensorSearchResult, ControlPlaneError> {
        self.request(
            "POST",
            &format!("{}/search", tensor_path(dataset.as_ref())),
            Some(tensor_body(request)?),
            true,
        )
    }

    /// Runs a tensor reduction; this SDK never substitutes local CPU execution.
    /// # Errors
    /// Fails on transport, authorization, invalid query or GPU/backend errors.
    pub fn compute_tensors(
        &self,
        dataset: impl AsRef<str>,
        request: &TensorComputeRequest,
    ) -> Result<TensorComputeResult, ControlPlaneError> {
        self.request(
            "POST",
            &format!("{}/compute", tensor_path(dataset.as_ref())),
            Some(tensor_body(request)?),
            true,
        )
    }
}

fn tensor_path(dataset: &str) -> String {
    format!("/data/tensor-datasets/{}", path_escape(dataset))
}

fn tensor_body(body: &impl Serialize) -> Result<Value, ControlPlaneError> {
    serde_json::to_value(body).map_err(|error| ControlPlaneError {
        status: 0,
        code: "invalid_json".into(),
        message: error.to_string(),
        body: Vec::new(),
    })
}
