#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::unnecessary_literal_bound,
    clippy::too_many_lines
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use wasmtime::{
    Config, Engine, ExternType, Instance, Module, Store, StoreLimits, StoreLimitsBuilder,
    Trap as WasmTrap, TypedFunc, ValType,
};

use crate::{
    model::{FieldPath, Value, decode_value, encode_value, extract_path},
    query::QueryError,
    repl::{Op, ReadConsistency, ReplError, Replication, propose_system},
    security::{Permission, Principal, Resource},
    storage::{Bytes, StorageError},
};

pub const EXTENSIONS_TABLE: &str = "__extensions";
pub const UDFS_TABLE: &str = "__udfs";
pub const CODECS_TABLE: &str = "__codecs";
pub const COLLATIONS_TABLE: &str = "__collations";
pub const POLICIES_TABLE: &str = "__policies";

const WASM_PAGE_SIZE: usize = 65_536;
const DEFAULT_FUEL: u64 = 1_000_000;
const DEFAULT_MEMORY_BYTES: usize = 16 * 1_024 * 1_024;
const DEFAULT_TIMEOUT_MS: u64 = 50;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum AbiVersion {
    V1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum UdfKind {
    Scalar,
    Aggregate,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct UdfBudget {
    pub fuel: u64,
    pub memory_bytes: usize,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct WasmModuleSpec {
    pub hash: String,
    pub abi: AbiVersion,
    pub bytes_len: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct UdfSpec {
    pub name: String,
    pub module_hash: String,
    pub kind: UdfKind,
    pub abi: AbiVersion,
    pub entry: String,
    pub budget: UdfBudget,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateWasmFunction {
    pub name: String,
    pub wasm: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CodecId {
    Identity,
    Lz4,
    Zstd,
    Custom(String),
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CodecSpec {
    pub id: String,
    pub version: u32,
    pub wasm_module_hash: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CodecCatalog {
    pub codecs: BTreeMap<String, CodecSpec>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum IndexPluginMode {
    Transactional,
    Derived { resume_lsn: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct IndexPluginSpec {
    pub name: String,
    pub mode: IndexPluginMode,
    pub supports_writes: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum CollationKind {
    Binary,
    CaseInsensitive,
    Numeric,
    Wasm {
        module_hash: String,
        function: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CollationSpec {
    pub id: String,
    pub version: u32,
    pub kind: CollationKind,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ValidationPolicy {
    pub resource: Resource,
    pub required_paths: Vec<FieldPath>,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct MaskingPolicy {
    pub resource: Resource,
    pub paths: Vec<FieldPath>,
    pub replacement: Value,
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct RowPolicy {
    pub resource: Resource,
    pub path: FieldPath,
    pub equals: Value,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct LimitPolicy {
    pub max_value_bytes: usize,
    pub max_batch_ops: usize,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct PolicyConfig {
    pub validations: Vec<ValidationPolicy>,
    pub masking: Vec<MaskingPolicy>,
    pub row_policies: Vec<RowPolicy>,
    pub limits: LimitPolicy,
}

#[derive(thiserror::Error, Debug)]
pub enum ExtensionError {
    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("query: {0}")]
    Query(#[from] QueryError),

    #[error("unsupported extension operation: {0}")]
    Unsupported(String),

    #[error("invalid extension syntax: {0}")]
    InvalidSyntax(String),

    #[error("invalid hex payload")]
    InvalidHex,

    #[error("unknown wasm module: {0}")]
    UnknownModule(String),

    #[error("unknown udf: {0}")]
    UnknownUdf(String),

    #[error("bad wasm ABI: {0}")]
    BadAbi(String),

    #[error("wasm trapped: {0}")]
    Trap(String),

    #[error("wasm ran out of fuel")]
    OutOfFuel,

    #[error("wasm exceeded memory limit")]
    OutOfMemory,

    #[error("wasm timeout")]
    Timeout,

    #[error("policy denied: {0}")]
    PolicyDenied(String),

    #[error("serialization: {0}")]
    Serde(String),
}

pub trait CodecPlugin: Send + Sync {
    fn id(&self) -> &str;
    fn encode(&self, bytes: &[u8]) -> Result<Bytes, ExtensionError>;
    fn decode(&self, bytes: &[u8]) -> Result<Bytes, ExtensionError>;
}

pub trait IndexPlugin: Send + Sync {
    fn spec(&self) -> IndexPluginSpec;
}

pub trait Collation: Send + Sync {
    fn spec(&self) -> CollationSpec;
    fn sort_key(&self, value: &str) -> Result<Bytes, ExtensionError>;
}

#[derive(Default)]
pub struct CodecRegistry {
    plugins: BTreeMap<String, Arc<dyn CodecPlugin>>,
}

#[derive(Default)]
pub struct IndexPluginRegistry {
    plugins: BTreeMap<String, Arc<dyn IndexPlugin>>,
}

#[derive(Default)]
pub struct CollationRegistry {
    collations: BTreeMap<String, Arc<dyn Collation>>,
}

pub struct IdentityCodec;
pub struct Lz4Codec;
pub struct ZstdCodec;

pub struct BuiltinIndexPlugin {
    spec: IndexPluginSpec,
}

pub struct BuiltinCollation {
    spec: CollationSpec,
}

pub struct WasmRuntime {
    engine: Engine,
    modules: Mutex<BTreeMap<String, Arc<Module>>>,
    epoch_stop: Arc<AtomicBool>,
    epoch_thread: Option<JoinHandle<()>>,
}

struct RuntimeState {
    limits: StoreLimits,
}

impl Default for UdfBudget {
    fn default() -> Self {
        Self {
            fuel: DEFAULT_FUEL,
            memory_bytes: DEFAULT_MEMORY_BYTES,
            timeout_ms: DEFAULT_TIMEOUT_MS,
        }
    }
}

impl UdfSpec {
    pub fn scalar(name: impl Into<String>, module_hash: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            module_hash: module_hash.into(),
            kind: UdfKind::Scalar,
            abi: AbiVersion::V1,
            entry: "udf_call".to_owned(),
            budget: UdfBudget::default(),
        }
    }
}

impl PolicyConfig {
    pub fn allow_all() -> Self {
        Self::default()
    }

    pub fn validate_value(&self, resource: &Resource, value: &Value) -> Result<(), ExtensionError> {
        for policy in self
            .validations
            .iter()
            .filter(|policy| &policy.resource == resource)
        {
            for path in &policy.required_paths {
                if extract_path(value, path).is_none() {
                    return Err(ExtensionError::PolicyDenied(format!(
                        "missing required path {} on {resource:?}",
                        path.to_dot_string()
                    )));
                }
            }
        }

        let encoded = encode_value(value)?;
        if encoded.len() > self.limits.max_value_bytes {
            return Err(ExtensionError::PolicyDenied(format!(
                "value size {} exceeds limit {}",
                encoded.len(),
                self.limits.max_value_bytes
            )));
        }

        Ok(())
    }

    pub fn validate_batch(&self, ops: &[Op]) -> Result<(), ExtensionError> {
        if ops.len() > self.limits.max_batch_ops {
            return Err(ExtensionError::PolicyDenied(format!(
                "batch size {} exceeds limit {}",
                ops.len(),
                self.limits.max_batch_ops
            )));
        }

        for op in ops {
            if let Op::Put { value, .. } = op
                && value.len() > self.limits.max_value_bytes
            {
                return Err(ExtensionError::PolicyDenied(format!(
                    "raw value size {} exceeds limit {}",
                    value.len(),
                    self.limits.max_value_bytes
                )));
            }
        }

        Ok(())
    }

    pub fn mask_value(&self, resource: &Resource, value: &Value) -> Value {
        let mut masked = value.clone();
        for policy in self
            .masking
            .iter()
            .filter(|policy| &policy.resource == resource)
        {
            for path in &policy.paths {
                replace_path(&mut masked, path, policy.replacement.clone());
            }
        }
        masked
    }

    pub fn row_visible(&self, principal: &Principal, resource: &Resource, value: &Value) -> bool {
        let policies = self
            .row_policies
            .iter()
            .filter(|policy| &policy.resource == resource)
            .collect::<Vec<_>>();
        if policies.is_empty() {
            return true;
        }

        policies.iter().any(|policy| {
            extract_path(value, &policy.path) == Some(&policy.equals)
                || extract_path(value, &policy.path)
                    == Some(&Value::Str(principal.name().to_owned()))
        })
    }
}

impl Default for LimitPolicy {
    fn default() -> Self {
        Self {
            max_value_bytes: 1_048_576,
            max_batch_ops: 1_024,
        }
    }
}

impl FieldPathExt for FieldPath {
    fn to_dot_string(&self) -> String {
        self.segments().join(".")
    }
}

trait FieldPathExt {
    fn to_dot_string(&self) -> String;
}

impl CodecId {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Identity => "identity",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
            Self::Custom(value) => value,
        }
    }
}

impl CodecRegistry {
    pub fn with_builtins() -> Self {
        let mut registry = Self::default();
        registry.register(Arc::new(IdentityCodec));
        registry.register(Arc::new(Lz4Codec));
        registry.register(Arc::new(ZstdCodec));
        registry
    }

    pub fn register(&mut self, plugin: Arc<dyn CodecPlugin>) {
        self.plugins.insert(plugin.id().to_owned(), plugin);
    }

    pub fn get(&self, id: &str) -> Option<Arc<dyn CodecPlugin>> {
        self.plugins.get(id).cloned()
    }

    pub fn specs(&self) -> Vec<CodecSpec> {
        self.plugins
            .keys()
            .map(|id| CodecSpec {
                id: id.clone(),
                version: 1,
                wasm_module_hash: None,
            })
            .collect()
    }
}

impl CodecPlugin for IdentityCodec {
    fn id(&self) -> &str {
        "identity"
    }

    fn encode(&self, bytes: &[u8]) -> Result<Bytes, ExtensionError> {
        Ok(bytes.to_vec())
    }

    fn decode(&self, bytes: &[u8]) -> Result<Bytes, ExtensionError> {
        Ok(bytes.to_vec())
    }
}

impl CodecPlugin for Lz4Codec {
    fn id(&self) -> &str {
        "lz4"
    }

    fn encode(&self, bytes: &[u8]) -> Result<Bytes, ExtensionError> {
        Ok(lz4_flex::compress_prepend_size(bytes))
    }

    fn decode(&self, bytes: &[u8]) -> Result<Bytes, ExtensionError> {
        lz4_flex::decompress_size_prepended(bytes)
            .map_err(|error| ExtensionError::Serde(error.to_string()))
    }
}

impl CodecPlugin for ZstdCodec {
    fn id(&self) -> &str {
        "zstd"
    }

    fn encode(&self, bytes: &[u8]) -> Result<Bytes, ExtensionError> {
        zstd::bulk::compress(bytes, 0).map_err(|error| ExtensionError::Serde(error.to_string()))
    }

    fn decode(&self, bytes: &[u8]) -> Result<Bytes, ExtensionError> {
        zstd::bulk::decompress(bytes, DEFAULT_MEMORY_BYTES)
            .map_err(|error| ExtensionError::Serde(error.to_string()))
    }
}

impl IndexPluginRegistry {
    pub fn with_builtins() -> Self {
        let mut registry = Self::default();
        for (name, mode) in [
            ("rel_btree", IndexPluginMode::Transactional),
            ("doc_btree", IndexPluginMode::Transactional),
            ("vector_hnsw", IndexPluginMode::Derived { resume_lsn: 0 }),
            ("full_text", IndexPluginMode::Derived { resume_lsn: 0 }),
            ("geo_haversine", IndexPluginMode::Derived { resume_lsn: 0 }),
        ] {
            registry.register(Arc::new(BuiltinIndexPlugin {
                spec: IndexPluginSpec {
                    name: name.to_owned(),
                    mode,
                    supports_writes: true,
                },
            }));
        }
        registry
    }

    pub fn register(&mut self, plugin: Arc<dyn IndexPlugin>) {
        self.plugins.insert(plugin.spec().name, plugin);
    }

    pub fn specs(&self) -> Vec<IndexPluginSpec> {
        self.plugins.values().map(|plugin| plugin.spec()).collect()
    }
}

impl IndexPlugin for BuiltinIndexPlugin {
    fn spec(&self) -> IndexPluginSpec {
        self.spec.clone()
    }
}

impl CollationRegistry {
    pub fn with_builtins() -> Self {
        let mut registry = Self::default();
        for spec in [
            CollationSpec {
                id: "binary".to_owned(),
                version: 1,
                kind: CollationKind::Binary,
            },
            CollationSpec {
                id: "case_insensitive".to_owned(),
                version: 1,
                kind: CollationKind::CaseInsensitive,
            },
            CollationSpec {
                id: "numeric".to_owned(),
                version: 1,
                kind: CollationKind::Numeric,
            },
        ] {
            registry.register(Arc::new(BuiltinCollation { spec }));
        }
        registry
    }

    pub fn register(&mut self, collation: Arc<dyn Collation>) {
        self.collations
            .insert(collation.spec().id.clone(), collation);
    }

    pub fn get(&self, id: &str) -> Option<Arc<dyn Collation>> {
        self.collations.get(id).cloned()
    }

    pub fn specs(&self) -> Vec<CollationSpec> {
        self.collations
            .values()
            .map(|collation| collation.spec())
            .collect()
    }
}

impl Collation for BuiltinCollation {
    fn spec(&self) -> CollationSpec {
        self.spec.clone()
    }

    fn sort_key(&self, value: &str) -> Result<Bytes, ExtensionError> {
        match self.spec.kind {
            CollationKind::Binary => Ok(value.as_bytes().to_vec()),
            CollationKind::CaseInsensitive => Ok(value.to_lowercase().into_bytes()),
            CollationKind::Numeric => Ok(numeric_sort_key(value)),
            CollationKind::Wasm { .. } => Err(ExtensionError::Unsupported(
                "WASM collations must be executed through WasmRuntime".to_owned(),
            )),
        }
    }
}

impl WasmRuntime {
    pub fn new() -> Result<Self, ExtensionError> {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.epoch_interruption(true);
        let engine =
            Engine::new(&config).map_err(|error| ExtensionError::Trap(error.to_string()))?;
        let epoch_stop = Arc::new(AtomicBool::new(false));
        let tick_engine = engine.clone();
        let tick_stop = epoch_stop.clone();
        let epoch_thread = thread::Builder::new()
            .name("multidb-wasm-epoch".to_owned())
            .spawn(move || {
                while !tick_stop.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(1));
                    tick_engine.increment_epoch();
                }
            })
            .map_err(|error| ExtensionError::Trap(error.to_string()))?;
        Ok(Self {
            engine,
            modules: Mutex::new(BTreeMap::new()),
            epoch_stop,
            epoch_thread: Some(epoch_thread),
        })
    }

    pub fn validate_module(&self, wasm: &[u8]) -> Result<WasmModuleSpec, ExtensionError> {
        let module = Module::new(&self.engine, wasm)
            .map_err(|error| ExtensionError::BadAbi(error.to_string()))?;
        validate_udf_exports(&module)?;
        Ok(WasmModuleSpec {
            hash: wasm_hash(wasm),
            abi: AbiVersion::V1,
            bytes_len: wasm.len(),
        })
    }

    pub fn call_udf(
        &self,
        spec: &UdfSpec,
        wasm: &[u8],
        args: &[Value],
    ) -> Result<Value, ExtensionError> {
        if spec.abi != AbiVersion::V1 {
            return Err(ExtensionError::BadAbi("unsupported ABI version".to_owned()));
        }

        let module = self.module(wasm)?;
        let limits = StoreLimitsBuilder::new()
            .memory_size(spec.budget.memory_bytes)
            .build();
        let mut store = Store::new(&self.engine, RuntimeState { limits });
        store.limiter(|state| &mut state.limits);
        store
            .set_fuel(spec.budget.fuel)
            .map_err(|error| ExtensionError::Trap(error.to_string()))?;
        store.set_epoch_deadline(spec.budget.timeout_ms.max(1));
        let instance = Instance::new(&mut store, module.as_ref(), &[])
            .map_err(|error| ExtensionError::BadAbi(error.to_string()))?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| ExtensionError::BadAbi("missing memory export".to_owned()))?;
        let payload = encode_value(&Value::Array(args.to_vec()))?;
        ensure_memory_capacity(&mut store, &memory, payload.len(), spec.budget.memory_bytes)?;
        memory
            .write(&mut store, 0, &payload)
            .map_err(|error| ExtensionError::Trap(error.to_string()))?;
        let entry: TypedFunc<(i32, i32), i64> = instance
            .get_typed_func(&mut store, &spec.entry)
            .map_err(|error| ExtensionError::BadAbi(error.to_string()))?;
        let payload_len = i32::try_from(payload.len())
            .map_err(|_| ExtensionError::BadAbi("input payload too large".to_owned()))?;
        let packed = entry
            .call(&mut store, (0, payload_len))
            .map_err(|error| classify_wasm_error(&error))?;
        let (ptr, len) = unpack_ptr_len(packed)?;
        validate_wasm_output_range(&mut store, &memory, ptr, len, spec.budget.memory_bytes)?;
        let mut output = vec![0; len];
        memory
            .read(&mut store, ptr, &mut output)
            .map_err(|error| ExtensionError::Trap(error.to_string()))?;
        decode_value(&output).map_err(ExtensionError::Storage)
    }

    fn module(&self, wasm: &[u8]) -> Result<Arc<Module>, ExtensionError> {
        let hash = wasm_hash(wasm);
        if let Some(module) = self
            .modules
            .lock()
            .map_err(|_| ExtensionError::Trap("module cache lock poisoned".to_owned()))?
            .get(&hash)
            .cloned()
        {
            return Ok(module);
        }

        let module = Arc::new(
            Module::new(&self.engine, wasm)
                .map_err(|error| ExtensionError::BadAbi(error.to_string()))?,
        );
        self.modules
            .lock()
            .map_err(|_| ExtensionError::Trap("module cache lock poisoned".to_owned()))?
            .insert(hash, module.clone());
        Ok(module)
    }

    #[cfg(test)]
    fn module_cache_len(&self) -> Result<usize, ExtensionError> {
        Ok(self
            .modules
            .lock()
            .map_err(|_| ExtensionError::Trap("module cache lock poisoned".to_owned()))?
            .len())
    }
}

impl Drop for WasmRuntime {
    fn drop(&mut self) {
        self.epoch_stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.epoch_thread.take() {
            let _ = handle.join();
        }
    }
}

pub fn wasm_hash(wasm: &[u8]) -> String {
    blake3::hash(wasm).to_hex().to_string()
}

pub fn register_wasm_module(
    repl: &dyn Replication,
    runtime: &WasmRuntime,
    wasm: &[u8],
) -> Result<WasmModuleSpec, ExtensionError> {
    let spec = runtime.validate_module(wasm)?;
    let value = serde_json::to_vec(&(spec.clone(), wasm))
        .map_err(|error| ExtensionError::Serde(error.to_string()))?;
    propose_system(
        repl,
        Op::Put {
            table: EXTENSIONS_TABLE.to_owned(),
            key: spec.hash.as_bytes().to_vec(),
            value,
        },
    )?;
    Ok(spec)
}

pub fn register_wasm_udf(
    repl: &dyn Replication,
    runtime: &WasmRuntime,
    name: &str,
    wasm: &[u8],
) -> Result<UdfSpec, ExtensionError> {
    validate_identifier(name)?;
    let module = register_wasm_module(repl, runtime, wasm)?;
    let spec = UdfSpec::scalar(name, module.hash);
    propose_system(
        repl,
        Op::Put {
            table: UDFS_TABLE.to_owned(),
            key: name.as_bytes().to_vec(),
            value: serde_json::to_vec(&spec)
                .map_err(|error| ExtensionError::Serde(error.to_string()))?,
        },
    )?;
    Ok(spec)
}

pub fn read_wasm_module(
    repl: &dyn Replication,
    hash: &str,
) -> Result<(WasmModuleSpec, Bytes), ExtensionError> {
    let Some(bytes) = repl.read(EXTENSIONS_TABLE, hash.as_bytes(), ReadConsistency::Strong)? else {
        return Err(ExtensionError::UnknownModule(hash.to_owned()));
    };
    serde_json::from_slice(&bytes).map_err(|error| ExtensionError::Serde(error.to_string()))
}

pub fn read_udf(repl: &dyn Replication, name: &str) -> Result<UdfSpec, ExtensionError> {
    let Some(bytes) = repl.read(UDFS_TABLE, name.as_bytes(), ReadConsistency::Strong)? else {
        return Err(ExtensionError::UnknownUdf(name.to_owned()));
    };
    serde_json::from_slice(&bytes).map_err(|error| ExtensionError::Serde(error.to_string()))
}

pub fn read_udfs(repl: &dyn Replication) -> Result<Vec<UdfSpec>, ExtensionError> {
    repl.range(UDFS_TABLE, &[], &[0xFF], ReadConsistency::Strong)?
        .into_iter()
        .map(|(_, value)| {
            serde_json::from_slice(&value).map_err(|error| ExtensionError::Serde(error.to_string()))
        })
        .collect()
}

pub fn call_registered_udf(
    repl: &dyn Replication,
    runtime: &WasmRuntime,
    name: &str,
    args: &[Value],
) -> Result<Value, ExtensionError> {
    let spec = read_udf(repl, name)?;
    let (_, wasm) = read_wasm_module(repl, &spec.module_hash)?;
    runtime.call_udf(&spec, &wasm, args)
}

pub fn write_policy_config(
    repl: &dyn Replication,
    config: &PolicyConfig,
) -> Result<(), ExtensionError> {
    propose_system(
        repl,
        Op::Put {
            table: POLICIES_TABLE.to_owned(),
            key: b"policy".to_vec(),
            value: serde_json::to_vec(config)
                .map_err(|error| ExtensionError::Serde(error.to_string()))?,
        },
    )?;
    Ok(())
}

pub fn read_policy_config(repl: &dyn Replication) -> Result<PolicyConfig, ExtensionError> {
    let Some(bytes) = repl.read(POLICIES_TABLE, b"policy", ReadConsistency::Strong)? else {
        return Ok(PolicyConfig::default());
    };
    serde_json::from_slice(&bytes).map_err(|error| ExtensionError::Serde(error.to_string()))
}

pub fn register_codec_spec(repl: &dyn Replication, spec: &CodecSpec) -> Result<(), ExtensionError> {
    propose_system(
        repl,
        Op::Put {
            table: CODECS_TABLE.to_owned(),
            key: spec.id.as_bytes().to_vec(),
            value: serde_json::to_vec(spec)
                .map_err(|error| ExtensionError::Serde(error.to_string()))?,
        },
    )?;
    Ok(())
}

pub fn register_collation_spec(
    repl: &dyn Replication,
    spec: &CollationSpec,
) -> Result<(), ExtensionError> {
    propose_system(
        repl,
        Op::Put {
            table: COLLATIONS_TABLE.to_owned(),
            key: spec.id.as_bytes().to_vec(),
            value: serde_json::to_vec(spec)
                .map_err(|error| ExtensionError::Serde(error.to_string()))?,
        },
    )?;
    Ok(())
}

pub fn parse_create_wasm_function(sql: &str) -> Result<Option<CreateWasmFunction>, ExtensionError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_lowercase();
    if !lower.starts_with("create function ") {
        return Ok(None);
    }
    if !lower.contains(" language wasm ") {
        return Err(ExtensionError::InvalidSyntax(
            "CREATE FUNCTION requires LANGUAGE wasm".to_owned(),
        ));
    }

    let after_function = trimmed["create function ".len()..].trim_start();
    let name_end = after_function
        .find(|ch: char| ch.is_whitespace() || ch == '(')
        .ok_or_else(|| ExtensionError::InvalidSyntax("missing function name".to_owned()))?;
    let name = after_function[..name_end].trim_matches('"').to_owned();
    validate_identifier(&name)?;

    let Some(hex_pos) = lower.find(" hex ") else {
        return Err(ExtensionError::InvalidSyntax(
            "CREATE FUNCTION requires AS HEX '<wasm>'".to_owned(),
        ));
    };
    let hex_part = trimmed[hex_pos + " hex ".len()..].trim();
    let Some(first_quote) = hex_part.find('\'') else {
        return Err(ExtensionError::InvalidSyntax(
            "missing HEX quote".to_owned(),
        ));
    };
    let rest = &hex_part[first_quote + 1..];
    let Some(second_quote) = rest.find('\'') else {
        return Err(ExtensionError::InvalidSyntax(
            "unterminated HEX quote".to_owned(),
        ));
    };
    let wasm = decode_hex(&rest[..second_quote])?;
    Ok(Some(CreateWasmFunction { name, wasm }))
}

pub fn parse_select_udf_call(sql: &str) -> Result<Option<(String, Vec<Value>)>, ExtensionError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_lowercase();
    if !lower.starts_with("select ") || lower.contains(" from ") {
        return Ok(None);
    }

    let expr = trimmed["select ".len()..].trim();
    let Some(open) = expr.find('(') else {
        return Ok(None);
    };
    if !expr.ends_with(')') {
        return Ok(None);
    }
    let name = expr[..open].trim().trim_matches('"').to_owned();
    validate_identifier(&name)?;
    let args = expr[open + 1..expr.len() - 1].trim();
    if args.is_empty() {
        return Ok(Some((name, Vec::new())));
    }
    let values = split_args(args)?
        .into_iter()
        .map(|arg| parse_literal_value(&arg))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some((name, values)))
}

pub fn extension_sql_requirements(
    sql: &str,
) -> Result<Option<Vec<(Resource, Permission)>>, ExtensionError> {
    if parse_create_wasm_function(sql)?.is_some() {
        return Ok(Some(vec![(Resource::System, Permission::Admin)]));
    }
    if parse_select_udf_call(sql)?.is_some() {
        return Ok(Some(vec![(Resource::Database, Permission::Read)]));
    }
    Ok(None)
}

pub fn builtin_collation_key(id: &str, value: &str) -> Result<Bytes, ExtensionError> {
    let registry = CollationRegistry::with_builtins();
    let collation = registry
        .get(id)
        .ok_or_else(|| ExtensionError::Unsupported(format!("unknown collation {id}")))?;
    collation.sort_key(value)
}

pub fn active_codec_ids(repl: &dyn Replication) -> Result<BTreeSet<String>, ExtensionError> {
    Ok(repl
        .range(CODECS_TABLE, &[], &[0xFF], ReadConsistency::Strong)?
        .into_iter()
        .map(|(key, _)| String::from_utf8_lossy(&key).into_owned())
        .collect())
}

fn classify_wasm_error(error: &wasmtime::Error) -> ExtensionError {
    if let Some(trap) = error.downcast_ref::<WasmTrap>() {
        return match trap {
            WasmTrap::Interrupt => ExtensionError::Timeout,
            WasmTrap::OutOfFuel => ExtensionError::OutOfFuel,
            WasmTrap::MemoryOutOfBounds | WasmTrap::AllocationTooLarge => {
                ExtensionError::OutOfMemory
            }
            _ => ExtensionError::Trap(trap.to_string()),
        };
    }

    let message = error.to_string();
    if message.contains("epoch") || message.contains("interrupt") {
        ExtensionError::Timeout
    } else if message.contains("all fuel consumed") || message.contains("fuel") {
        ExtensionError::OutOfFuel
    } else if message.contains("memory") && message.contains("limit") {
        ExtensionError::OutOfMemory
    } else {
        ExtensionError::Trap(message)
    }
}

fn validate_udf_exports(module: &Module) -> Result<(), ExtensionError> {
    let mut has_memory = false;
    let mut entry_valid = false;
    for export in module.exports() {
        match (export.name(), export.ty()) {
            ("memory", ExternType::Memory(_)) => has_memory = true,
            ("udf_call", ExternType::Func(func)) => {
                let mut params = func.params();
                let mut results = func.results();
                entry_valid = matches!(params.next(), Some(ValType::I32))
                    && matches!(params.next(), Some(ValType::I32))
                    && params.next().is_none()
                    && matches!(results.next(), Some(ValType::I64))
                    && results.next().is_none();
            }
            _ => {}
        }
    }
    if !has_memory {
        return Err(ExtensionError::BadAbi("missing memory export".to_owned()));
    }
    if !entry_valid {
        return Err(ExtensionError::BadAbi(
            "missing udf_call(i32, i32) -> i64 export".to_owned(),
        ));
    }
    Ok(())
}

fn ensure_memory_capacity(
    store: &mut Store<RuntimeState>,
    memory: &wasmtime::Memory,
    bytes: usize,
    memory_limit: usize,
) -> Result<(), ExtensionError> {
    if bytes > memory_limit {
        return Err(ExtensionError::OutOfMemory);
    }
    let current = memory.data_size(&mut *store);
    if current >= bytes {
        return Ok(());
    }
    let needed = bytes - current;
    let pages = needed.div_ceil(WASM_PAGE_SIZE);
    let pages = u64::try_from(pages)
        .map_err(|_| ExtensionError::BadAbi("memory grow overflow".to_owned()))?;
    memory
        .grow(&mut *store, pages)
        .map_err(|_| ExtensionError::OutOfMemory)?;
    Ok(())
}

fn unpack_ptr_len(packed: i64) -> Result<(usize, usize), ExtensionError> {
    let packed = u64::try_from(packed)
        .map_err(|_| ExtensionError::BadAbi("negative pointer/length result".to_owned()))?;
    let ptr = usize::try_from(packed >> 32)
        .map_err(|_| ExtensionError::BadAbi("pointer too large".to_owned()))?;
    let len = usize::try_from(packed & 0xFFFF_FFFF)
        .map_err(|_| ExtensionError::BadAbi("length too large".to_owned()))?;
    Ok((ptr, len))
}

fn validate_wasm_output_range(
    store: &mut Store<RuntimeState>,
    memory: &wasmtime::Memory,
    ptr: usize,
    len: usize,
    memory_limit: usize,
) -> Result<(), ExtensionError> {
    if len > memory_limit {
        return Err(ExtensionError::OutOfMemory);
    }
    let end = ptr
        .checked_add(len)
        .ok_or_else(|| ExtensionError::BadAbi("output pointer overflow".to_owned()))?;
    let memory_size = memory.data_size(store);
    if end > memory_size {
        return Err(ExtensionError::BadAbi(format!(
            "output range {ptr}..{end} exceeds wasm memory size {memory_size}"
        )));
    }
    Ok(())
}

fn validate_identifier(value: &str) -> Result<(), ExtensionError> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err(ExtensionError::InvalidSyntax("empty identifier".to_owned()));
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(ExtensionError::InvalidSyntax(format!(
            "invalid identifier {value}"
        )));
    }
    if chars.any(|ch| !(ch == '_' || ch.is_ascii_alphanumeric())) {
        return Err(ExtensionError::InvalidSyntax(format!(
            "invalid identifier {value}"
        )));
    }
    Ok(())
}

fn decode_hex(value: &str) -> Result<Bytes, ExtensionError> {
    let compact = value
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>();
    if compact.len() % 2 != 0 {
        return Err(ExtensionError::InvalidHex);
    }
    compact
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            let hi = hex_value(pair[0])?;
            let lo = hex_value(pair[1])?;
            Ok((hi << 4) | lo)
        })
        .collect()
}

fn hex_value(byte: u8) -> Result<u8, ExtensionError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(ExtensionError::InvalidHex),
    }
}

pub(crate) fn split_args(args: &str) -> Result<Vec<String>, ExtensionError> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut chars = args.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                current.push(ch);
                if in_string && chars.peek() == Some(&'\'') {
                    current.push('\'');
                    let _ = chars.next();
                } else {
                    in_string = !in_string;
                }
            }
            ',' if !in_string => {
                values.push(current.trim().to_owned());
                current.clear();
            }
            other => current.push(other),
        }
    }
    if in_string {
        return Err(ExtensionError::InvalidSyntax(
            "unterminated string literal".to_owned(),
        ));
    }
    if !current.trim().is_empty() {
        values.push(current.trim().to_owned());
    }
    Ok(values)
}

pub(crate) fn parse_literal_value(value: &str) -> Result<Value, ExtensionError> {
    if value.eq_ignore_ascii_case("null") {
        return Ok(Value::Null);
    }
    if value.eq_ignore_ascii_case("true") {
        return Ok(Value::Bool(true));
    }
    if value.eq_ignore_ascii_case("false") {
        return Ok(Value::Bool(false));
    }
    if let Some(stripped) = value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')) {
        return Ok(Value::Str(stripped.replace("''", "'")));
    }
    if value.contains('.') {
        return value
            .parse::<f64>()
            .map(Value::Float)
            .map_err(|error| ExtensionError::InvalidSyntax(error.to_string()));
    }
    value
        .parse::<i64>()
        .map(Value::Int)
        .map_err(|error| ExtensionError::InvalidSyntax(error.to_string()))
}

fn numeric_sort_key(value: &str) -> Bytes {
    let mut out = Vec::new();
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_digit() {
            let mut digits = String::new();
            while let Some(digit) = chars.peek().copied() {
                if digit.is_ascii_digit() {
                    digits.push(digit);
                    let _ = chars.next();
                } else {
                    break;
                }
            }
            let trimmed = digits.trim_start_matches('0');
            let normalized = if trimmed.is_empty() { "0" } else { trimmed };
            out.push(0x01);
            let len = u32::try_from(normalized.len()).unwrap_or(u32::MAX);
            out.extend_from_slice(&len.to_be_bytes());
            out.extend_from_slice(normalized.as_bytes());
        } else {
            out.push(0x00);
            out.extend(ch.to_lowercase().to_string().as_bytes());
            let _ = chars.next();
        }
    }
    out
}

fn replace_path(value: &mut Value, path: &FieldPath, replacement: Value) {
    fn inner(value: &mut Value, segments: &[String], replacement: Value) {
        let Some((head, tail)) = segments.split_first() else {
            *value = replacement;
            return;
        };
        let Value::Object(map) = value else {
            return;
        };
        if tail.is_empty() {
            if let Some(slot) = map.get_mut(head) {
                *slot = replacement;
            }
        } else if let Some(next) = map.get_mut(head) {
            inner(next, tail, replacement);
        }
    }
    inner(value, path.segments(), replacement);
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        CodecRegistry, CollationRegistry, ExtensionError, PolicyConfig, UdfSpec, WasmRuntime,
        builtin_collation_key, parse_create_wasm_function, parse_select_udf_call,
    };
    use crate::{
        extension::{LimitPolicy, MaskingPolicy, ValidationPolicy},
        model::{FieldPath, Value, encode_value},
        security::Resource,
    };

    fn constant_value_wasm(value: &Value) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let encoded = encode_value(value)?;
        let bytes = wat_data_bytes(&encoded);
        let len = encoded.len();
        let wat = format!(
            r#"
            (module
              (memory (export "memory") 1)
              (data (i32.const 4096) "{bytes}")
              (func (export "udf_call") (param i32 i32) (result i64)
                (i64.or
                  (i64.shl (i64.const 4096) (i64.const 32))
                  (i64.const {len}))))
            "#
        );
        Ok(wat::parse_str(wat)?)
    }

    fn output_range_wasm(ptr: u32, len: u32) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let packed = (u64::from(ptr) << 32) | u64::from(len);
        let wat = format!(
            r#"
            (module
              (memory (export "memory") 1)
              (func (export "udf_call") (param i32 i32) (result i64)
                (i64.const {packed})))
            "#
        );
        Ok(wat::parse_str(wat)?)
    }

    fn wat_data_bytes(bytes: &[u8]) -> String {
        let mut out = String::with_capacity(bytes.len() * 3);
        for byte in bytes {
            out.push('\\');
            out.push(hex_digit(byte >> 4));
            out.push(hex_digit(byte & 0x0F));
        }
        out
    }

    fn hex_digit(value: u8) -> char {
        char::from(b"0123456789abcdef"[usize::from(value)])
    }

    #[test]
    fn wasm_runtime_executes_scalar_udf() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = WasmRuntime::new()?;
        let wasm = constant_value_wasm(&Value::Int(42))?;
        let hash = super::wasm_hash(&wasm);
        let spec = UdfSpec::scalar("answer", hash);
        assert_eq!(runtime.call_udf(&spec, &wasm, &[])?, Value::Int(42));
        Ok(())
    }

    #[test]
    fn wasm_runtime_rejects_missing_memory() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = WasmRuntime::new()?;
        let wasm = wat::parse_str(
            r#"
            (module
              (func (export "udf_call") (param i32 i32) (result i64)
                (i64.const 0)))
            "#,
        )?;
        let spec = UdfSpec::scalar("bad", super::wasm_hash(&wasm));
        assert!(matches!(
            runtime.call_udf(&spec, &wasm, &[]),
            Err(ExtensionError::BadAbi(_))
        ));
        Ok(())
    }

    #[test]
    fn wasm_validation_rejects_missing_entry() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = WasmRuntime::new()?;
        let wasm = wat::parse_str(
            r#"
            (module
              (memory (export "memory") 1))
            "#,
        )?;

        assert!(matches!(
            runtime.validate_module(&wasm),
            Err(ExtensionError::BadAbi(_))
        ));
        Ok(())
    }

    #[test]
    fn wasm_runtime_rejects_oversized_output_before_allocation()
    -> Result<(), Box<dyn std::error::Error>> {
        let runtime = WasmRuntime::new()?;
        let wasm = output_range_wasm(0, u32::MAX)?;
        let spec = UdfSpec::scalar("bad_len", super::wasm_hash(&wasm));

        assert!(matches!(
            runtime.call_udf(&spec, &wasm, &[]),
            Err(ExtensionError::OutOfMemory)
        ));
        Ok(())
    }

    #[test]
    fn wasm_runtime_rejects_output_range_outside_memory() -> Result<(), Box<dyn std::error::Error>>
    {
        let runtime = WasmRuntime::new()?;
        let wasm = output_range_wasm(65_535, 2)?;
        let spec = UdfSpec::scalar("bad_ptr", super::wasm_hash(&wasm));

        assert!(matches!(
            runtime.call_udf(&spec, &wasm, &[]),
            Err(ExtensionError::BadAbi(_))
        ));
        Ok(())
    }

    #[test]
    fn wasm_runtime_stops_infinite_loop_with_fuel() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = WasmRuntime::new()?;
        let wasm = wat::parse_str(
            r#"
            (module
              (memory (export "memory") 1)
              (func (export "udf_call") (param i32 i32) (result i64)
                (loop br 0)
                (i64.const 0)))
            "#,
        )?;
        let mut spec = UdfSpec::scalar("loop", super::wasm_hash(&wasm));
        spec.budget.fuel = 1_000;
        assert!(matches!(
            runtime.call_udf(&spec, &wasm, &[]),
            Err(ExtensionError::OutOfFuel | ExtensionError::Trap(_))
        ));
        Ok(())
    }

    #[test]
    fn wasm_runtime_stops_infinite_loop_with_timeout() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = WasmRuntime::new()?;
        let wasm = wat::parse_str(
            r#"
            (module
              (memory (export "memory") 1)
              (func (export "udf_call") (param i32 i32) (result i64)
                (loop br 0)
                (i64.const 0)))
            "#,
        )?;
        let mut spec = UdfSpec::scalar("loop_timeout", super::wasm_hash(&wasm));
        spec.budget.fuel = u64::MAX / 4;
        spec.budget.timeout_ms = 5;
        assert!(matches!(
            runtime.call_udf(&spec, &wasm, &[]),
            Err(ExtensionError::Timeout)
        ));
        Ok(())
    }

    #[test]
    fn wasm_runtime_reuses_module_cache() -> Result<(), Box<dyn std::error::Error>> {
        let runtime = WasmRuntime::new()?;
        let wasm = constant_value_wasm(&Value::Int(7))?;
        let spec = UdfSpec::scalar("cached", super::wasm_hash(&wasm));
        assert_eq!(runtime.call_udf(&spec, &wasm, &[])?, Value::Int(7));
        assert_eq!(runtime.call_udf(&spec, &wasm, &[])?, Value::Int(7));
        assert_eq!(runtime.module_cache_len()?, 1);
        Ok(())
    }

    #[test]
    fn codec_registry_round_trips_builtins() -> Result<(), Box<dyn std::error::Error>> {
        let registry = CodecRegistry::with_builtins();
        for id in ["identity", "lz4", "zstd"] {
            let codec = registry
                .get(id)
                .ok_or_else(|| format!("missing codec {id}"))?;
            let encoded = codec.encode(b"hello world")?;
            assert_eq!(codec.decode(&encoded)?, b"hello world");
        }
        Ok(())
    }

    #[test]
    fn built_in_collations_create_stable_sort_keys() -> Result<(), Box<dyn std::error::Error>> {
        let registry = CollationRegistry::with_builtins();
        let case_insensitive = registry
            .get("case_insensitive")
            .ok_or_else(|| "missing collation".to_owned())?;
        assert_eq!(
            case_insensitive.sort_key("ANNA")?,
            case_insensitive.sort_key("anna")?
        );
        assert!(
            builtin_collation_key("numeric", "file2")?
                < builtin_collation_key("numeric", "file10")?
        );
        Ok(())
    }

    #[test]
    fn policies_validate_and_mask_values() -> Result<(), Box<dyn std::error::Error>> {
        let resource = Resource::Collection("users".to_owned());
        let policy = PolicyConfig {
            validations: vec![ValidationPolicy {
                resource: resource.clone(),
                required_paths: vec![FieldPath::new(["email"])],
            }],
            masking: vec![MaskingPolicy {
                resource: resource.clone(),
                paths: vec![FieldPath::new(["email"])],
                replacement: Value::Str("***".to_owned()),
            }],
            row_policies: Vec::new(),
            limits: LimitPolicy::default(),
        };
        let mut map = BTreeMap::new();
        map.insert("email".to_owned(), Value::Str("a@example.com".to_owned()));
        let value = Value::Object(map);
        policy.validate_value(&resource, &value)?;
        let masked = policy.mask_value(&resource, &value);
        assert_eq!(
            crate::model::extract_path(&masked, &FieldPath::new(["email"])),
            Some(&Value::Str("***".to_owned()))
        );
        Ok(())
    }

    #[test]
    fn extension_sql_parsers_handle_function_registration_and_calls()
    -> Result<(), Box<dyn std::error::Error>> {
        let create = parse_create_wasm_function("CREATE FUNCTION f LANGUAGE wasm AS HEX '00ff';")?
            .ok_or_else(|| "missing create".to_owned())?;
        assert_eq!(create.name, "f");
        assert_eq!(create.wasm, vec![0x00, 0xFF]);
        let call = parse_select_udf_call("SELECT f(1, 'Ada', true);")?
            .ok_or_else(|| "missing call".to_owned())?;
        assert_eq!(call.0, "f");
        assert_eq!(
            call.1,
            vec![
                Value::Int(1),
                Value::Str("Ada".to_owned()),
                Value::Bool(true)
            ]
        );
        Ok(())
    }
}
