//! Native tensor dataset catalog, authorization and execution boundaries.

use std::{sync::Mutex, time::Duration};

use super::{
    CatalogEntry, ConfigError, Database, DatabaseRepl, DbError, Profile, catalog_put_op,
    kind_mismatch, policy_has_row_or_masking,
};
use crate::{
    extension::{ExtensionError, PolicyConfig},
    query::QueryError,
    repl::{ConditionalBatch, Op, ReadConsistency, ReplError, Replication, propose_system_batch},
    security::{AuditEvent, AuditOutcome, Permission, Principal, Resource},
    storage::Bytes,
    tensor::{
        TensorAppendResult, TensorBatch, TensorComputeRequest, TensorComputeResult, TensorDType,
        TensorData, TensorDataset, TensorDatasetConfig, TensorDatasetInfo, TensorDeadline,
        TensorError, TensorExecution, TensorGpuOperation, TensorReadBatch, TensorReadRequest,
        TensorSearchRequest, TensorSearchResult,
    },
    tensor_gpu::TensorGpuEngine,
};

const MAX_GPU_BATCH_FLOATS: usize = 4_194_304;
const MAX_GPU_BATCH_ROWS: usize = 1_024;

impl Database {
    /// Creates a native tensor dataset and its catalog entry atomically.
    /// Tensor samples are stored in the binary tensor engine, not document/relational rows.
    /// # Errors
    /// Fails for an unsupported backend, invalid/duplicate config, or a storage failure.
    pub fn create_tensor_dataset(
        &mut self,
        config: TensorDatasetConfig,
    ) -> Result<TensorDataset<'_, dyn Replication + '_>, DbError> {
        self.ensure_tensor_backend()?;
        self.ensure_name_available(&config.name)?;
        let name = config.name.clone();
        let mut operations = TensorDataset::<dyn Replication>::create_ops(&config)?;
        let entry = CatalogEntry::TensorDataset { config };
        operations.push(catalog_put_op(&name, &entry)?);
        propose_system_batch(&self.repl, operations)?;
        self.catalog.insert(name.clone(), entry);
        self.invalidate_plan_cache();
        self.tensor_dataset(&name)
    }

    /// Creates a dataset after checking Admin on its Collection resource.
    /// # Errors
    /// Fails when permission, policies, audit or native dataset initialization fails.
    pub fn create_tensor_dataset_as(
        &mut self,
        principal: &Principal,
        config: TensorDatasetConfig,
    ) -> Result<TensorDataset<'_, dyn Replication + '_>, DbError> {
        let resource = Resource::Collection(config.name.clone());
        self.authorize_or_audit(
            principal,
            &resource,
            Permission::Admin,
            "create_tensor_dataset",
        )?;
        self.ensure_tensor_backend()?;
        self.tensor_policy(&resource)?;
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "create_tensor_dataset",
            resource,
            AuditOutcome::Allowed,
            None,
        ))?;
        self.create_tensor_dataset(config)
    }

    /// Opens a trusted native dataset handle using the common user write pipeline.
    /// # Errors
    /// Fails when the backend or named catalog object is unsupported or corrupt.
    pub fn tensor_dataset(
        &self,
        name: &str,
    ) -> Result<TensorDataset<'_, dyn Replication + '_>, DbError> {
        self.ensure_tensor_backend()?;
        match self.catalog.get(name) {
            Some(CatalogEntry::TensorDataset { config }) => {
                let repl: &dyn Replication = self;
                Ok(TensorDataset::new(repl, config.clone())?
                    .with_gpu_required(self.profile() == Profile::Tensor)
                    .with_execution_timeout(Duration::from_millis(
                        self.performance.query.timeout_ms,
                    ))
                    .with_memory_limit(self.performance.query.memory_limit_bytes))
            }
            Some(other) => Err(kind_mismatch(name, "tensor dataset", other)),
            None => Err(DbError::MissingCatalogObject(name.to_owned())),
        }
    }

    /// Reads bounded dataset metadata or one immutable revision after checking Read.
    /// # Errors
    /// Fails when permissions/policies, the dataset or the revision reject the request.
    pub fn tensor_dataset_info_as(
        &self,
        principal: &Principal,
        name: &str,
        revision: Option<u64>,
    ) -> Result<TensorDatasetInfo, DbError> {
        self.authorize_tensor(principal, name, Permission::Read, "tensor_dataset_info")?;
        Ok(self.tensor_dataset(name)?.info(revision)?)
    }

    /// Appends a typed binary batch after checking Write and current policies/limits.
    /// # Errors
    /// Fails when authorization, binary batch limits, validation or commit fails.
    pub fn append_tensors_as(
        &self,
        principal: &Principal,
        name: &str,
        batch: &TensorBatch,
    ) -> Result<TensorAppendResult, DbError> {
        let policy = self.authorize_tensor(principal, name, Permission::Write, "append_tensors")?;
        let config = self.tensor_dataset(name)?.config().clone();
        self.record_audit(&AuditEvent::new(
            Some(principal),
            "append_tensors",
            Resource::Collection(name.to_owned()),
            AuditOutcome::Allowed,
            None,
        ))?;
        let guarded = TensorPolicyReplication {
            database: self,
            policy: &policy,
            violation: Mutex::new(None),
        };
        let result = TensorDataset::new(&guarded, config)?
            .with_gpu_required(self.profile() == Profile::Tensor)
            .append(batch);
        if let Some(message) = guarded
            .violation
            .lock()
            .map_err(|_| ExtensionError::PolicyDenied("tensor policy lock poisoned".to_owned()))?
            .take()
        {
            return Err(ExtensionError::PolicyDenied(message).into());
        }
        Ok(result?)
    }

    /// Reads a bounded selection of tensors after checking Read and acquiring a query slot.
    /// # Errors
    /// Fails when permissions/policies, the shared governor or tensor read rejects it.
    pub fn read_tensors_as(
        &self,
        principal: &Principal,
        name: &str,
        request: &TensorReadRequest,
    ) -> Result<TensorReadBatch, DbError> {
        self.authorize_tensor(principal, name, Permission::Read, "read_tensors")?;
        let _permit = self.try_tensor_permit()?;
        Ok(self.tensor_dataset(name)?.read(request)?)
    }

    /// Searches a numeric tensor; Tensor-profile databases execute distances on a physical GPU.
    /// Other local profiles expose the explicit CPU reference implementation.
    /// # Errors
    /// Fails when authorization, bounded execution, dtype or the selected device rejects it.
    pub fn search_tensors_as(
        &self,
        principal: &Principal,
        name: &str,
        request: &TensorSearchRequest,
    ) -> Result<TensorSearchResult, DbError> {
        self.authorize_tensor(principal, name, Permission::Read, "search_tensors")?;
        let _permit = self.try_tensor_permit()?;
        let dataset = self.tensor_dataset(name)?;
        Ok(dataset.search(request)?)
    }

    /// Reduces F32/U8 sample tensors, using a physical GPU for Tensor-profile databases.
    /// # Errors
    /// Fails when authorization, limits, dtype, snapshot or device execution rejects it.
    pub fn compute_tensors_as(
        &self,
        principal: &Principal,
        name: &str,
        request: &TensorComputeRequest,
    ) -> Result<TensorComputeResult, DbError> {
        self.authorize_tensor(principal, name, Permission::Read, "compute_tensors")?;
        let _permit = self.try_tensor_permit()?;
        let dataset = self.tensor_dataset(name)?;
        Ok(compute_tensor_rows(
            &dataset,
            request,
            self.profile() == Profile::Tensor,
        )?)
    }

    fn ensure_tensor_backend(&self) -> Result<(), DbError> {
        if !matches!(self.repl, DatabaseRepl::SingleNode(_))
            || !self.replication_ref().supports_bounded_scans()
        {
            return Err(ConfigError::Unsupported("native tensor datasets require a local single-node backend; AP, sharding and live CP are outside this preview".to_owned()).into());
        }
        Ok(())
    }

    fn tensor_policy(&self, resource: &Resource) -> Result<PolicyConfig, DbError> {
        let policy = self.policy_config()?;
        if policy_has_row_or_masking(&policy, resource)
            || policy
                .validations
                .iter()
                .any(|rule| &rule.resource == resource)
        {
            return Err(ExtensionError::PolicyDenied("native tensor datasets do not support Value row, masking or required-path policies".to_owned()).into());
        }
        Ok(policy)
    }

    fn authorize_tensor(
        &self,
        principal: &Principal,
        name: &str,
        permission: Permission,
        action: &'static str,
    ) -> Result<PolicyConfig, DbError> {
        let resource = Resource::Collection(name.to_owned());
        self.authorize_or_audit(principal, &resource, permission, action)?;
        self.ensure_tensor_backend()?;
        self.tensor_policy(&resource)
    }

    fn try_tensor_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, QueryError> {
        self.query_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| QueryError::Busy("tensor concurrent query limit reached".to_owned()))
    }
}

/// Validates the actual encoded write batch, while retaining Database's hooks and quotas.
struct TensorPolicyReplication<'a> {
    database: &'a Database,
    policy: &'a PolicyConfig,
    violation: Mutex<Option<String>>,
}

impl TensorPolicyReplication<'_> {
    fn check(&self, operations: &[Op]) -> Result<(), ReplError> {
        if let Err(error) = self.policy.validate_batch(operations) {
            let message = error.to_string();
            if let Ok(mut violation) = self.violation.lock() {
                *violation = Some(message.clone());
            }
            return Err(ReplError::Rejected(message));
        }
        Ok(())
    }
}

impl Replication for TensorPolicyReplication<'_> {
    fn supports_bounded_scans(&self) -> bool {
        self.database.supports_bounded_scans()
    }
    fn supports_commit_log(&self) -> bool {
        self.database.supports_commit_log()
    }
    fn propose(&self, operation: Op) -> Result<(), ReplError> {
        self.propose_batch(vec![operation])
    }
    fn propose_batch(&self, operations: Vec<Op>) -> Result<(), ReplError> {
        self.check(&operations)?;
        self.database.propose_batch(operations)
    }
    fn propose_authorized_batch(
        &self,
        operations: Vec<Op>,
        authorization: crate::txn::WriteAuthorization,
    ) -> Result<(), ReplError> {
        self.check(&operations)?;
        self.database
            .propose_authorized_batch(operations, authorization)
    }
    fn propose_conditional_batch(&self, batch: ConditionalBatch) -> Result<(), ReplError> {
        self.check(&batch.ops)?;
        self.database.propose_conditional_batch(batch)
    }

    fn propose_tensor_batch(
        &self,
        batch: ConditionalBatch,
        authorization: crate::tensor::TensorWriteAuthorization,
    ) -> Result<(), ReplError> {
        self.check(&batch.ops)?;
        self.database.propose_tensor_batch(batch, authorization)
    }
    fn read(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Option<Bytes>, ReplError> {
        self.database.read(table, key, consistency)
    }
    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        self.database.range(table, start, end, consistency)
    }
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn scan_range_batches(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
        batch_rows: usize,
        cancelled: &dyn Fn() -> bool,
        on_batch: &mut dyn FnMut(&[(Bytes, Bytes)]) -> Result<bool, ReplError>,
    ) -> Result<(), ReplError> {
        self.database.scan_range_batches(
            table,
            start,
            end,
            consistency,
            batch_rows,
            cancelled,
            on_batch,
        )
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "Keep the bounded read, flush and reduction transaction together so all retained buffers share one budget."
)]
fn compute_tensor_rows(
    dataset: &TensorDataset<'_, dyn Replication + '_>,
    request: &TensorComputeRequest,
    gpu: bool,
) -> Result<TensorComputeResult, TensorError> {
    let deadline = dataset.execution_deadline()?;
    request.validate()?;
    let column = dataset
        .config()
        .tensors
        .iter()
        .find(|column| column.name == request.tensor)
        .ok_or_else(|| TensorError::InvalidValue("unknown compute tensor".to_owned()))?;
    if !matches!(column.dtype, TensorDType::F32 | TensorDType::U8) {
        return Err(TensorError::GpuUnsupported(
            "tensor reductions currently support F32 and U8 tensors".to_owned(),
        ));
    }
    let info = dataset.info(request.revision)?;
    let end = execution_end(request.start, request.limit, info.samples)?;
    let engine = if gpu {
        Some(TensorGpuEngine::shared()?)
    } else {
        None
    };
    let mut values = Vec::new();
    // Ragged rows can be large. Read each row at the captured revision, and batch only
    // reductions whose aggregate GPU input stays inside the storage-buffer budget.
    let mut rows = Vec::new();
    let mut floats = 0_usize;
    let max_floats = (dataset.memory_limit() / if gpu { 16 } else { 8 }).min(MAX_GPU_BATCH_FLOATS);
    for index in request.start..end {
        deadline.check()?;
        let read = TensorReadRequest {
            start: index,
            limit: 1,
            columns: Some(vec![request.tensor.clone()]),
            revision: Some(info.revision),
        };
        let retained = floats
            .saturating_mul(std::mem::size_of::<f32>())
            .saturating_add(
                rows.capacity()
                    .saturating_mul(std::mem::size_of::<Vec<f32>>()),
            )
            .saturating_add(values.capacity().saturating_mul(std::mem::size_of::<f32>()));
        let available = dataset.memory_limit().saturating_sub(retained);
        let read_result = dataset.read_with_budget(&read, deadline, available);
        let mut batch = match read_result {
            Err(TensorError::InvalidValue(_)) if !rows.is_empty() => {
                // A large ragged successor may fit by itself. Flush the
                // previous GPU/CPU reduction batch and retry the bounded read.
                reduce_tensor_rows(
                    &rows,
                    request.operation,
                    engine.as_deref(),
                    &mut values,
                    deadline,
                )?;
                rows = Vec::new();
                floats = 0;
                dataset.read_with_budget(
                    &read,
                    deadline,
                    dataset.memory_limit().saturating_sub(
                        values.capacity().saturating_mul(std::mem::size_of::<f32>()),
                    ),
                )?
            }
            result => result?,
        };
        let tensor = batch
            .tensors
            .remove(&request.tensor)
            .and_then(|mut tensors| tensors.pop())
            .ok_or_else(|| {
                TensorError::Corruption("compute tensor missing from selected row".to_owned())
            })?;
        let row_elements = match &tensor.data {
            TensorData::F32(values) => values.len(),
            TensorData::U8(values) => values.len(),
            _ => {
                return Err(TensorError::Corruption(
                    "compute tensor has another dtype".to_owned(),
                ));
            }
        };
        if row_elements > max_floats {
            return Err(TensorError::GpuUnsupported(
                "one tensor exceeds the configured reduction buffer budget".to_owned(),
            ));
        }
        if rows.len() >= MAX_GPU_BATCH_ROWS || floats.saturating_add(row_elements) > max_floats {
            reduce_tensor_rows(
                &rows,
                request.operation,
                engine.as_deref(),
                &mut values,
                deadline,
            )?;
            rows.clear();
            floats = 0;
        }
        let row = match tensor.data {
            TensorData::F32(values) => values,
            TensorData::U8(values) => values.into_iter().map(f32::from).collect(),
            _ => {
                return Err(TensorError::Corruption(
                    "compute tensor has another dtype".to_owned(),
                ));
            }
        };
        floats += row.len();
        rows.push(row);
    }
    reduce_tensor_rows(
        &rows,
        request.operation,
        engine.as_deref(),
        &mut values,
        deadline,
    )?;
    deadline.check()?;
    Ok(TensorComputeResult {
        start: request.start,
        end,
        revision: info.revision,
        values,
        execution: engine.map_or_else(TensorExecution::default, |engine| engine.execution()),
    })
}

fn reduce_tensor_rows(
    rows: &[Vec<f32>],
    operation: TensorGpuOperation,
    engine: Option<&TensorGpuEngine>,
    values: &mut Vec<f32>,
    deadline: TensorDeadline,
) -> Result<(), TensorError> {
    deadline.check()?;
    if rows.is_empty() {
        return Ok(());
    }
    if let Some(engine) = engine {
        values.extend(engine.reduce_rows_until(rows, operation, deadline)?);
        return Ok(());
    }
    for row in rows {
        deadline.check()?;
        let mut total = 0.0;
        for chunk in row.chunks(4096) {
            deadline.check()?;
            total += chunk
                .iter()
                .map(|value| {
                    let value = f64::from(*value);
                    if operation == TensorGpuOperation::L2Norm {
                        value * value
                    } else {
                        value
                    }
                })
                .sum::<f64>();
        }
        let result = match operation {
            TensorGpuOperation::Sum => total,
            TensorGpuOperation::Mean => {
                if row.is_empty() {
                    return Err(TensorError::InvalidValue(
                        "mean of an empty tensor is undefined".to_owned(),
                    ));
                }
                #[allow(clippy::cast_precision_loss)]
                let length = row.len() as f64;
                total / length
            }
            TensorGpuOperation::L2Norm => total.sqrt(),
        };
        values.push(checked_f32(result)?);
    }
    deadline.check()?;
    Ok(())
}

fn execution_end(start: u64, limit: usize, samples: u64) -> Result<u64, TensorError> {
    if start > samples {
        return Err(TensorError::InvalidValue(
            "tensor range starts beyond snapshot sample count".to_owned(),
        ));
    }
    let count =
        u64::try_from(limit).map_err(|error| TensorError::InvalidValue(error.to_string()))?;
    Ok(start.saturating_add(count).min(samples))
}

#[allow(clippy::cast_possible_truncation)]
fn checked_f32(value: f64) -> Result<f32, TensorError> {
    let converted = value as f32;
    if !value.is_finite() || !converted.is_finite() {
        return Err(TensorError::InvalidValue(
            "tensor value is not a finite representable F32".to_owned(),
        ));
    }
    Ok(converted)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{checked_f32, compute_tensor_rows};
    use crate::{
        backup::{BackupConfig, RestoreTarget, full_backup, restore_backup},
        db::{
            ConfigError, Database, DbConfig, DbError, Profile, ReplicationKind, create_ap_database,
            create_database, open_database,
        },
        extension::{ExtensionError, MaskingPolicy, PolicyConfig, RowPolicy, ValidationPolicy},
        model::{FieldPath, Value},
        query::QueryError,
        repl::{ApClusterConfig, ReadConsistency, Replication},
        security::{AuthzPolicy, Permission, Principal, Resource, Role},
        tensor::{
            TENSOR_META_TABLE, Tensor, TensorBatch, TensorColumn, TensorComputeRequest,
            TensorDType, TensorData, TensorDatasetConfig, TensorDevice, TensorError,
            TensorGpuOperation, TensorReadRequest, TensorSearchRequest,
        },
        tensor_gpu::TensorGpuEngine,
        vector::VectorMetric,
    };

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn schema(name: &str) -> TensorDatasetConfig {
        let mut config = TensorDatasetConfig::new(
            name,
            vec![
                TensorColumn::new("embedding", TensorDType::F32, vec![Some(2)]),
                TensorColumn::new("tokens", TensorDType::U8, vec![None]),
            ],
        );
        config.chunk_samples = 2;
        config
    }

    fn rows() -> TensorBatch {
        BTreeMap::from([
            (
                "embedding".to_owned(),
                vec![
                    Tensor {
                        shape: vec![2],
                        data: TensorData::F32(vec![1.0, 0.0]),
                    },
                    Tensor {
                        shape: vec![2],
                        data: TensorData::F32(vec![0.0, 1.0]),
                    },
                    Tensor {
                        shape: vec![2],
                        data: TensorData::F32(vec![1.0, 1.0]),
                    },
                ],
            ),
            (
                "tokens".to_owned(),
                vec![
                    Tensor {
                        shape: vec![3],
                        data: TensorData::U8(vec![1, 2, 3]),
                    },
                    Tensor {
                        shape: vec![1],
                        data: TensorData::U8(vec![4]),
                    },
                    Tensor {
                        shape: vec![2],
                        data: TensorData::U8(vec![5, 6]),
                    },
                ],
            ),
        ])
    }

    fn authz() -> AuthzPolicy {
        AuthzPolicy::new([
            Role::new("reader").grant(Resource::Collection("samples".to_owned()), Permission::Read),
            Role::new("writer").grant(
                Resource::Collection("samples".to_owned()),
                Permission::Write,
            ),
            Role::new("creator").grant(
                Resource::Collection("created".to_owned()),
                Permission::Admin,
            ),
        ])
    }

    fn alice() -> Principal {
        Principal::new("alice")
            .with_role("reader")
            .with_role("writer")
    }

    fn compute(operation: TensorGpuOperation) -> TensorComputeRequest {
        TensorComputeRequest {
            tensor: "tokens".to_owned(),
            operation,
            start: 0,
            limit: 100,
            revision: None,
        }
    }

    fn fixture() -> Result<Database, Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database
            .create_tensor_dataset(schema("samples"))?
            .append(&rows())?;
        database.set_authz_policy(authz());
        Ok(database)
    }

    #[test]
    fn tensor_reads_searches_and_reductions_inherit_the_database_query_deadline() -> TestResult {
        let mut database = fixture()?;
        database.performance.query.timeout_ms = 0;
        assert!(matches!(
            database.read_tensors_as(&alice(), "samples", &TensorReadRequest::default()),
            Err(DbError::Tensor(TensorError::Timeout(_)))
        ));
        assert!(matches!(
            database.search_tensors_as(
                &alice(),
                "samples",
                &TensorSearchRequest::new("embedding", vec![1.0, 0.0])
            ),
            Err(DbError::Tensor(TensorError::Timeout(_)))
        ));
        assert!(matches!(
            database.compute_tensors_as(&alice(), "samples", &compute(TensorGpuOperation::Sum)),
            Err(DbError::Tensor(TensorError::Timeout(_)))
        ));
        // Reusing the same dataset after a completed operation starts a fresh budget.
        database.performance.query.timeout_ms = 1_000;
        assert_eq!(
            database
                .read_tensors_as(&alice(), "samples", &TensorReadRequest::new(0, 1))?
                .end,
            1
        );
        Ok(())
    }

    #[test]
    fn typed_dataset_roundtrips_ragged_columns_and_reports_explicit_cpu_execution() -> TestResult {
        let database = fixture()?;
        let page =
            database.read_tensors_as(&alice(), "samples", &TensorReadRequest::new(0, 100))?;
        assert_eq!(page.tensors, rows());
        assert_eq!(page.end, 3);
        let mut search = TensorSearchRequest::new("embedding", vec![1.0, 0.0]);
        search.k = 2;
        search.metric = VectorMetric::Cosine;
        let result = database.search_tensors_as(&alice(), "samples", &search)?;
        assert_eq!(
            result.hits.iter().map(|hit| hit.index).collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert_eq!(result.execution.device, TensorDevice::Cpu);
        let sum =
            database.compute_tensors_as(&alice(), "samples", &compute(TensorGpuOperation::Sum))?;
        assert_eq!(sum.values, vec![6.0, 4.0, 11.0]);
        assert_eq!(sum.execution.device, TensorDevice::Cpu);
        let mean =
            database.compute_tensors_as(&alice(), "samples", &compute(TensorGpuOperation::Mean))?;
        assert_eq!(mean.values, vec![2.0, 4.0, 5.5]);
        let norms = database.compute_tensors_as(
            &alice(),
            "samples",
            &compute(TensorGpuOperation::L2Norm),
        )?;
        assert!((norms.values[0] - 14.0_f32.sqrt()).abs() < 0.00001);
        Ok(())
    }

    #[test]
    fn tensor_revisions_remain_immutable_for_read_search_and_compute() -> TestResult {
        let database = fixture()?;
        let first = database.tensor_dataset_info_as(&alice(), "samples", None)?;
        database.append_tensors_as(&alice(), "samples", &rows())?;
        let historical = database.read_tensors_as(
            &alice(),
            "samples",
            &TensorReadRequest {
                revision: Some(first.revision),
                ..TensorReadRequest::new(0, 100)
            },
        )?;
        assert_eq!(historical.total_samples, 3);
        assert_eq!(historical.tensors, rows());
        assert_eq!(
            database
                .tensor_dataset_info_as(&alice(), "samples", None)?
                .samples,
            6
        );
        let mut search = TensorSearchRequest::new("embedding", vec![1.0, 0.0]);
        search.revision = Some(first.revision);
        assert_eq!(
            database
                .search_tensors_as(&alice(), "samples", &search)?
                .total_samples,
            3
        );
        let mut request = compute(TensorGpuOperation::Sum);
        request.revision = Some(first.revision);
        let result = database.compute_tensors_as(&alice(), "samples", &request)?;
        assert_eq!(result.values, vec![6.0, 4.0, 11.0]);
        assert_eq!(result.revision, first.revision);
        Ok(())
    }

    #[test]
    fn tensor_grants_are_checked_before_catalog_lookup_and_writes() -> TestResult {
        let mut database = fixture()?;
        let nobody = Principal::new("nobody");
        assert!(matches!(
            database.tensor_dataset_info_as(&nobody, "missing", None),
            Err(DbError::AuthzDenied(_))
        ));
        assert!(matches!(
            database.read_tensors_as(&nobody, "samples", &TensorReadRequest::new(0, 1)),
            Err(DbError::AuthzDenied(_))
        ));
        assert!(matches!(
            database.search_tensors_as(
                &nobody,
                "samples",
                &TensorSearchRequest::new("embedding", vec![1.0, 0.0])
            ),
            Err(DbError::AuthzDenied(_))
        ));
        assert!(matches!(
            database.compute_tensors_as(&nobody, "samples", &compute(TensorGpuOperation::Sum)),
            Err(DbError::AuthzDenied(_))
        ));
        assert!(matches!(
            database.append_tensors_as(
                &Principal::new("reader").with_role("reader"),
                "samples",
                &rows()
            ),
            Err(DbError::AuthzDenied(_))
        ));
        assert!(matches!(
            database.create_tensor_dataset_as(&nobody, schema("created")),
            Err(DbError::AuthzDenied(_))
        ));
        assert!(!database.catalog().contains_key("created"));
        database.create_tensor_dataset_as(
            &Principal::new("creator").with_role("creator"),
            schema("created"),
        )?;
        assert!(database.catalog().contains_key("created"));
        assert_eq!(database.tensor_dataset("samples")?.info(None)?.samples, 3);
        Ok(())
    }

    #[test]
    fn tensor_value_policies_fail_closed_in_every_principal_surface() -> TestResult {
        let database = fixture()?;
        let resource = Resource::Collection("samples".to_owned());
        for policy in [
            PolicyConfig {
                row_policies: vec![RowPolicy {
                    resource: resource.clone(),
                    path: FieldPath::new(["tenant"]),
                    equals: Value::Str("alice".to_owned()),
                }],
                ..PolicyConfig::default()
            },
            PolicyConfig {
                masking: vec![MaskingPolicy {
                    resource: resource.clone(),
                    paths: vec![FieldPath::new(["embedding"])],
                    replacement: Value::Null,
                }],
                ..PolicyConfig::default()
            },
            PolicyConfig {
                validations: vec![ValidationPolicy {
                    resource: resource.clone(),
                    required_paths: vec![FieldPath::new(["required"])],
                }],
                ..PolicyConfig::default()
            },
        ] {
            database.set_policy_config(&policy)?;
            assert!(matches!(
                database.tensor_dataset_info_as(&alice(), "samples", None),
                Err(DbError::Extension(ExtensionError::PolicyDenied(_)))
            ));
            assert!(
                database
                    .read_tensors_as(&alice(), "samples", &TensorReadRequest::new(0, 1))
                    .is_err()
            );
            assert!(
                database
                    .append_tensors_as(&alice(), "samples", &rows())
                    .is_err()
            );
            assert!(
                database
                    .search_tensors_as(
                        &alice(),
                        "samples",
                        &TensorSearchRequest::new("embedding", vec![1.0, 0.0])
                    )
                    .is_err()
            );
            assert!(
                database
                    .compute_tensors_as(&alice(), "samples", &compute(TensorGpuOperation::Sum))
                    .is_err()
            );
            assert_eq!(database.tensor_dataset("samples")?.info(None)?.samples, 3);
        }
        Ok(())
    }

    #[test]
    fn invalid_schema_and_encoded_policy_limits_leave_no_partial_dataset_or_append() -> TestResult {
        let mut database = fixture()?;
        let invalid = TensorDatasetConfig::new("invalid", Vec::new());
        assert!(database.create_tensor_dataset(invalid).is_err());
        assert!(!database.catalog().contains_key("invalid"));
        assert_eq!(
            database.read(TENSOR_META_TABLE, b"invalid", ReadConsistency::Strong)?,
            None
        );
        let before = database.tensor_dataset("samples")?.info(None)?;
        let mut policy = PolicyConfig::default();
        policy.limits.max_value_bytes = 1;
        database.set_policy_config(&policy)?;
        assert!(matches!(
            database.append_tensors_as(&alice(), "samples", &rows()),
            Err(DbError::Extension(ExtensionError::PolicyDenied(_)))
        ));
        assert_eq!(database.tensor_dataset("samples")?.info(None)?, before);
        assert_eq!(
            database
                .tensor_dataset("samples")?
                .read(&TensorReadRequest::new(0, 100))?
                .tensors,
            rows()
        );
        Ok(())
    }

    #[test]
    fn malformed_append_does_not_advance_revision_or_publish_samples() -> TestResult {
        let database = fixture()?;
        let before = database.tensor_dataset("samples")?.info(None)?;
        let mut invalid = rows();
        invalid
            .get_mut("embedding")
            .ok_or("missing fixture column")?[0]
            .shape = vec![3];
        assert!(
            database
                .append_tensors_as(&alice(), "samples", &invalid)
                .is_err()
        );
        assert_eq!(database.tensor_dataset("samples")?.info(None)?, before);
        Ok(())
    }

    #[test]
    fn tensor_read_search_compute_share_governor_and_release_slots_on_errors() -> TestResult {
        let mut database = fixture()?;
        database.set_query_governor(1, 100)?;
        let permit = database.query_permits.clone().try_acquire_owned()?;
        assert!(matches!(
            database.read_tensors_as(&alice(), "samples", &TensorReadRequest::new(0, 1)),
            Err(DbError::Query(QueryError::Busy(_)))
        ));
        assert!(matches!(
            database.search_tensors_as(
                &alice(),
                "samples",
                &TensorSearchRequest::new("embedding", vec![1.0, 0.0])
            ),
            Err(DbError::Query(QueryError::Busy(_)))
        ));
        assert!(matches!(
            database.compute_tensors_as(&alice(), "samples", &compute(TensorGpuOperation::Sum)),
            Err(DbError::Query(QueryError::Busy(_)))
        ));
        drop(permit);
        assert!(
            database
                .read_tensors_as(&alice(), "samples", &TensorReadRequest::new(0, 0))
                .is_err()
        );
        assert_eq!(database.query_permits.available_permits(), 1);
        database.compute_tensors_as(&alice(), "samples", &compute(TensorGpuOperation::Sum))?;
        assert_eq!(database.query_permits.available_permits(), 1);
        Ok(())
    }

    #[test]
    fn tensor_single_node_boundary_rejects_ap_without_creating_metadata() -> TestResult {
        let mut database = create_ap_database(
            DbConfig::new(Profile::InMemory).with_replication(ReplicationKind::Ap),
            ApClusterConfig::single_node_for_tests(1),
        )?;
        assert!(matches!(
            database.create_tensor_dataset(schema("samples")),
            Err(DbError::Config(ConfigError::Unsupported(_)))
        ));
        assert!(!database.catalog().contains_key("samples"));
        Ok(())
    }

    #[test]
    fn binary_dataset_and_revision_snapshots_survive_reopen_and_backup_restore() -> TestResult {
        let temp = tempfile::tempdir()?;
        let source = temp.path().join("source.redb");
        {
            let mut database = create_database(DbConfig::on_disk(Profile::Balanced, &source))?;
            let dataset = database.create_tensor_dataset(schema("samples"))?;
            dataset.append(&rows())?;
            dataset.append(&rows())?;
        }
        let database = open_database(DbConfig::on_disk(Profile::Balanced, &source))?;
        assert_eq!(
            database.tensor_dataset("samples")?.info(Some(1))?.samples,
            3
        );
        let snapshot = database
            .tensor_dataset("samples")?
            .read(&TensorReadRequest {
                revision: Some(1),
                ..TensorReadRequest::new(0, 100)
            })?;
        assert_eq!(snapshot.tensors, rows());
        let backup = full_backup(
            &database,
            temp.path().join("backups"),
            &BackupConfig::default(),
        )?;
        let target = temp.path().join("restored.redb");
        restore_backup(
            &backup.path,
            DbConfig::on_disk(Profile::Balanced, &target),
            RestoreTarget::Latest,
        )?;
        let restored = open_database(DbConfig::on_disk(Profile::Balanced, &target))?;
        assert_eq!(restored.tensor_dataset("samples")?.info(None)?.samples, 6);
        assert_eq!(
            restored
                .tensor_dataset("samples")?
                .read(&TensorReadRequest {
                    revision: Some(1),
                    ..TensorReadRequest::new(0, 100)
                })?
                .tensors,
            rows()
        );
        Ok(())
    }

    #[test]
    fn f32_gpu_conversion_rejects_nonfinite_and_overflowing_values() {
        for value in [f64::NAN, f64::INFINITY, f64::MAX, -f64::MAX] {
            assert!(checked_f32(value).is_err());
        }
        assert_eq!(checked_f32(1.5).ok(), Some(1.5));
    }

    #[test]
    fn tensor_write_boundary_rejects_raw_bytes_and_keeps_typed_append_policy_checks() -> TestResult
    {
        use crate::{
            repl::{ConditionalBatch, Op, ReplError},
            tensor::{TENSOR_CHUNKS_TABLE, TENSOR_REVISIONS_TABLE, TENSOR_SAMPLE_INDEX_TABLE},
        };
        let mut database = fixture()?;
        // Exercise the storage write boundary independently of adapter initialization.
        // Production constructors perform the separate strict hardware check first.
        database.config.profile = Profile::Tensor;
        for table in [
            TENSOR_CHUNKS_TABLE,
            TENSOR_META_TABLE,
            TENSOR_SAMPLE_INDEX_TABLE,
            TENSOR_REVISIONS_TABLE,
        ] {
            let operation = Op::Put {
                table: table.to_owned(),
                key: b"forged".to_vec(),
                value: br#"{"arbitrary":"JSON document"}"#.to_vec(),
            };
            assert!(matches!(
                database.propose(operation.clone()),
                Err(ReplError::Unsupported(_))
            ));
            assert!(matches!(
                database
                    .propose_conditional_batch(ConditionalBatch::new(Vec::new(), vec![operation])),
                Err(ReplError::Unsupported(_))
            ));
            assert_eq!(
                database.read(table, b"forged", ReadConsistency::Strong)?,
                None
            );
        }
        let (key, value) = database
            .range(TENSOR_CHUNKS_TABLE, &[], &[], ReadConsistency::Strong)?
            .into_iter()
            .next()
            .ok_or("missing fixture chunk")?;
        // Even a copied valid binary frame is not authority to overwrite immutable data.
        assert!(matches!(
            database.propose_batch(vec![Op::Put {
                table: TENSOR_CHUNKS_TABLE.to_owned(),
                key,
                value,
            }]),
            Err(ReplError::Unsupported(_))
        ));
        assert!(matches!(
            database.propose(Op::Delete {
                table: TENSOR_META_TABLE.to_owned(),
                key: b"samples".to_vec(),
            }),
            Err(ReplError::Unsupported(_))
        ));
        let appended = database.append_tensors_as(&alice(), "samples", &rows())?;
        assert_eq!(appended.end, 6);
        assert_eq!(database.tensor_dataset("samples")?.info(None)?.samples, 6);
        // The new capability goes through the authenticated user's existing limit wrapper.
        let mut policy = database.policy_config()?;
        policy.limits.max_batch_ops = 1;
        database.set_policy_config(&policy)?;
        assert!(
            database
                .append_tensors_as(&alice(), "samples", &rows())
                .is_err()
        );
        assert_eq!(database.tensor_dataset("samples")?.info(None)?.samples, 6);
        Ok(())
    }

    #[test]
    fn tensor_profile_has_no_cpu_fallback_or_file_creation_without_gpu() -> TestResult {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("gpu.redb");
        match TensorGpuEngine::shared() {
            Err(TensorError::GpuUnavailable(_)) => {
                assert!(matches!(
                    create_database(DbConfig::on_disk(Profile::Tensor, &path)),
                    Err(ConfigError::Unsupported(message)) if message.contains("GPU")
                ));
                assert!(!path.exists());
            }
            Err(error) => return Err(error.into()),
            Ok(_) => {
                let mut database = create_database(DbConfig::on_disk(Profile::Tensor, &path))?;
                database
                    .create_tensor_dataset(schema("samples"))?
                    .append(&rows())?;
                let result = database
                    .tensor_dataset("samples")?
                    .search(&TensorSearchRequest::new("embedding", vec![1.0, 0.0]))?;
                assert_eq!(result.execution.device, TensorDevice::Gpu);
                let result = compute_tensor_rows(
                    &database.tensor_dataset("samples")?,
                    &compute(TensorGpuOperation::Sum),
                    true,
                )?;
                assert_eq!(result.execution.device, TensorDevice::Gpu);
                assert_eq!(result.values, vec![6.0, 4.0, 11.0]);
            }
        }
        Ok(())
    }
}
