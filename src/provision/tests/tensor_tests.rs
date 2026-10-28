use super::*;
use crate::tensor::{Tensor, TensorBatch, TensorData, TensorDatasetConfig, TensorReadRequest};

fn tensor_spec() -> Result<DatabaseSpec, Error> {
    spec(
        "balanced",
        &json!([{
            "name": "training", "role": "analytics", "domain": "primary", "indexes": ["columnar"],
            "physical": {
                "kind": "tensor_dataset",
                "tensors": [
                    {"name":"embedding","dtype":"f32","shape":[3]},
                    {"name":"tokens","dtype":"i64","shape":[null]},
                    {"name":"text","dtype":"utf8","shape":[]}
                ],
                "tensor_chunk_samples":2,
                "tensor_max_chunk_bytes":4096
            }
        }]),
    )
}

fn declared_config(spec: &DatabaseSpec) -> Result<TensorDatasetConfig, Error> {
    let physical = spec.collections[0].physical.as_ref().ok_or("physical")?;
    let mut config = TensorDatasetConfig::new("training", physical.tensors.clone());
    config.chunk_samples = physical.tensor_chunk_samples.unwrap_or(64);
    config.max_chunk_bytes = physical.tensor_max_chunk_bytes.unwrap_or(4_194_304);
    Ok(config)
}

#[test]
fn tensor_provisioning_plans_applies_exports_and_detects_schema_drift() -> TestResult {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("tensor-provision.redb");
    let spec = tensor_spec()?;
    let plan = plan_path(&spec, &path, ProvisionSurface::Cli)?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{plan:#?}");
    assert!(plan.auto_approvable);
    assert!(has_step(
        &plan,
        ProvisionStepKind::CreateTensorDataset,
        StepSafety::Additive
    ));
    assert_eq!(plan.objects[0].kind, ObjectKind::TensorDataset);
    assert!(!path.exists(), "planning created the database");
    let (mut database, applied) = provision_new_database(
        &spec,
        Some(&path),
        &Confirmation::PlanId(plan.plan_id),
        &ProvisionOptions::default(),
        &env(ProvisionSurface::Cli),
    )?;
    assert_applied(&applied);
    let expected = declared_config(&spec)?;
    assert!(
        matches!(database.catalog().get("training"), Some(CatalogEntry::TensorDataset{config}) if config == &expected)
    );
    assert_eq!(plan_open(&spec, &database)?.outcome, PlanOutcome::Noop);
    assert!(drift(&database, &spec, &ProvisionOptions::default())?.in_sync);
    assert_eq!(export_spec(&database)?.spec.collections, spec.collections);
    let reapplied = apply_cli(&mut database, &spec, &ProvisionOptions::default())?;
    assert!(matches!(
        reapplied.status,
        ProvisionOutcome::Noop | ProvisionOutcome::Applied
    ));

    for field in ["dtype", "shape", "chunk_samples", "max_chunk_bytes"] {
        let mut changed = spec.clone();
        let physical = changed.collections[0].physical.as_mut().ok_or("physical")?;
        match field {
            "dtype" => physical.tensors[0].dtype = crate::tensor::TensorDType::F64,
            "shape" => physical.tensors[0].shape = vec![Some(4)],
            "chunk_samples" => physical.tensor_chunk_samples = Some(3),
            _ => physical.tensor_max_chunk_bytes = Some(8192),
        }
        let conflict = plan_open(&changed, &database)?;
        assert_blocked(&conflict, "PROVISION_OBJECT_CONFLICT");
        assert!(!drift(&database, &changed, &ProvisionOptions::default())?.in_sync);
    }
    Ok(())
}

#[test]
fn existing_native_tensor_dataset_exports_and_is_adopted_with_exact_schema() -> TestResult {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("tensor-adopt.redb");
    let mut database = create_database(DbConfig::on_disk(Profile::Balanced, &path))?;
    let spec = tensor_spec()?;
    let config = declared_config(&spec)?;
    database.create_tensor_dataset(config.clone())?;
    let exported = export_spec(&database)?;
    assert_eq!(exported.source, ExportSource::Inferred);
    assert_eq!(exported.spec.version, 2);
    let physical = exported.spec.collections[0]
        .physical
        .as_ref()
        .ok_or("physical")?;
    assert_eq!(
        physical.kind,
        Some(crate::config_spec::PhysicalKind::TensorDataset)
    );
    assert_eq!(physical.tensors, config.tensors);
    assert_eq!(physical.tensor_chunk_samples, Some(config.chunk_samples));
    assert_eq!(
        physical.tensor_max_chunk_bytes,
        Some(config.max_chunk_bytes)
    );
    let plan = plan_open(&exported.spec, &database)?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{plan:#?}");
    assert!(has_step(
        &plan,
        ProvisionStepKind::AdoptObject,
        StepSafety::Additive
    ));
    assert!(!has_step(
        &plan,
        ProvisionStepKind::CreateTensorDataset,
        StepSafety::Additive
    ));
    assert_applied(&apply_cli(
        &mut database,
        &exported.spec,
        &ProvisionOptions::default(),
    )?);
    // This legacy database had no profile manifest. Adoption persists the
    // resolved runtime knobs; like other profile changes, they take effect on
    // reopen. Dataset schema adoption itself must introduce no other drift.
    let pending = drift(&database, &exported.spec, &ProvisionOptions::default())?;
    assert_eq!(
        pending
            .items
            .iter()
            .map(|item| item.kind)
            .collect::<Vec<_>>(),
        vec![DriftKind::SessionPendingRestart],
        "unexpected drift after native dataset adoption: {pending:#?}"
    );
    drop(database);
    let reopened = open_database(DbConfig::on_disk(Profile::Balanced, &path))?;
    let converged = drift(&reopened, &exported.spec, &ProvisionOptions::default())?;
    assert!(converged.in_sync, "{converged:#?}");
    Ok(())
}

#[test]
fn tensor_schema_validation_reuses_core_bounds_and_keeps_physical_fields_v2_only() -> TestResult {
    let good = tensor_spec()?;
    assert!(GuaranteeValidator::validate(&good).valid);
    let mut v1 = good.clone();
    v1.version = 1;
    assert!(
        GuaranteeValidator::validate(&v1)
            .issues
            .iter()
            .any(|issue| issue.code == "FIELD_REQUIRES_SPEC_V2")
    );
    for bad in [
        json!({"kind":"tensor_dataset","tensors":[{"name":"x","dtype":"f32","shape":[4_000_001]}]}),
        json!({"kind":"tensor_dataset","tensors":[{"name":"x","dtype":"f32","shape":[2000,3000]}]}),
        json!({"kind":"tensor_dataset","tensors":[{"name":"x","dtype":"f32","shape":[1]},{"name":"x","dtype":"i64","shape":[]}]}),
        json!({"kind":"tensor_dataset","tensors":[{"name":"x","dtype":"f32","shape":[null]}],"tensor_chunk_samples":0}),
        json!({"kind":"tensor_dataset","tensors":[{"name":"x","dtype":"f32","shape":[1]}],"tensor_max_chunk_bytes":1023}),
    ] {
        let mut invalid = good.clone();
        invalid.collections[0].physical = Some(serde_json::from_value(bad)?);
        let report = GuaranteeValidator::validate(&invalid);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.code == "TENSOR_PARAMS_INVALID"),
            "{report:#?}"
        );
        assert!(!report.valid);
    }
    let mut missing = good;
    missing.collections[0]
        .physical
        .as_mut()
        .ok_or("physical")?
        .tensors
        .clear();
    let plan = plan_path(
        &missing,
        Path::new("missing-tensors.redb"),
        ProvisionSurface::Cli,
    )?;
    assert_blocked(&plan, "PHYSICAL_PARAMETER_REQUIRED");
    Ok(())
}

#[test]
fn tensor_profile_infers_native_dataset_and_fails_before_file_creation_without_gpu() -> TestResult {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("gpu-required.redb");
    let mut spec = tensor_spec()?;
    spec.profile = "tensor_dataset".into();
    spec.collections[0]
        .physical
        .as_mut()
        .ok_or("physical")?
        .kind = None;
    let plan = plan_path(&spec, &path, ProvisionSurface::Cli)?;
    assert_eq!(plan.outcome, PlanOutcome::Ready, "{plan:#?}");
    assert_eq!(plan.objects[0].kind, ObjectKind::TensorDataset);
    assert!(!path.exists());
    if crate::tensor_gpu::TensorGpuEngine::shared().is_err() {
        let result = provision_new_database(
            &spec,
            Some(&path),
            &Confirmation::PlanId(plan.plan_id),
            &ProvisionOptions::default(),
            &env(ProvisionSurface::Cli),
        );
        let Err(error) = result else {
            return Err("GPU profile opened without a GPU".into());
        };
        assert!(matches!(
            error,
            ProvisionError::Config(ConfigError::Unsupported(_))
        ));
        assert!(error.to_string().to_lowercase().contains("gpu"), "{error}");
        assert!(!path.exists(), "GPU refusal created the database file");
    }
    let mut sql_kind = spec.clone();
    sql_kind.collections[0]
        .physical
        .as_mut()
        .ok_or("physical")?
        .kind = Some(crate::config_spec::PhysicalKind::Table);
    assert!(!GuaranteeValidator::validate(&sql_kind).valid);
    Ok(())
}

fn tensor_batch(value: f32, text: &str, tokens: Vec<i64>) -> TensorBatch {
    [
        (
            "embedding".into(),
            vec![Tensor {
                shape: vec![3],
                data: TensorData::F32(vec![value, 0.0, 1.0]),
            }],
        ),
        (
            "tokens".into(),
            vec![Tensor {
                shape: vec![tokens.len()],
                data: TensorData::I64(tokens),
            }],
        ),
        (
            "text".into(),
            vec![Tensor {
                shape: vec![],
                data: TensorData::Utf8(vec![text.into()]),
            }],
        ),
    ]
    .into()
}

#[test]
fn provisioned_tensor_revisions_survive_backup_restore_and_reopen() -> TestResult {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("tensor-backup.redb");
    let spec = tensor_spec()?;
    let (database, applied) = provision_new_database(
        &spec,
        Some(&path),
        &Confirmation::AutoApproveAdditive,
        &ProvisionOptions::default(),
        &env(ProvisionSurface::Cli),
    )?;
    assert_applied(&applied);
    let first = tensor_batch(1.0, "first", vec![1, 2]);
    let second = tensor_batch(2.0, "second", vec![3]);
    let historical = {
        let dataset = database.tensor_dataset("training")?;
        let first_append = dataset.append(&first)?;
        assert_eq!(first_append.revision, 1);
        let request = TensorReadRequest {
            revision: Some(1),
            ..TensorReadRequest::new(0, 10)
        };
        let before = dataset.read(&request)?;
        dataset.append(&second)?;
        assert_eq!(
            dataset.read(&request)?,
            before,
            "later appends changed the revision snapshot"
        );
        before
    };
    let backup = full_backup(
        &database,
        temp.path().join("backups"),
        &BackupConfig::default(),
    )?;
    verify_backup(&backup.path)?;
    drop(database);
    let restored_path = temp.path().join("restored.redb");
    restore_backup(
        &backup.path,
        DbConfig::on_disk(Profile::Balanced, &restored_path),
        RestoreTarget::Latest,
    )?;
    let restored = open_database(DbConfig::on_disk(Profile::Balanced, &restored_path))?;
    let dataset = restored.tensor_dataset("training")?;
    assert_eq!(dataset.info(None)?.samples, 2);
    assert_eq!(dataset.info(None)?.config, declared_config(&spec)?);
    assert_eq!(
        dataset.read(&TensorReadRequest {
            revision: Some(1),
            ..TensorReadRequest::new(0, 10)
        })?,
        historical
    );
    let latest = dataset.read(&TensorReadRequest::new(0, 10))?;
    assert_eq!(
        latest.tensors["embedding"],
        [first["embedding"].clone(), second["embedding"].clone()].concat()
    );
    assert_eq!(plan_open(&spec, &restored)?.outcome, PlanOutcome::Noop);
    Ok(())
}
