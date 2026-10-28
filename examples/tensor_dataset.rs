//! Native tensor storage, revision reads, GPU search and reductions.
//!
//! GPU: `cargo run --locked --example tensor_dataset -- --db tensor-gpu.redb`
//! Explicit CPU reference: `cargo run --locked --example tensor_dataset -- --cpu-reference --db tensor-reference.redb`

use std::{collections::BTreeMap, path::PathBuf};

use multidb::{
    db::{
        Database, DbConfig, OperationalConfig, Profile, SecurityConfig, create_database_with_ops,
    },
    security::{AuthzPolicy, Permission, Principal, PrincipalRegistry, Resource, Role},
    tensor::{
        Tensor, TensorBatch, TensorColumn, TensorComputeRequest, TensorDType, TensorData,
        TensorDatasetConfig, TensorDevice, TensorGpuOperation, TensorReadRequest,
        TensorSearchRequest,
    },
    vector::VectorMetric,
};

fn batch(rows: &[(&str, &[u8], [f32; 3])]) -> TensorBatch {
    BTreeMap::from([
        (
            "text".to_owned(),
            rows.iter()
                .map(|(text, _, _)| Tensor {
                    shape: vec![],
                    data: TensorData::Utf8(vec![(*text).to_owned()]),
                })
                .collect(),
        ),
        (
            "image".to_owned(),
            rows.iter()
                .map(|(_, image, _)| Tensor {
                    shape: vec![image.len()],
                    data: TensorData::U8(image.to_vec()),
                })
                .collect(),
        ),
        (
            "embedding".to_owned(),
            rows.iter()
                .map(|(_, _, embedding)| Tensor {
                    shape: vec![3],
                    data: TensorData::F32(embedding.to_vec()),
                })
                .collect(),
        ),
    ])
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cpu_reference = false;
    let mut path = PathBuf::from("tensor-dataset-example.redb");
    let mut arguments = std::env::args().skip(1);
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--cpu-reference" => cpu_reference = true,
            "--db" => path = arguments.next().ok_or("--db requires a path")?.into(),
            "--help" | "-h" => {
                println!("tensor_dataset [--db NEW_DATABASE_PATH] [--cpu-reference]");
                println!(
                    "Default: strict native GPU profile. --cpu-reference explicitly uses Balanced storage and CPU operations."
                );
                return Ok(());
            }
            _ => return Err(format!("unknown option {argument:?}; use --help").into()),
        }
    }
    if path.exists() {
        return Err(format!(
            "example target {} already exists; choose a fresh --db path",
            path.display()
        )
        .into());
    }
    let profile = if cpu_reference {
        Profile::Balanced
    } else {
        Profile::Tensor
    };
    let resource = Resource::Collection("training".to_owned());
    let policy = AuthzPolicy::new([Role::new("tensor_user")
        .grant(resource.clone(), Permission::Admin)
        .grant(resource.clone(), Permission::Read)
        .grant(resource, Permission::Write)]);
    let principal = Principal::new("example").with_role("tensor_user");
    // Tensor profile initialization requires a physical GPU before creating the file.
    // There is no automatic CPU retry if GPU initialization fails. Operational
    // creation also persists this example's policy and principal mapping.
    let mut database = create_database_with_ops(
        DbConfig::on_disk(profile, &path),
        OperationalConfig::new().with_security(SecurityConfig {
            authz_policy: policy,
            principals: PrincipalRegistry::new().with_principal("example", principal.clone()),
            ..SecurityConfig::default()
        }),
    )?;
    let config = TensorDatasetConfig::new(
        "training",
        vec![
            TensorColumn::new("text", TensorDType::Utf8, vec![]),
            TensorColumn::new("image", TensorDType::U8, vec![None]),
            TensorColumn::new("embedding", TensorDType::F32, vec![Some(3)]),
        ],
    );
    database.create_tensor_dataset_as(&principal, config)?;
    println!("database={} profile={profile:?}", path.display());
    run_workload(&database, &principal, cpu_reference)
}

fn run_workload(
    database: &Database,
    principal: &Principal,
    cpu_reference: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let first = database.append_tensors_as(
        principal,
        "training",
        &batch(&[
            ("first caption", &[0, 128, 255], [1.0, 0.0, 0.0]),
            ("second caption", &[10, 20], [0.0, 1.0, 0.0]),
        ]),
    )?;
    assert_eq!((first.start, first.end, first.revision), (0, 2, 1));
    assert_eq!(
        database
            .tensor_dataset_info_as(principal, "training", Some(0))?
            .samples,
        0
    );
    let selected = TensorReadRequest {
        start: 0,
        limit: 2,
        columns: Some(vec!["text".to_owned(), "embedding".to_owned()]),
        revision: Some(first.revision),
    };
    let snapshot = database.read_tensors_as(principal, "training", &selected)?;
    assert!(!snapshot.tensors.contains_key("image"));
    database.append_tensors_as(
        principal,
        "training",
        &batch(&[("later caption", &[42], [0.0, 0.0, 1.0])]),
    )?;
    assert_eq!(
        database.read_tensors_as(principal, "training", &selected)?,
        snapshot,
        "a later append changed the immutable revision"
    );
    let mut search = TensorSearchRequest::new("embedding", vec![1.0, 0.0, 0.0]);
    search.metric = VectorMetric::L2;
    search.k = 2;
    search.revision = Some(first.revision);
    let found = database.search_tensors_as(principal, "training", &search)?;
    assert_eq!(found.hits[0].index, 0);
    assert!(found.hits[0].distance.abs() < 1e-5);
    let reductions = database.compute_tensors_as(
        principal,
        "training",
        &TensorComputeRequest {
            tensor: "image".to_owned(),
            operation: TensorGpuOperation::Sum,
            start: 0,
            limit: 2,
            revision: Some(first.revision),
        },
    )?;
    assert_eq!(reductions.values, vec![383.0, 30.0]);
    let expected_device = if cpu_reference {
        TensorDevice::Cpu
    } else {
        TensorDevice::Gpu
    };
    assert_eq!(found.execution.device, expected_device);
    assert_eq!(reductions.execution.device, expected_device);
    println!(
        "snapshot revision={} samples={} selected columns={:?}",
        snapshot.revision,
        snapshot.total_samples,
        snapshot.tensors.keys().collect::<Vec<_>>()
    );
    println!("nearest={:?} execution={:?}", found.hits, found.execution);
    println!(
        "image sums={:?} execution={:?}",
        reductions.values, reductions.execution
    );
    println!(
        "current samples={}",
        database
            .tensor_dataset_info_as(principal, "training", None)?
            .samples
    );
    Ok(())
}
