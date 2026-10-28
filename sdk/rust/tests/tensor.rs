use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use multidb_client::{
    ControlPlaneClient, ControlPlaneError, HttpRequest, HttpResponse, Tensor, TensorBatch,
    TensorColumn, TensorComputeRequest, TensorDType, TensorData, TensorDatasetConfig, TensorDevice,
    TensorGpuOperation, TensorMetric, TensorReadRequest, TensorSearchRequest, Transport,
};
use serde_json::{Value, json};

fn config() -> TensorDatasetConfig {
    TensorDatasetConfig::new(
        "training",
        vec![
            TensorColumn {
                name: "embedding".into(),
                dtype: TensorDType::F32,
                shape: vec![Some(2)],
            },
            TensorColumn {
                name: "image".into(),
                dtype: TensorDType::U8,
                shape: vec![None],
            },
            TensorColumn {
                name: "text".into(),
                dtype: TensorDType::Utf8,
                shape: vec![],
            },
        ],
    )
}

fn batch() -> TensorBatch {
    [
        (
            "embedding".into(),
            vec![Tensor {
                shape: vec![2],
                data: TensorData::F32(vec![1.0, 0.0]),
            }],
        ),
        (
            "image".into(),
            vec![Tensor {
                shape: vec![3],
                data: TensorData::U8(vec![0, 128, 255]),
            }],
        ),
        (
            "text".into(),
            vec![Tensor {
                shape: vec![],
                data: TensorData::Utf8(vec!["caption".into()]),
            }],
        ),
    ]
    .into()
}

#[test]
fn all_tensor_dtypes_round_trip_as_externally_tagged_arrays() {
    for (data, expected) in [
        (TensorData::U8(vec![0, 128, 255]), json!({"u8":[0,128,255]})),
        (
            TensorData::I64(vec![i64::MIN, i64::MAX]),
            json!({"i64":[i64::MIN,i64::MAX]}),
        ),
        (TensorData::F32(vec![0.5]), json!({"f32":[0.5]})),
        (TensorData::F64(vec![1.25]), json!({"f64":[1.25]})),
        (
            TensorData::Bool(vec![false, true]),
            json!({"bool":[false,true]}),
        ),
        (
            TensorData::Utf8(vec!["żółć".into()]),
            json!({"utf8":["żółć"]}),
        ),
        (TensorData::U8(vec![]), json!({"u8":[]})),
    ] {
        let value = serde_json::to_value(&data).unwrap();
        assert_eq!(value, expected);
        assert_eq!(serde_json::from_value::<TensorData>(value).unwrap(), data);
    }
    for invalid in [
        json!({"u8":"AA=="}),
        json!({"u8":[256]}),
        json!({"f32":[],"u8":[]}),
        json!({"f32":null}),
    ] {
        assert!(serde_json::from_value::<TensorData>(invalid).is_err());
    }
}

#[derive(Default)]
struct TensorTransport {
    calls: Mutex<Vec<HttpRequest>>,
}

impl Transport for TensorTransport {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, ControlPlaneError> {
        let path = request.url.clone();
        self.calls.lock().unwrap().push(request);
        let data = if path.ends_with("/append") {
            json!({"start":0,"end":1,"revision":1})
        } else if path.ends_with("/read") {
            json!({"start":0,"end":1,"revision":1,"total_samples":1,"tensors":batch()})
        } else if path.ends_with("/search") {
            json!({"hits":[{"index":0,"distance":0.25}],"scanned_samples":1,"next_start":0,"total_samples":2,"revision":1,"skipped_samples":0,
                "execution":{"device":"gpu","adapter":"hardware","backend":"Vulkan"}})
        } else if path.ends_with("/compute") {
            json!({"start":0,"end":1,"revision":1,"values":[1.0],"execution":{"device":"cpu","adapter":null,"backend":null}})
        } else {
            json!({"config":config(),"samples":0,"revision":0})
        };
        Ok(HttpResponse {
            status: 200,
            body: serde_json::to_vec(&json!({"ok":true,"data":data})).unwrap(),
        })
    }
}

#[test]
fn tensor_methods_keep_paths_auth_optional_zero_and_actual_execution() {
    let transport = Arc::new(TensorTransport::default());
    let client = ControlPlaneClient::with_transport("http://unit.test/api/", transport.clone())
        .with_token("secret")
        .with_principal("alice");
    assert_eq!(
        client.create_tensor_dataset(&config()).unwrap().config,
        config()
    );
    let name = "training /?#%ą";
    client.tensor_dataset_info(name, None).unwrap();
    assert_eq!(
        client.tensor_dataset_info(name, Some(0)).unwrap().revision,
        0
    );
    assert_eq!(client.append_tensors(name, &batch()).unwrap().revision, 1);
    let read = TensorReadRequest {
        start: Some(0),
        limit: Some(2),
        columns: Some(vec!["embedding".into()]),
        revision: Some(0),
    };
    assert_eq!(client.read_tensors(name, &read).unwrap().tensors, batch());
    let mut search = TensorSearchRequest::new("embedding", vec![1.0, 0.0]);
    search.start = Some(0);
    search.revision = Some(0);
    search.metric = Some(TensorMetric::L2);
    let found = client.search_tensors(name, &search).unwrap();
    assert_eq!(found.execution.device, TensorDevice::Gpu);
    assert_eq!(found.execution.adapter.as_deref(), Some("hardware"));
    assert_eq!(found.execution.backend.as_deref(), Some("Vulkan"));
    assert_eq!(found.next_start, Some(0));
    let mut compute = TensorComputeRequest::new("embedding", TensorGpuOperation::L2Norm);
    compute.start = Some(0);
    compute.limit = Some(2);
    compute.revision = Some(0);
    let computed = client.compute_tensors(name, &compute).unwrap();
    assert_eq!(computed.values, vec![1.0]);
    assert_eq!(
        computed.execution.device,
        TensorDevice::Cpu,
        "client relabeled CPU reference work as GPU"
    );
    assert_eq!(computed.execution.adapter, None);
    assert_eq!(computed.execution.backend, None);

    let calls = transport.calls.lock().unwrap();
    let base = "http://unit.test/api/data/tensor-datasets/training%20%2F%3F%23%25%C4%85";
    assert_eq!(calls.len(), 7);
    for (index, want) in [
        "http://unit.test/api/builder/tensor-dataset".into(),
        base.into(),
        format!("{base}?revision=0"),
        format!("{base}/append"),
        format!("{base}/read"),
        format!("{base}/search"),
        format!("{base}/compute"),
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(&calls[index].url, want);
        assert_eq!(
            calls[index].method,
            if index == 1 || index == 2 {
                "GET"
            } else {
                "POST"
            }
        );
        for (header, value) in [
            ("Authorization", "Bearer secret"),
            ("x-multidb-principal", "alice"),
            ("Accept", "application/json"),
        ] {
            assert!(
                calls[index]
                    .headers
                    .iter()
                    .any(|(key, found)| key == header && found == value)
            );
        }
        if calls[index].method == "POST" {
            assert!(
                calls[index]
                    .headers
                    .iter()
                    .any(|(key, value)| key == "Content-Type" && value == "application/json")
            );
        }
    }
    let body = |index: usize| serde_json::from_slice::<Value>(&calls[index].body).unwrap();
    assert_eq!(body(0), serde_json::to_value(config()).unwrap());
    assert!(body(0).get("chunk_samples").is_none());
    assert_eq!(body(3), json!({"tensors":batch()}));
    assert_eq!(
        body(4),
        json!({"start":0,"limit":2,"columns":["embedding"],"revision":0})
    );
    assert_eq!(
        body(5),
        json!({"tensor":"embedding","query":[1.0,0.0],"metric":"L2","start":0,"revision":0})
    );
    assert_eq!(
        body(6),
        json!({"tensor":"embedding","operation":"l2_norm","start":0,"limit":2,"revision":0})
    );
    assert_eq!(
        serde_json::to_value(TensorReadRequest::default()).unwrap(),
        json!({})
    );
}

#[test]
fn unavailable_gpu_error_is_not_retried_or_replaced_by_cpu_fallback() {
    struct UnavailableTransport {
        calls: AtomicUsize,
    }
    impl Transport for UnavailableTransport {
        fn send(&self, _: HttpRequest) -> Result<HttpResponse, ControlPlaneError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(HttpResponse{status:503,body:br#"{"ok":false,"error":{"code":"tensor_gpu_unavailable","message":"native GPU required"}}"#.to_vec()})
        }
    }
    let transport = Arc::new(UnavailableTransport {
        calls: AtomicUsize::new(0),
    });
    let client = ControlPlaneClient::with_transport("http://unit.test/api", transport.clone());
    let errors = [
        client
            .search_tensors(
                "training",
                &TensorSearchRequest::new("embedding", vec![1.0]),
            )
            .unwrap_err(),
        client
            .compute_tensors(
                "training",
                &TensorComputeRequest::new("embedding", TensorGpuOperation::Sum),
            )
            .unwrap_err(),
    ];
    for error in errors {
        assert_eq!(error.status, 503);
        assert_eq!(error.code, "tensor_gpu_unavailable");
        assert_eq!(error.message, "native GPU required");
    }
    assert_eq!(transport.calls.load(Ordering::SeqCst), 2);
}
