//! Native GPU tensor kernels. A missing GPU is an error, never a CPU fallback.
//!
//! Production accepts only discrete, integrated or virtual GPUs on Vulkan, Metal or DX12.
//! Host code validates and packs bounded buffers; distances and reductions run in WGSL.

mod dataset;
pub(crate) use dataset::search_dataset;

use std::sync::{Arc, OnceLock};

use crate::{
    tensor::{TensorDeadline, TensorDevice, TensorError, TensorExecution, TensorGpuOperation},
    vector::VectorMetric,
};

const MAX_ROWS: usize = 1_024;
const MAX_ELEMENTS: usize = 4 * 1_024 * 1_024;
const MAX_QUERY_DIMENSION: usize = 16_384;

/// A shared native device and compute pipeline. Construction refuses software adapters.
pub struct TensorGpuEngine {
    #[cfg(feature = "tensor-gpu")]
    inner: native::GpuState,
    #[cfg(not(feature = "tensor-gpu"))]
    _unavailable: (),
}

#[derive(Clone)]
struct CachedInitError {
    unavailable: bool,
    message: String,
}

impl TensorGpuEngine {
    /// Opens a native GPU and compiles the tensor compute kernels.
    /// # Errors
    /// Fails when GPU support is disabled, no hardware adapter exists or initialization fails.
    pub fn new() -> Result<Self, TensorError> {
        #[cfg(feature = "tensor-gpu")]
        {
            Ok(Self {
                inner: native::GpuState::new(false)?,
            })
        }
        #[cfg(not(feature = "tensor-gpu"))]
        {
            Err(TensorError::GpuUnavailable(
                "this build has no tensor-gpu feature; rebuild with tensor-gpu and a native GPU"
                    .to_owned(),
            ))
        }
    }

    /// Initializes once and shares the native device, including a cached initialization error.
    /// # Errors
    /// Fails for the same reasons as `new`; a failed initialization remains failed for this process.
    pub fn shared() -> Result<Arc<Self>, TensorError> {
        static ENGINE: OnceLock<Result<Arc<TensorGpuEngine>, CachedInitError>> = OnceLock::new();
        ENGINE
            .get_or_init(|| {
                Self::new().map(Arc::new).map_err(|error| CachedInitError {
                    unavailable: matches!(error, TensorError::GpuUnavailable(_)),
                    message: error.to_string(),
                })
            })
            .clone()
            .map_err(|error| {
                if error.unavailable {
                    TensorError::GpuUnavailable(error.message)
                } else {
                    TensorError::Gpu(error.message)
                }
            })
    }

    /// Reports the actual adapter selected for this engine.
    #[must_use]
    pub fn execution(&self) -> TensorExecution {
        #[cfg(feature = "tensor-gpu")]
        {
            self.inner.execution.clone()
        }
        #[cfg(not(feature = "tensor-gpu"))]
        {
            TensorExecution {
                device: TensorDevice::Gpu,
                adapter: None,
                backend: None,
            }
        }
    }

    /// Computes one distance per equally sized row by dispatching the GPU kernel.
    /// # Errors
    /// Fails for invalid/bounded input, zero cosine vectors, GPU errors or nonfinite output.
    pub fn compute_distances(
        &self,
        rows: &[Vec<f32>],
        query: &[f32],
        metric: VectorMetric,
    ) -> Result<Vec<f32>, TensorError> {
        self.compute_distances_until(rows, query, metric, TensorDeadline::default())
    }

    pub(crate) fn compute_distances_until(
        &self,
        rows: &[Vec<f32>],
        query: &[f32],
        metric: VectorMetric,
        deadline: TensorDeadline,
    ) -> Result<Vec<f32>, TensorError> {
        deadline.check()?;
        validate_rows(rows)?;
        if query.is_empty()
            || query.len() > MAX_QUERY_DIMENSION
            || query.iter().any(|value| !value.is_finite())
        {
            return invalid("GPU query must contain 1 to 16384 finite f32 values");
        }
        if rows.iter().any(|row| row.len() != query.len()) {
            return invalid("every GPU search row must match the query dimension");
        }
        if query.iter().any(|value| value.is_subnormal())
            || metric == VectorMetric::L2
                && rows.iter().any(|row| {
                    row.iter()
                        .zip(query)
                        .any(|(left, right)| (left - right).is_subnormal())
                })
        {
            return Err(TensorError::GpuUnsupported(
                "subnormal GPU inputs or L2 differences are not portable across native adapters"
                    .to_owned(),
            ));
        }
        if metric == VectorMetric::Cosine
            && (query.iter().all(|value| *value == 0.0)
                || rows.iter().any(|row| row.iter().all(|value| *value == 0.0)))
        {
            return invalid("cosine GPU search requires nonzero query and sample vectors");
        }
        self.dispatch(rows, query, u32::from(metric != VectorMetric::L2), deadline)
    }

    /// Reduces each possibly ragged row on the GPU.
    /// # Errors
    /// Fails for invalid/bounded input, empty means, GPU errors or nonfinite output.
    pub fn reduce_rows(
        &self,
        rows: &[Vec<f32>],
        operation: TensorGpuOperation,
    ) -> Result<Vec<f32>, TensorError> {
        self.reduce_rows_until(rows, operation, TensorDeadline::default())
    }

    pub(crate) fn reduce_rows_until(
        &self,
        rows: &[Vec<f32>],
        operation: TensorGpuOperation,
        deadline: TensorDeadline,
    ) -> Result<Vec<f32>, TensorError> {
        deadline.check()?;
        validate_rows(rows)?;
        let mode = match operation {
            TensorGpuOperation::Sum => 2,
            TensorGpuOperation::Mean => {
                if rows.iter().any(Vec::is_empty) {
                    return invalid("GPU mean is undefined for an empty row");
                }
                3
            }
            TensorGpuOperation::L2Norm => 4,
        };
        self.dispatch(rows, &[], mode, deadline)
    }

    fn dispatch(
        &self,
        rows: &[Vec<f32>],
        query: &[f32],
        mode: u32,
        deadline: TensorDeadline,
    ) -> Result<Vec<f32>, TensorError> {
        #[cfg(feature = "tensor-gpu")]
        {
            self.inner.dispatch(rows, query, mode, deadline)
        }
        #[cfg(not(feature = "tensor-gpu"))]
        {
            let _ = (rows, query, mode, deadline);
            Err(TensorError::GpuUnavailable(
                "this build has no tensor-gpu feature".to_owned(),
            ))
        }
    }
}

fn validate_rows(rows: &[Vec<f32>]) -> Result<(), TensorError> {
    if rows.len() > MAX_ROWS {
        return invalid("GPU batch exceeds 1024 rows");
    }
    let mut elements = 0_usize;
    for row in rows {
        elements = elements
            .checked_add(row.len())
            .ok_or_else(|| TensorError::InvalidValue("GPU element count overflow".to_owned()))?;
        if elements > MAX_ELEMENTS {
            return invalid("GPU batch exceeds 4194304 elements");
        }
        if row.iter().any(|value| !value.is_finite()) {
            return invalid("GPU tensor inputs must be finite f32 values");
        }
        if row.iter().any(|value| value.is_subnormal()) {
            return Err(TensorError::GpuUnsupported(
                "subnormal GPU inputs are not portable across native adapters".to_owned(),
            ));
        }
    }
    Ok(())
}

fn invalid<T>(message: &str) -> Result<T, TensorError> {
    Err(TensorError::InvalidValue(message.to_owned()))
}

#[cfg(feature = "tensor-gpu")]
mod native {
    use std::{
        borrow::Cow,
        sync::{
            Mutex, TryLockError,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        time::{Duration, Instant},
    };

    use super::{TensorDeadline, TensorDevice, TensorError, TensorExecution, invalid};

    pub(super) struct GpuState {
        device: wgpu::Device,
        queue: wgpu::Queue,
        pipeline: wgpu::ComputePipeline,
        pub(super) execution: TensorExecution,
        // Error scopes are device-wide. Keep dispatch, mapping and scope draining together.
        serial: Mutex<()>,
        failed: AtomicBool,
    }

    pub(super) fn hardware_type(kind: wgpu::DeviceType) -> bool {
        matches!(
            kind,
            wgpu::DeviceType::DiscreteGpu
                | wgpu::DeviceType::IntegratedGpu
                | wgpu::DeviceType::VirtualGpu
        )
    }

    impl GpuState {
        pub(super) fn new(allow_software_for_tests: bool) -> Result<Self, TensorError> {
            let backends = wgpu::Backends::VULKAN | wgpu::Backends::METAL | wgpu::Backends::DX12;
            if (wgpu::Instance::enabled_backend_features() & backends).is_empty() {
                return Err(TensorError::GpuUnavailable(
                    "no native GPU backend is compiled".to_owned(),
                ));
            }
            let instance = wgpu::Instance::new(&wgpu::InstanceDescriptor {
                backends,
                ..Default::default()
            });
            let adapter =
                pollster::block_on(instance.request_adapter(&wgpu::RequestAdapterOptions {
                    power_preference: wgpu::PowerPreference::HighPerformance,
                    force_fallback_adapter: false,
                    compatible_surface: None,
                }))
                .map_err(|error| {
                    TensorError::GpuUnavailable(format!("no native GPU adapter: {error}"))
                })?;
            let info = adapter.get_info();
            // The test override is crate-private and has no production call path.
            if !(hardware_type(info.device_type) || cfg!(test) && allow_software_for_tests) {
                return Err(TensorError::GpuUnavailable(format!(
                    "adapter {} is {:?}; a discrete, integrated or virtual GPU is required (no software fallback)",
                    info.name, info.device_type,
                )));
            }
            let (device, queue) =
                pollster::block_on(adapter.request_device(&wgpu::DeviceDescriptor {
                    label: Some("MultiDB tensor GPU"),
                    required_features: wgpu::Features::empty(),
                    required_limits: wgpu::Limits::default(),
                    memory_hints: wgpu::MemoryHints::MemoryUsage,
                    trace: wgpu::Trace::Off,
                }))
                .map_err(|error| {
                    TensorError::GpuUnavailable(format!(
                        "GPU device initialization failed: {error}"
                    ))
                })?;
            device.on_uncaptured_error(Box::new(|error| {
                tracing::error!(error = %error, "uncaptured tensor GPU device error");
            }));
            push_scopes(&device);
            let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
                label: Some("tensor distance and reduction WGSL"),
                source: wgpu::ShaderSource::Wgsl(Cow::Borrowed(include_str!(
                    "shaders/tensor_compute.wgsl"
                ))),
            });
            let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
                label: Some("tensor compute"),
                layout: None,
                module: &shader,
                entry_point: Some("main"),
                compilation_options: wgpu::PipelineCompilationOptions::default(),
                cache: None,
            });
            check_scopes(&device)?;
            Ok(Self {
                device,
                queue,
                pipeline,
                serial: Mutex::new(()),
                failed: AtomicBool::new(false),
                execution: TensorExecution {
                    device: TensorDevice::Gpu,
                    adapter: Some(info.name),
                    backend: Some(format!("{:?}", info.backend)),
                },
            })
        }

        pub(super) fn dispatch(
            &self,
            rows: &[Vec<f32>],
            query: &[f32],
            mode: u32,
            deadline: TensorDeadline,
        ) -> Result<Vec<f32>, TensorError> {
            deadline.check()?;
            self.ensure_available()?;
            if rows.is_empty() {
                return Ok(Vec::new());
            }
            let _serial = loop {
                deadline.check()?;
                match self.serial.try_lock() {
                    Ok(guard) => break guard,
                    Err(TryLockError::Poisoned(_)) => {
                        return Err(TensorError::Gpu("GPU dispatch lock poisoned".to_owned()));
                    }
                    Err(TryLockError::WouldBlock) => std::thread::sleep(Duration::from_millis(1)),
                }
            };
            self.ensure_available()?;
            push_scopes(&self.device);
            let result = self.dispatch_scoped(rows, query, mode, deadline);
            let errors = check_scopes(&self.device);
            match result {
                Err(error @ TensorError::Timeout(_)) => Err(error),
                other => errors.and(other),
            }
        }

        // Keep buffer lifetimes, GPU submission and readback in one error-scoped operation.
        #[allow(clippy::too_many_lines)]
        fn dispatch_scoped(
            &self,
            rows: &[Vec<f32>],
            query: &[f32],
            mode: u32,
            query_deadline: TensorDeadline,
        ) -> Result<Vec<f32>, TensorError> {
            let mut values = Vec::new();
            let mut offsets = Vec::with_capacity((rows.len() + 1) * 4);
            offsets.extend_from_slice(&0_u32.to_le_bytes());
            let mut total = 0_u32;
            for row in rows {
                query_deadline.check()?;
                for value in row {
                    values.extend_from_slice(&value.to_le_bytes());
                }
                total += u32::try_from(row.len())
                    .map_err(|_| TensorError::InvalidValue("row length exceeds u32".to_owned()))?;
                offsets.extend_from_slice(&total.to_le_bytes());
            }
            query_deadline.check()?;
            if values.is_empty() {
                values.extend_from_slice(&0_f32.to_le_bytes());
            }
            let query_bytes = if query.is_empty() {
                0_f32.to_le_bytes().to_vec()
            } else {
                query.iter().flat_map(|value| value.to_le_bytes()).collect()
            };
            let row_count = u32::try_from(rows.len())
                .map_err(|_| TensorError::InvalidValue("row count exceeds u32".to_owned()))?;
            let dimension = u32::try_from(query.len())
                .map_err(|_| TensorError::InvalidValue("query dimension exceeds u32".to_owned()))?;
            let params = [row_count, dimension, mode, 0]
                .into_iter()
                .flat_map(u32::to_le_bytes)
                .collect::<Vec<_>>();
            let values = self.input("tensor values", &values, wgpu::BufferUsages::STORAGE);
            let offsets = self.input("tensor row offsets", &offsets, wgpu::BufferUsages::STORAGE);
            let query_buffer =
                self.input("tensor query", &query_bytes, wgpu::BufferUsages::STORAGE);
            let params = self.input("tensor params", &params, wgpu::BufferUsages::UNIFORM);
            let output_size = u64::from(row_count) * 4;
            let output = self.device.create_buffer(&wgpu::BufferDescriptor {
                label: Some("tensor output"),
                size: output_size,
                usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
                mapped_at_creation: false,
            });
            let readback = self.device.create_buffer(&wgpu::BufferDescriptor {
                label: Some("tensor readback"),
                size: output_size,
                usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
                mapped_at_creation: false,
            });
            let bind_group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
                label: Some("tensor compute bindings"),
                layout: &self.pipeline.get_bind_group_layout(0),
                entries: &[
                    wgpu::BindGroupEntry {
                        binding: 0,
                        resource: values.as_entire_binding(),
                    },
                    wgpu::BindGroupEntry {
                        binding: 1,
                        resource: offsets.as_entire_binding(),
                    },
                    wgpu::BindGroupEntry {
                        binding: 2,
                        resource: query_buffer.as_entire_binding(),
                    },
                    wgpu::BindGroupEntry {
                        binding: 3,
                        resource: params.as_entire_binding(),
                    },
                    wgpu::BindGroupEntry {
                        binding: 4,
                        resource: output.as_entire_binding(),
                    },
                ],
            });
            let mut encoder = self
                .device
                .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                    label: Some("tensor GPU commands"),
                });
            {
                let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                    label: Some("tensor distance/reduction"),
                    timestamp_writes: None,
                });
                pass.set_pipeline(&self.pipeline);
                pass.set_bind_group(0, &bind_group, &[]);
                pass.dispatch_workgroups(row_count.div_ceil(64), 1, 1);
            }
            encoder.copy_buffer_to_buffer(&output, 0, &readback, 0, output_size);
            query_deadline.check()?;
            self.queue.submit([encoder.finish()]);
            let (sender, receiver) = mpsc::sync_channel(1);
            readback
                .slice(..)
                .map_async(wgpu::MapMode::Read, move |result| {
                    let _ = sender.send(result);
                });
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                if let Err(error) = query_deadline.check() {
                    self.failed.store(true, Ordering::Release);
                    self.device.destroy();
                    return Err(error);
                }
                self.device
                    .poll(wgpu::PollType::Poll)
                    .map_err(|error| TensorError::Gpu(format!("GPU polling failed: {error}")))?;
                match receiver.try_recv() {
                    Ok(result) => {
                        result.map_err(|error| {
                            TensorError::Gpu(format!("GPU readback failed: {error}"))
                        })?;
                        break;
                    }
                    Err(mpsc::TryRecvError::Disconnected) => {
                        return Err(TensorError::Gpu(
                            "GPU readback callback disconnected".to_owned(),
                        ));
                    }
                    Err(mpsc::TryRecvError::Empty) => {}
                }
                if Instant::now() >= deadline {
                    self.failed.store(true, Ordering::Release);
                    self.device.destroy();
                    return Err(TensorError::Gpu(
                        "GPU dispatch timed out after 30 seconds; device destroyed".to_owned(),
                    ));
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            let mapped = readback.slice(..).get_mapped_range();
            let result = mapped
                .as_chunks::<4>()
                .0
                .iter()
                .map(|bytes| f32::from_le_bytes(*bytes))
                .collect::<Vec<_>>();
            drop(mapped);
            readback.unmap();
            if result.iter().any(|value| !value.is_finite()) {
                return invalid("GPU result exceeds the finite f32 range");
            }
            Ok(result)
        }

        fn ensure_available(&self) -> Result<(), TensorError> {
            if self.failed.load(Ordering::Acquire) {
                return Err(TensorError::GpuUnavailable("tensor GPU device stopped after a timeout; restart the server to reinitialize it".to_owned()));
            }
            Ok(())
        }

        fn input(&self, label: &str, bytes: &[u8], usage: wgpu::BufferUsages) -> wgpu::Buffer {
            let buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
                label: Some(label),
                size: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                usage: usage | wgpu::BufferUsages::COPY_DST,
                mapped_at_creation: false,
            });
            self.queue.write_buffer(&buffer, 0, bytes);
            buffer
        }
    }

    fn push_scopes(device: &wgpu::Device) {
        device.push_error_scope(wgpu::ErrorFilter::Internal);
        device.push_error_scope(wgpu::ErrorFilter::OutOfMemory);
        device.push_error_scope(wgpu::ErrorFilter::Validation);
    }

    fn check_scopes(device: &wgpu::Device) -> Result<(), TensorError> {
        let mut first = None;
        for _ in 0..3 {
            if let Some(error) = pollster::block_on(device.pop_error_scope()) {
                first.get_or_insert_with(|| error.to_string());
            }
        }
        match first {
            Some(error) => Err(TensorError::Gpu(error)),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gpu_input_limits_and_nonfinite_values_are_rejected_before_upload() {
        assert!(validate_rows(&vec![Vec::new(); MAX_ROWS + 1]).is_err());
        assert!(validate_rows(&[vec![0.0; MAX_ELEMENTS + 1]]).is_err());
        assert!(validate_rows(&[vec![f32::NAN]]).is_err());
        assert!(validate_rows(&[vec![f32::INFINITY]]).is_err());
        assert!(matches!(
            validate_rows(&[vec![f32::from_bits(1)]]),
            Err(TensorError::GpuUnsupported(_))
        ));
        assert!(validate_rows(&[vec![1.0, 2.0], vec![]]).is_ok());
    }

    #[cfg(feature = "tensor-gpu")]
    #[test]
    fn production_adapter_policy_rejects_software_and_other_devices() {
        assert!(!native::hardware_type(wgpu::DeviceType::Cpu));
        assert!(!native::hardware_type(wgpu::DeviceType::Other));
        for kind in [
            wgpu::DeviceType::DiscreteGpu,
            wgpu::DeviceType::IntegratedGpu,
            wgpu::DeviceType::VirtualGpu,
        ] {
            assert!(native::hardware_type(kind));
        }
    }

    #[cfg(not(feature = "tensor-gpu"))]
    #[test]
    fn feature_disabled_refuses_to_create_a_gpu_engine() {
        assert!(matches!(
            TensorGpuEngine::new(),
            Err(TensorError::GpuUnavailable(_))
        ));
    }

    #[cfg(feature = "tensor-gpu")]
    #[test]
    fn hardware_compute_matches_known_distances_and_ragged_reductions_when_available()
    -> Result<(), TensorError> {
        let engine = match TensorGpuEngine::new() {
            Ok(engine) => engine,
            Err(TensorError::GpuUnavailable(reason)) => {
                eprintln!("SKIP physical GPU computation: {reason}");
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        assert_eq!(engine.execution().device, TensorDevice::Gpu);
        let distances = engine.compute_distances(
            &[vec![3.0, 4.0], vec![0.0, 0.0]],
            &[0.0, 0.0],
            VectorMetric::L2,
        )?;
        near(&distances, &[5.0, 0.0]);
        near(
            &engine.compute_distances(&[vec![1.0e20, 1.0]], &[1.0e20, 0.0], VectorMetric::L2)?,
            &[1.0],
        );
        let huge = f32::MAX / 4.0;
        let cosine = engine.compute_distances(
            &[vec![huge, huge], vec![-huge, -huge]],
            &[huge, huge],
            VectorMetric::Cosine,
        )?;
        near(&cosine, &[0.0, 2.0]);
        let rows = [vec![1.0, 2.0, 3.0], vec![3.0, 4.0], Vec::new()];
        near(
            &engine.reduce_rows(&rows, TensorGpuOperation::Sum)?,
            &[6.0, 7.0, 0.0],
        );
        near(
            &engine.reduce_rows(&rows[..2], TensorGpuOperation::Mean)?,
            &[2.0, 3.5],
        );
        near(
            &engine.reduce_rows(&rows, TensorGpuOperation::L2Norm)?,
            &[14.0_f32.sqrt(), 5.0, 0.0],
        );
        assert!(
            engine
                .compute_distances(&[vec![1.0]], &[0.0], VectorMetric::Cosine)
                .is_err()
        );
        Ok(())
    }

    // This validates shader compilation, GPU API dispatch and mapping with a software
    // Vulkan driver only when explicitly requested. It is never a production fallback,
    // and does not establish that physical hardware was tested.
    #[cfg(feature = "tensor-gpu")]
    #[test]
    #[ignore = "explicit software Vulkan kernel validation; not physical GPU coverage"]
    fn software_kernel_validation() -> Result<(), TensorError> {
        assert!(
            matches!(TensorGpuEngine::new(), Err(TensorError::GpuUnavailable(_))),
            "run this software-only validation with a software Vulkan ICD; production must refuse it"
        );
        let engine = TensorGpuEngine {
            inner: native::GpuState::new(true)?,
        };
        let distances = engine.compute_distances(
            &[vec![3.0, 4.0], vec![0.0, 0.0]],
            &[0.0, 0.0],
            VectorMetric::L2,
        )?;
        near(&distances, &[5.0, 0.0]);
        near(
            &engine.compute_distances(&[vec![1.0e20, 1.0]], &[1.0e20, 0.0], VectorMetric::L2)?,
            &[1.0],
        );
        near(
            &engine.compute_distances(
                &[vec![f32::MAX, f32::MAX], vec![-f32::MAX, -f32::MAX]],
                &[f32::MAX, f32::MAX],
                VectorMetric::Cosine,
            )?,
            &[0.0, 2.0],
        );
        let ragged = [vec![1.0, 2.0, 3.0], vec![3.0, 4.0], Vec::new()];
        near(
            &engine.reduce_rows(&ragged, TensorGpuOperation::Sum)?,
            &[6.0, 7.0, 0.0],
        );
        near(
            &engine.reduce_rows(&ragged[..2], TensorGpuOperation::Mean)?,
            &[2.0, 3.5],
        );
        near(
            &engine.reduce_rows(&ragged, TensorGpuOperation::L2Norm)?,
            &[14.0_f32.sqrt(), 5.0, 0.0],
        );
        assert!(matches!(
            engine.compute_distances(
                &[vec![f32::MAX, f32::MAX]],
                &[-f32::MAX, -f32::MAX],
                VectorMetric::L2
            ),
            Err(TensorError::InvalidValue(_))
        ));
        assert!(matches!(
            engine.reduce_rows(&[vec![f32::MAX, f32::MAX]], TensorGpuOperation::Sum),
            Err(TensorError::InvalidValue(_))
        ));
        near(
            &engine.reduce_rows(&[vec![f32::MAX, f32::MAX]], TensorGpuOperation::Mean)?,
            &[f32::MAX],
        );
        software_dataset_search_validation(&engine)?;
        Ok(())
    }

    #[cfg(feature = "tensor-gpu")]
    fn software_dataset_search_validation(engine: &TensorGpuEngine) -> Result<(), TensorError> {
        use crate::{
            db::{DbConfig, Profile, create_database},
            repl::{Op, ReadConsistency, Replication},
            tensor::{
                TENSOR_CHUNKS_TABLE, Tensor, TensorColumn, TensorDType, TensorData,
                TensorDatasetConfig, TensorSearchRequest,
            },
        };
        use std::collections::BTreeMap;
        let setup_error = |error: crate::db::DbError| TensorError::Gpu(error.to_string());
        let directory = tempfile::tempdir().map_err(|e| TensorError::Gpu(e.to_string()))?;
        // Balanced storage allows the software device to be injected only into this private
        // helper. Production Tensor-profile initialization still rejects software adapters.
        let mut database = create_database(DbConfig::on_disk(
            Profile::Balanced,
            directory.path().join("software-kernel-test.redb"),
        ))
        .map_err(|error| TensorError::Gpu(error.to_string()))?;
        database
            .create_tensor_dataset(TensorDatasetConfig::new(
                "gpu_e2e",
                vec![
                    TensorColumn::new("embedding", TensorDType::F32, vec![Some(2)]),
                    TensorColumn::new("text", TensorDType::Utf8, vec![]),
                ],
            ))
            .map_err(setup_error)?;
        let dataset = database.tensor_dataset("gpu_e2e").map_err(setup_error)?;
        let batch = |rows: Vec<Vec<f32>>| {
            BTreeMap::from([
                (
                    "text".to_owned(),
                    rows.iter()
                        .map(|_| Tensor {
                            shape: vec![],
                            data: TensorData::Utf8(vec!["not searched".to_owned()]),
                        })
                        .collect(),
                ),
                (
                    "embedding".to_owned(),
                    rows.into_iter()
                        .map(|values| Tensor {
                            shape: vec![2],
                            data: TensorData::F32(values),
                        })
                        .collect(),
                ),
            ])
        };
        let first = dataset.append(&batch(vec![vec![0.0, 1.0], vec![1.0, 1.0], vec![0.0, 0.0]]))?;
        let second = dataset.append(&batch(vec![vec![-1.0, 0.0], vec![1.0, 0.0]]))?;
        // Poison an unselected text chunk: selecting only embedding must not decode it.
        let key = database
            .range(TENSOR_CHUNKS_TABLE, &[], &[], ReadConsistency::Strong)?
            .into_iter()
            .find(|(key, _)| key.windows(4).any(|part| part == b"text"))
            .map(|(key, _)| key)
            .ok_or_else(|| TensorError::Corruption("fixture text chunk missing".to_owned()))?;
        database.propose(Op::Put {
            table: TENSOR_CHUNKS_TABLE.to_owned(),
            key,
            value: b"corrupt unselected text".to_vec(),
        })?;
        let mut request = TensorSearchRequest::new("embedding", vec![1.0, 0.0]);
        request.k = 2;
        let latest = super::dataset::search_dataset_with_engine(&dataset, &request, engine)?;
        assert_eq!(
            latest.hits.iter().map(|hit| hit.index).collect::<Vec<_>>(),
            vec![4, 1]
        );
        assert_eq!(latest.scanned_samples, 5);
        assert_eq!(latest.skipped_samples, 1);
        assert_eq!(latest.total_samples, 5);
        assert_eq!(latest.revision, second.revision);
        assert_eq!(latest.execution.device, TensorDevice::Gpu);
        assert_eq!(latest.next_start, None);
        request.revision = Some(first.revision);
        let snapshot = super::dataset::search_dataset_with_engine(&dataset, &request, engine)?;
        assert_eq!(
            snapshot
                .hits
                .iter()
                .map(|hit| hit.index)
                .collect::<Vec<_>>(),
            vec![1, 0]
        );
        assert_eq!(snapshot.total_samples, 3);
        assert_eq!(snapshot.scanned_samples, 3);
        assert_eq!(snapshot.skipped_samples, 1);
        assert_eq!(snapshot.revision, first.revision);
        request.revision = None;
        request.start = 1;
        request.max_samples = 2;
        let bounded = super::dataset::search_dataset_with_engine(&dataset, &request, engine)?;
        assert_eq!(
            bounded.hits.iter().map(|hit| hit.index).collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(bounded.scanned_samples, 2);
        assert_eq!(bounded.skipped_samples, 1);
        assert_eq!(bounded.next_start, Some(3));
        Ok(())
    }

    #[cfg(feature = "tensor-gpu")]
    fn near(actual: &[f32], expected: &[f32]) {
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected) {
            assert!((actual - expected).abs() < 1e-4, "{actual} != {expected}");
        }
    }
}
