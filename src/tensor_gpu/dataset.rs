//! Bounded native-GPU distance search over immutable tensor dataset revisions.

use super::TensorGpuEngine;
use crate::{
    repl::Replication,
    tensor::{
        TensorDType, TensorData, TensorDataset, TensorDatasetConfig, TensorDeadline, TensorError,
        TensorReadRequest, TensorSearchHit, TensorSearchRequest, TensorSearchResult,
    },
    vector::VectorMetric,
};

const MAX_GPU_BATCH_FLOATS: usize = 4_194_304;
const MAX_GPU_BATCH_ROWS: usize = 1_024;

pub(crate) fn search_dataset<R: Replication + ?Sized>(
    dataset: &TensorDataset<'_, R>,
    request: &TensorSearchRequest,
) -> Result<TensorSearchResult, TensorError> {
    let deadline = dataset.execution_deadline()?;
    let query = validated_query(dataset.config(), request)?;
    deadline.check()?;
    let engine = TensorGpuEngine::shared()?;
    search_dataset_until(dataset, request, &engine, &query, deadline)
}

#[cfg(test)]
pub(super) fn search_dataset_with_engine<R: Replication + ?Sized>(
    dataset: &TensorDataset<'_, R>,
    request: &TensorSearchRequest,
    engine: &TensorGpuEngine,
) -> Result<TensorSearchResult, TensorError> {
    let deadline = dataset.execution_deadline()?;
    let query = validated_query(dataset.config(), request)?;
    search_dataset_until(dataset, request, engine, &query, deadline)
}

fn validated_query(
    config: &TensorDatasetConfig,
    request: &TensorSearchRequest,
) -> Result<Vec<f32>, TensorError> {
    request.validate()?;
    gpu_search_dimension(config, request)?;
    let query = request
        .query
        .iter()
        .map(|value| checked_f32(*value))
        .collect::<Result<Vec<_>, _>>()?;
    if request.metric == VectorMetric::Cosine && query.iter().all(|value| *value == 0.0) {
        return Err(TensorError::InvalidValue(
            "cosine query must not be zero".to_owned(),
        ));
    }
    Ok(query)
}

fn search_dataset_until<R: Replication + ?Sized>(
    dataset: &TensorDataset<'_, R>,
    request: &TensorSearchRequest,
    engine: &TensorGpuEngine,
    query: &[f32],
    deadline: TensorDeadline,
) -> Result<TensorSearchResult, TensorError> {
    deadline.check()?;
    let dim = query.len();
    let info = dataset.info(request.revision)?;
    let end = execution_end(request.start, request.max_samples, info.samples)?;
    let mut hits = Vec::new();
    let mut position = request.start;
    let mut skipped_samples = 0;
    let batch_size = MAX_GPU_BATCH_ROWS.min(MAX_GPU_BATCH_FLOATS / dim).max(1);
    while position < end {
        deadline.check()?;
        let limit = batch_size.min(
            usize::try_from(end - position)
                .map_err(|error| TensorError::InvalidValue(error.to_string()))?,
        );
        let mut batch = dataset.read_with_deadline(
            &TensorReadRequest {
                start: position,
                limit,
                columns: Some(vec![request.tensor.clone()]),
                revision: Some(info.revision),
            },
            deadline,
        )?;
        let tensors = batch.tensors.remove(&request.tensor).ok_or_else(|| {
            TensorError::Corruption("search tensor missing from selected batch".to_owned())
        })?;
        let mut rows = Vec::with_capacity(tensors.len());
        let mut indices = Vec::with_capacity(tensors.len());
        for (offset, tensor) in tensors.into_iter().enumerate() {
            let TensorData::F32(values) = tensor.data else {
                return Err(TensorError::Corruption(
                    "F32 tensor payload has another dtype".to_owned(),
                ));
            };
            if request.metric == VectorMetric::Cosine && values.iter().all(|value| *value == 0.0) {
                skipped_samples += 1;
                continue;
            }
            indices.push(
                position
                    + u64::try_from(offset)
                        .map_err(|error| TensorError::InvalidValue(error.to_string()))?,
            );
            rows.push(values);
        }
        if !rows.is_empty() {
            let distances =
                engine.compute_distances_until(&rows, query, request.metric, deadline)?;
            for (index, distance) in indices.into_iter().zip(distances) {
                hits.push(TensorSearchHit {
                    index,
                    distance: f64::from(distance),
                });
            }
            hits.sort_by(|left, right| {
                left.distance
                    .total_cmp(&right.distance)
                    .then_with(|| left.index.cmp(&right.index))
            });
            hits.truncate(request.k);
        }
        if batch.end <= position {
            return Err(TensorError::Corruption(
                "tensor search did not advance".to_owned(),
            ));
        }
        position = batch.end;
    }
    deadline.check()?;
    Ok(TensorSearchResult {
        hits,
        scanned_samples: end - request.start,
        skipped_samples,
        next_start: (end < info.samples).then_some(end),
        total_samples: info.samples,
        revision: info.revision,
        execution: engine.execution(),
    })
}

fn gpu_search_dimension(
    config: &TensorDatasetConfig,
    request: &TensorSearchRequest,
) -> Result<usize, TensorError> {
    let column = config
        .tensors
        .iter()
        .find(|column| column.name == request.tensor)
        .ok_or_else(|| TensorError::InvalidValue("unknown search tensor".to_owned()))?;
    let [Some(dim)] = column.shape.as_slice() else {
        return Err(TensorError::GpuUnsupported(
            "GPU tensor search requires a fixed rank-one F32 tensor".to_owned(),
        ));
    };
    if column.dtype != TensorDType::F32 || *dim == 0 || *dim > 16_384 {
        return Err(TensorError::GpuUnsupported(
            "GPU tensor search requires a fixed rank-one F32 tensor with dimension <=16384"
                .to_owned(),
        ));
    }
    if request.query.len() != *dim {
        return Err(TensorError::InvalidValue(
            "query dimension does not match the tensor schema".to_owned(),
        ));
    }
    Ok(*dim)
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
    if !value.is_finite() || !converted.is_finite() || value != 0.0 && converted == 0.0 {
        return Err(TensorError::InvalidValue(
            "tensor value is not a finite representable F32".to_owned(),
        ));
    }
    if converted.is_subnormal() {
        return Err(TensorError::GpuUnsupported(
            "subnormal GPU queries are not portable across native adapters".to_owned(),
        ));
    }
    Ok(converted)
}
