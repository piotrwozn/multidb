// One invocation per row. Inputs/outputs stay on the GPU for the whole reduction.
struct Params {
    row_count: u32,
    dimension: u32,
    mode: u32,
    padding: u32,
};
@group(0) @binding(0) var<storage, read> values: array<f32>;
@group(0) @binding(1) var<storage, read> offsets: array<u32>;
@group(0) @binding(2) var<storage, read> query: array<f32>;
@group(0) @binding(3) var<uniform> params: Params;
@group(0) @binding(4) var<storage, read_write> output: array<f32>;

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) id: vec3<u32>) {
    let row = id.x;
    if row >= params.row_count {
        return;
    }
    let start = offsets[row];
    let end = offsets[row + 1u];
    let count = end - start;
    // Mode 0: L2 distance. Scale differences, so equal large coordinates cannot
    // erase smaller differences in other dimensions before they are squared.
    if params.mode == 0u {
        var scale = 0.0;
        for (var i = 0u; i < params.dimension; i += 1u) {
            let a = values[start + i];
            let b = query[i];
            if ((a < 0.0 && b > 0.0) || (a > 0.0 && b < 0.0)) &&
                abs(a) > 3.4028234663852886e38 - abs(b) {
                output[row] = bitcast<f32>(0x7f800000u);
                return;
            }
            scale = max(scale, abs(a - b));
        }
        var squared = 0.0;
        if scale > 0.0 {
            for (var i = 0u; i < params.dimension; i += 1u) {
                let delta = (values[start + i] - query[i]) / scale;
                squared += delta * delta;
            }
        }
        let norm = sqrt(squared);
        if norm > 1.0 && scale > 3.4028234663852886e38 / norm {
            output[row] = bitcast<f32>(0x7f800000u);
        } else {
            output[row] = scale * norm;
        }
        return;
    }
    // Mode 1: cosine distance. Each vector is scaled independently before norms/dot.
    if params.mode == 1u {
        var row_scale = 0.0;
        var query_scale = 0.0;
        for (var i = 0u; i < params.dimension; i += 1u) {
            row_scale = max(row_scale, abs(values[start + i]));
            query_scale = max(query_scale, abs(query[i]));
        }
        if row_scale == 0.0 || query_scale == 0.0 {
            output[row] = bitcast<f32>(0x7f800000u);
            return;
        }
        var dot = 0.0;
        var row_norm = 0.0;
        var query_norm = 0.0;
        for (var i = 0u; i < params.dimension; i += 1u) {
            let a = values[start + i] / row_scale;
            let b = query[i] / query_scale;
            dot += a * b;
            row_norm += a * a;
            query_norm += b * b;
        }
        output[row] = 1.0 - clamp(dot / (sqrt(row_norm) * sqrt(query_norm)), -1.0, 1.0);
        return;
    }
    // Mode 4: L2 norm, also scaled before squaring.
    if params.mode == 4u {
        var scale = 0.0;
        for (var i = start; i < end; i += 1u) {
            scale = max(scale, abs(values[i]));
        }
        var squared = 0.0;
        if scale > 0.0 {
            for (var i = start; i < end; i += 1u) {
                let value = values[i] / scale;
                squared += value * value;
            }
        }
        let norm = sqrt(squared);
        if norm > 1.0 && scale > 3.4028234663852886e38 / norm {
            output[row] = bitcast<f32>(0x7f800000u);
        } else {
            output[row] = scale * norm;
        }
        return;
    }
    // Mode 2: sum; mode 3: mean, scaled before accumulation.
    var scale = 0.0;
    for (var i = start; i < end; i += 1u) {
        scale = max(scale, abs(values[i]));
    }
    var total = 0.0;
    if scale > 0.0 {
        for (var i = start; i < end; i += 1u) {
            total += values[i] / scale;
        }
    }
    if params.mode == 3u {
        output[row] = clamp(total / f32(count), -1.0, 1.0) * scale;
    } else if abs(total) > 1.0 && scale > 3.4028234663852886e38 / abs(total) {
        output[row] = bitcast<f32>(0x7f800000u);
    } else {
        output[row] = total * scale;
    }
}
