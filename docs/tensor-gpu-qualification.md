# Physical GPU qualification

Run the executable release API qualification on each supported POSIX GPU host:

```sh
python3 scripts/qualify_tensor_gpu.py \
  --binary target/release/multidb \
  --revision "$(git rev-parse HEAD)" \
  --output /var/tmp/multidb-gpu-qualification \
  --requests 200 --concurrency 4
```

The output directory must be new. It contains a disposable persistent database,
server log, binary SHA-256, host information, adapter/backend evidence and
`result.json`. The command uses loopback listeners and fresh temporary credentials;
it never opens an existing database. Build the intended release binary first and
keep its source revision together with the evidence. A failed run exits nonzero.

The strict Tensor profile must execute on a physical GPU. Missing adapters,
software adapters, unsupported operations and mismatched CPU reference results
fail explicitly. The harness checks all six storage dtypes, fixed/ragged/empty
shapes, multimodal data, exact GPU search, reductions, immutable revisions,
concurrent requests and persistence after a real graceful server restart.
Unsupported tensor operations must reject without CPU fallback.

A passing run qualifies these functional checks on the recorded host. Production
qualification additionally requires driver/device-loss injection, every declared
hardware/OS combination, and production-duration load and memory pressure. Windows
needs a platform-specific runner because this harness uses POSIX process signals.
`complete_production_qualification` remains false to preserve that distinction.
No physical GPU was available in the implementation environment; this document
and executable are preparation for real hardware testing, not a hardware pass.
