# Tensor datasets in Docker

The `tensor_dataset` profile (`tensor` or `deeplake_like`) requires a native
GPU compute adapter. The default Cargo build includes `tensor-gpu`. The Linux
runtime image includes the Vulkan loader and Mesa Vulkan drivers; the
container also needs access to a compatible host GPU. The existing Compose
quickstart keeps its transactional profile and does not expose a GPU.

The Tensor profile refuses to start when a supported GPU is unavailable.
Software adapters such as Lavapipe/llvmpipe are rejected; installing a Vulkan
loader alone does not make a CPU-only host a GPU host. Native drivers and
hardware must support the compute limits requested by `wgpu`.

## Intel or AMD on a Linux Docker host

Use a host with a working Vulkan-capable driver and a render node under
`/dev/dri`. The example uses `renderD128`; select the render node of the GPU
you intend to expose. The container runs as UID 10001, so add the render
node's host group to its supplementary groups.

Build the image, set your own `MULTIDB_ADMIN_PASSWORD`, `MULTIDB_ADMIN_TOKEN`
and `MULTIDB_PG_PASSWORD` in the host environment, then run:

```bash
docker build -t multidb:tensor .
docker volume create multidb-tensor-data
docker run --rm --name multidb-tensor \
  --device=/dev/dri/renderD128 \
  --group-add="$(stat -c '%g' /dev/dri/renderD128)" \
  -p 127.0.0.1:8080:8080 -p 127.0.0.1:5432:5432 \
  -v multidb-tensor-data:/var/lib/multidb \
  -e MULTIDB_PROFILE=tensor_dataset \
  -e MULTIDB_RUNTIME_MODE=local-dev \
  -e MULTIDB_PG_TLS_MODE=disabled \
  -e MULTIDB_ADMIN_PASSWORD -e MULTIDB_ADMIN_TOKEN -e MULTIDB_PG_PASSWORD \
  multidb:tensor
```

These loopback binds and plaintext PostgreSQL settings are for local
evaluation. Production runtime settings are described in [docker.md](docker.md).
The image supplies a private `XDG_RUNTIME_DIR` owned by the runtime user;
a desktop display server is not needed for headless Vulkan compute.

## NVIDIA

Configure the host NVIDIA driver and NVIDIA Container Toolkit with Vulkan
support. Use the same runtime settings above, replacing `--device` and
`--group-add` with:

```bash
--gpus all -e NVIDIA_DRIVER_CAPABILITIES=graphics,compute,utility
```

The toolkit must expose the host driver libraries and Vulkan ICD in the
container. `--gpus all` by itself does not supply a missing driver or make an
unsupported device compatible. A custom image may be needed for a host driver
stack that the supplied Debian/Mesa runtime does not support.

## Confirm the execution device

Create a small typed embedding column, append a sample, and request a GPU
reduction. Run these commands in another shell with the same admin token:

```bash
curl --fail-with-body http://127.0.0.1:8080/api/builder/tensor-dataset \
  -H "Authorization: Bearer $MULTIDB_ADMIN_TOKEN" \
  -H 'x-multidb-principal: admin' -H 'Content-Type: application/json' \
  -d '{"name":"demo","tensors":[{"name":"embedding","dtype":"f32","shape":[3]}]}'
curl --fail-with-body http://127.0.0.1:8080/api/data/tensor-datasets/demo/append \
  -H "Authorization: Bearer $MULTIDB_ADMIN_TOKEN" \
  -H 'x-multidb-principal: admin' -H 'Content-Type: application/json' \
  -d '{"tensors":{"embedding":[{"shape":[3],"data":{"f32":[1,2,3]}}]}}'
curl --fail-with-body http://127.0.0.1:8080/api/data/tensor-datasets/demo/compute \
  -H "Authorization: Bearer $MULTIDB_ADMIN_TOKEN" \
  -H 'x-multidb-principal: admin' -H 'Content-Type: application/json' \
  -d '{"tensor":"embedding","operation":"sum","limit":1}'
```

The compute response should contain `values: [6.0]` and
`execution.device: "gpu"`, with the actual adapter and backend. Tensor search
also reports its execution device. GPU unavailability returns
`503 tensor_gpu_unavailable`; an unsupported GPU dtype, shape or memory bound
returns `422 tensor_gpu_unsupported`. The SDKs surface those errors without
retrying on the CPU.

The shared HTTP request-body limit remains **1 MiB**, including JSON encoding.
Larger binary storage chunk limits do not enlarge that transport limit. Tensor
values use typed arrays, not arbitrary JSON document blobs. Search and reductions
perform bounded numeric operations; they do not train or invoke a model.

`docker stop multidb-tensor` uses the normal graceful-shutdown path. The named
volume keeps the database. Docker GPU passthrough and physical-device execution
must be validated on the intended host; a CPU-only test environment verifies
compilation and unavailable-device errors, not GPU performance.
