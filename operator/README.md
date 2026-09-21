Operator that handles CKH CRD.
Langauge: Go.
Use controller runtime & envtest for testing.

Currently a skeleton: the types and the reconciler compile-shaped but empty, no
generated files checked in.

## Structure

```
api/v1alpha1/         the API. Hand-written types + the markers codegen reads.
  groupversion_info.go  group (ckh.io), version, SchemeBuilder
  ckh_types.go          CKHSpec / CKHStatus / CKH / CKHList
internal/controller/  the reconcile loop
cmd/main.go           builds a manager, registers the reconciler, starts it
config/               codegen output (CRD + RBAC yaml). Does not exist yet.
bin/                  downloaded tools. Does not exist yet.
```

The pieces fit together like this: `api/v1alpha1` defines the Go structs and
registers them in a **scheme**; `main.go` adds that scheme to a **manager**,
which owns the caches and clients; the manager runs the **reconciler**, which is
handed the *name* of a changed CKH and is expected to read current state and
make it match `.spec`.

## Codegen

`// +kubebuilder:...` comments in the source are the input. Two outputs come
from them, both via `controller-gen`:

| output | from | why it is needed |
| --- | --- | --- |
| `api/v1alpha1/zz_generated.deepcopy.go` | `+kubebuilder:object:*` | the types do not satisfy `runtime.Object` without it, so **nothing compiles until you run this** |
| `config/crd/*.yaml`, `config/rbac/*.yaml` | field comments, `+kubebuilder:rbac:*` | the CRD the apiserver needs, and the Role the operator needs |

```sh
make tidy      # first time only -- go.sum is not checked in
make codegen   # generate + manifests
```

Rerun `make codegen` after editing anything under `api/` or any marker.
`make clean` deletes all of it, including `bin/`.

## Running

```sh
make install   # apply the CRD to the current cluster
make run       # run the controller locally against your kubeconfig
make test      # envtest: real apiserver + etcd, no cluster
```
