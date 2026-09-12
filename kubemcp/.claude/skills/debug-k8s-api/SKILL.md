---
name: debug-k8s-api
description: Debug a Kubernetes apiserver interaction in kubemcp - a tool returning wrong or empty data, an unexpected status code, a request that does not look like what kubectl sends, or a kubernetes_asyncio behaviour that seems wrong. Also covers turning a reproduction into a permanent test in the fake apiserver.
---

# Debugging an apiserver interaction

Most bugs here are not in the tool logic. They are in the gap between what we
think the client sends and what it actually sends, or between what the fake
apiserver accepts and what a real one accepts.

## First: is it the library, not you?

`kubernetes_asyncio` has several sharp edges that this codebase works around.
Check these before going deeper — each has cost real time:

| Symptom | Cause |
|---|---|
| A tool "succeeds" but returns `{"kind": "Status", "code": 404, ...}` | The dynamic client requests with `_preload_content=False`, and the REST layer only checks the HTTP status when that flag is true. It never raises. `raise_for_status()` in `k8s/client.py` is what catches it — make sure the call went through `KubeClient`. |
| Discovery yields plain strings instead of resources | `LazyDiscoverer.__aiter__` iterates a dict without `.values()`. Don't iterate the discoverer; read the discovery endpoints (`k8s/discovery.py`). |
| Exec loses the exit code, or stderr shows up in stdout | `WsApiClient` with `_preload_content=True` merges channels and drops channel 3. `k8s/exec.py` drives the socket itself. |
| `406 Not Acceptable` on a subresource | The apiserver negotiates `Accept` against its own media types. Send `*/*`, as kubectl does. |

## See the actual request

The fake apiserver records every request, which is usually faster than reading
client source:

```python
# in a test
listing = [r for r in cluster.requests if r["path"].endswith("/pods")][-1]
print(listing["method"], listing["path"], listing["query"])
```

Against a real cluster, compare with what kubectl sends:

```bash
kubectl get pods -v=8 2>&1 | grep -E "GET|POST|PATCH|Request Headers|Response Status"
kubectl logs <pod> -v=8 2>&1 | grep -E "GET|Accept"
```

A mismatch in the query string, the `Content-Type`, or the `Accept` header is
the usual answer.

## Reproduce it in isolation

Drive the client directly, without MCP in the way, against the real cluster:

```python
import asyncio
from kubemcp.config import Settings
from kubemcp.k8s.client import KubeClient, to_dict


async def main():
    kube = KubeClient(Settings(_env_file=None))
    await kube.connect()
    resource = await kube.resource_for("v1", "Pod")
    obj = await kube.get(resource, action="probe", namespace="kube-system", limit=1)
    print(to_dict(obj))
    await kube.close()


asyncio.run(main())
```

If that works and the tool does not, the bug is in the tool layer (shaping,
parameters, namespace resolution). If it fails the same way, it is in the client
or the request shape.

## Then make it a test

A bug that only a live cluster can catch will come back. Once reproduced, teach
`tests/fake_apiserver.py` the real behaviour — including the refusal — and add
an integration test.

Rules for the fake:

- **Reproduce refusals, not just successes.** It answered pod logs happily while
  the real apiserver returned 406; the fake now checks the `Accept` header. A
  fake that is more permissive than production hides exactly the bugs worth
  catching.
- Return real `Status` bodies for errors (`_status(message, reason, code)`), so
  the error-mapping layer is exercised on the real shape.
- Use `cluster.fail(...)` / `cluster.fail_forbidden(...)` to inject a failure
  for one method and resource rather than adding a special-case route.

## Checking against a live cluster

```bash
uv run pytest -m e2e                      # needs minikube/kind running
minikube start                            # if none is up
```

The e2e suite creates a throwaway namespace and deletes it afterwards. If you
add a test there, keep that property — and keep anything environment-dependent
(log retention, scheduling timing) out of the assertions, or assert it loosely.
