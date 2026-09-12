"""A small but real Kubernetes apiserver, for tests.

This serves genuine HTTP on a loopback port rather than monkey-patching the
client, so the integration tests exercise the real ``kubernetes_asyncio`` stack:
discovery, path construction, query-parameter encoding, serialization and error
handling all run as they would against a cluster. What is faked is only the
cluster's *contents*.

It also records every request, which lets a test assert on what the apiserver
actually received -- that a label selector arrived intact, say, rather than
merely that the call returned something.
"""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from typing import Any

from aiohttp import web


@dataclass(slots=True)
class FakeResource:
    """One resource type the fake cluster serves."""

    kind: str
    plural: str
    namespaced: bool = True
    group: str = ""
    version: str = "v1"
    verbs: list[str] = field(
        default_factory=lambda: ["get", "list", "create", "update", "patch", "delete"]
    )
    short_names: list[str] = field(default_factory=list)
    subresources: list[str] = field(default_factory=list)

    @property
    def group_version(self) -> str:
        return f"{self.group}/{self.version}" if self.group else self.version

    @property
    def prefix(self) -> str:
        return "/apis" if self.group else "/api"


DEFAULT_RESOURCES = [
    FakeResource("Pod", "pods", short_names=["po"], subresources=["log", "exec", "status"]),
    FakeResource("Service", "services", short_names=["svc"]),
    FakeResource("Event", "events", short_names=["ev"]),
    FakeResource("ConfigMap", "configmaps", short_names=["cm"]),
    FakeResource("Node", "nodes", namespaced=False, short_names=["no"]),
    FakeResource("Namespace", "namespaces", namespaced=False, short_names=["ns"]),
    FakeResource(
        "Deployment",
        "deployments",
        group="apps",
        short_names=["deploy"],
        subresources=["scale", "status"],
    ),
    FakeResource(
        "StatefulSet",
        "statefulsets",
        group="apps",
        short_names=["sts"],
        subresources=["scale", "status"],
    ),
    # A custom resource, so tests can prove the generic tools reach a CRD the
    # server has never heard of -- the property the CKH type depends on.
    FakeResource(
        "ClusterHarness",
        "clusterharnesses",
        group="ckh.io",
        version="v1alpha1",
        short_names=["ckh"],
        subresources=["status"],
    ),
]


def _status(message: str, reason: str, code: int, **details: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "kind": "Status",
        "apiVersion": "v1",
        "metadata": {},
        "status": "Failure",
        "message": message,
        "reason": reason,
        "code": code,
    }
    if details:
        body["details"] = details
    return body


class FakeCluster:
    """An in-memory cluster with an HTTP front end."""

    def __init__(self, resources: list[FakeResource] | None = None) -> None:
        self.resources = list(resources if resources is not None else DEFAULT_RESOURCES)
        self.objects: dict[tuple[str, str, str | None, str], dict[str, Any]] = {}
        self.requests: list[dict[str, Any]] = []
        # (method, plural) -> (status, Status body). Lets a test force a failure.
        self.failures: dict[tuple[str, str], tuple[int, dict[str, Any]]] = {}
        self.logs: dict[tuple[str, str, str | None, bool], str] = {}
        # (namespace, pod) -> scripted exec outcome.
        self.execs: dict[tuple[str, str], dict[str, Any]] = {}
        self._version = 1

    # -- seeding ------------------------------------------------------------

    def add(self, obj: dict[str, Any]) -> dict[str, Any]:
        """Insert an object, filling in the bookkeeping a real apiserver adds."""
        obj = copy.deepcopy(obj)
        metadata = obj.setdefault("metadata", {})
        metadata.setdefault("uid", f"uid-{len(self.objects)}")
        metadata.setdefault("creationTimestamp", "2026-01-01T00:00:00Z")
        metadata.setdefault("resourceVersion", str(self._next_version()))
        # Real objects always carry managedFields; tests rely on it being here
        # so they can prove the server strips it.
        metadata.setdefault("managedFields", [{"manager": "kubelet", "operation": "Update"}])
        self.objects[self._key(obj)] = obj
        return obj

    def add_log(
        self,
        namespace: str,
        pod: str,
        text: str,
        *,
        container: str | None = None,
        previous: bool = False,
    ) -> None:
        self.logs[(namespace, pod, container, previous)] = text

    def add_exec(
        self,
        namespace: str,
        pod: str,
        *,
        stdout: str = "",
        stderr: str = "",
        exit_code: int = 0,
        hang: bool = False,
    ) -> None:
        """Script the outcome of an exec against this pod."""
        self.execs[(namespace, pod)] = {
            "stdout": stdout,
            "stderr": stderr,
            "exit_code": exit_code,
            "hang": hang,
        }

    def fail(self, method: str, plural: str, status: int, body: dict[str, Any]) -> None:
        """Make the next requests of this shape fail."""
        self.failures[(method.upper(), plural)] = (status, body)

    def fail_forbidden(self, method: str, plural: str, *, group: str = "") -> None:
        self.fail(
            method,
            plural,
            403,
            _status(
                f'{plural} is forbidden: User "system:serviceaccount:ckh:kubemcp" cannot '
                f'{method.lower()} resource "{plural}" in API group "{group}"',
                "Forbidden",
                403,
                kind=plural,
                group=group,
            ),
        )

    def _next_version(self) -> int:
        self._version += 1
        return self._version

    def _key(self, obj: dict[str, Any]) -> tuple[str, str, str | None, str]:
        metadata = obj.get("metadata") or {}
        return (
            obj["apiVersion"],
            obj["kind"],
            metadata.get("namespace"),
            metadata["name"],
        )

    def _resource(self, plural: str, group: str, version: str) -> FakeResource | None:
        for resource in self.resources:
            if (
                resource.plural == plural
                and resource.group == group
                and resource.version == version
            ):
                return resource
        return None

    # -- HTTP ---------------------------------------------------------------

    def build_app(self) -> web.Application:
        app = web.Application()
        app.router.add_get("/version", self._version_handler)
        app.router.add_get("/api", self._api_root)
        app.router.add_get("/apis", self._apis_root)
        app.router.add_get("/readyz", self._readyz)
        app.router.add_get("/api/v1", self._resource_list_core)
        app.router.add_get("/apis/{group}/{version}", self._resource_list_group)

        # Core group.
        app.router.add_route("*", "/api/v1/namespaces/{namespace}/{plural}", self._collection)
        app.router.add_route("*", "/api/v1/namespaces/{namespace}/{plural}/{name}", self._single)
        app.router.add_route(
            "*", "/api/v1/namespaces/{namespace}/{plural}/{name}/{subresource}", self._sub
        )
        app.router.add_route("*", "/api/v1/{plural}", self._collection)
        app.router.add_route("*", "/api/v1/{plural}/{name}", self._single)

        # Named groups.
        app.router.add_route(
            "*", "/apis/{group}/{version}/namespaces/{namespace}/{plural}", self._collection
        )
        app.router.add_route(
            "*",
            "/apis/{group}/{version}/namespaces/{namespace}/{plural}/{name}",
            self._single,
        )
        app.router.add_route(
            "*",
            "/apis/{group}/{version}/namespaces/{namespace}/{plural}/{name}/{subresource}",
            self._sub,
        )
        app.router.add_route("*", "/apis/{group}/{version}/{plural}", self._collection)
        app.router.add_route("*", "/apis/{group}/{version}/{plural}/{name}", self._single)
        app.middlewares.append(self._record)
        return app

    @web.middleware
    async def _record(self, request: web.Request, handler: Any) -> web.StreamResponse:
        self.requests.append(
            {
                "method": request.method,
                "path": request.path,
                "query": dict(request.query),
            }
        )
        return await handler(request)

    async def _readyz(self, _: web.Request) -> web.Response:
        return web.Response(text="ok")

    async def _exec(self, request: web.Request, namespace: str, pod: str) -> web.StreamResponse:
        """Serve the exec subresource over the SPDY-style multiplexed websocket.

        Frames are ``bytes([channel]) + payload``: 1 is stdout, 2 stderr, 3 a
        terminal ``Status`` carrying the exit code. Reproducing that here is
        what makes the client's channel demuxing and exit-code parsing testable.
        """
        websocket = web.WebSocketResponse(protocols=("v4.channel.k8s.io",))
        await websocket.prepare(request)

        # The client sends argv as repeated ?command= parameters.
        self.requests[-1]["command"] = request.query.getall("command", [])

        script = self.execs.get((namespace, pod))
        if script is None:
            await websocket.send_bytes(
                b"\x03" + json.dumps(_status(f'pod "{pod}" not found', "NotFound", 404)).encode()
            )
            await websocket.close()
            return websocket

        if script["hang"]:
            # Never send the terminal frame, so the client's timeout is what
            # ends the call.
            await websocket.send_bytes(b"\x01" + b"partial output before hanging\n")
            try:
                await websocket.receive()
            except Exception:
                pass
            return websocket

        if script["stdout"]:
            await websocket.send_bytes(b"\x01" + script["stdout"].encode())
        if script["stderr"]:
            await websocket.send_bytes(b"\x02" + script["stderr"].encode())

        exit_code = script["exit_code"]
        if exit_code == 0:
            status: dict[str, Any] = {"metadata": {}, "status": "Success"}
        else:
            status = {
                "metadata": {},
                "status": "Failure",
                "message": "command terminated with non-zero exit code",
                "reason": "NonZeroExitCode",
                "details": {"causes": [{"reason": "ExitCode", "message": str(exit_code)}]},
            }
        await websocket.send_bytes(b"\x03" + json.dumps(status).encode())
        await websocket.close()
        return websocket

    async def _version_handler(self, _: web.Request) -> web.Response:
        return web.json_response(
            {"major": "1", "minor": "31", "gitVersion": "v1.31.0", "platform": "linux/amd64"}
        )

    async def _api_root(self, _: web.Request) -> web.Response:
        return web.json_response({"kind": "APIVersions", "versions": ["v1"]})

    async def _apis_root(self, _: web.Request) -> web.Response:
        groups: dict[str, dict[str, Any]] = {}
        for resource in self.resources:
            if not resource.group:
                continue
            entry = groups.setdefault(
                resource.group,
                {"name": resource.group, "versions": [], "preferredVersion": None},
            )
            version = {
                "groupVersion": resource.group_version,
                "version": resource.version,
            }
            if version not in entry["versions"]:
                entry["versions"].append(version)
                entry["preferredVersion"] = version
        return web.json_response(
            {"kind": "APIGroupList", "apiVersion": "v1", "groups": list(groups.values())}
        )

    def _resource_payload(self, group: str, version: str) -> dict[str, Any]:
        group_version = f"{group}/{version}" if group else version
        entries = []
        for resource in self.resources:
            if resource.group != group or resource.version != version:
                continue
            entries.append(
                {
                    "name": resource.plural,
                    "singularName": resource.kind.lower(),
                    "namespaced": resource.namespaced,
                    "kind": resource.kind,
                    "verbs": resource.verbs,
                    "shortNames": resource.short_names,
                }
            )
            for sub in resource.subresources:
                entries.append(
                    {
                        "name": f"{resource.plural}/{sub}",
                        "singularName": "",
                        "namespaced": resource.namespaced,
                        "kind": "Scale" if sub == "scale" else resource.kind,
                        "verbs": ["get", "patch", "update"],
                    }
                )
        return {
            "kind": "APIResourceList",
            "apiVersion": "v1",
            "groupVersion": group_version,
            "resources": entries,
        }

    async def _resource_list_core(self, _: web.Request) -> web.Response:
        return web.json_response(self._resource_payload("", "v1"))

    async def _resource_list_group(self, request: web.Request) -> web.Response:
        return web.json_response(
            self._resource_payload(request.match_info["group"], request.match_info["version"])
        )

    def _check_failure(self, request: web.Request, plural: str) -> web.Response | None:
        injected = self.failures.get((request.method, plural))
        if injected is None:
            return None
        status, body = injected
        return web.json_response(body, status=status)

    async def _collection(self, request: web.Request) -> web.Response:
        plural = request.match_info["plural"]
        group = request.match_info.get("group", "")
        version = request.match_info.get("version", "v1")
        namespace = request.match_info.get("namespace")

        if (failure := self._check_failure(request, plural)) is not None:
            return failure

        resource = self._resource(plural, group, version)
        if resource is None:
            return web.json_response(
                _status(
                    f"the server could not find the requested resource ({plural})", "NotFound", 404
                ),
                status=404,
            )

        if request.method == "GET":
            return web.json_response(self._list(resource, namespace, request))
        if request.method == "POST":
            body = await request.json()
            key = (
                body.get("apiVersion", resource.group_version),
                body.get("kind", resource.kind),
                namespace,
                (body.get("metadata") or {}).get("name"),
            )
            if key in self.objects:
                return web.json_response(
                    _status(
                        f'{plural} "{key[3]}" already exists',
                        "AlreadyExists",
                        409,
                        kind=plural,
                        name=key[3],
                    ),
                    status=409,
                )
            if namespace:
                body.setdefault("metadata", {})["namespace"] = namespace
            if request.query.get("dryRun"):
                return web.json_response(body)
            return web.json_response(self.add(body), status=201)
        return web.json_response(_status("method not allowed", "MethodNotAllowed", 405), status=405)

    def _list(
        self, resource: FakeResource, namespace: str | None, request: web.Request
    ) -> dict[str, Any]:
        items = [
            obj
            for (api_version, kind, ns, _), obj in self.objects.items()
            if kind == resource.kind
            and api_version == resource.group_version
            and (namespace is None or ns == namespace)
        ]

        # Honour the selectors well enough that a test can tell they arrived.
        if selector := request.query.get("labelSelector"):
            items = [obj for obj in items if _matches_labels(obj, selector)]
        if field_selector := request.query.get("fieldSelector"):
            items = [obj for obj in items if _matches_fields(obj, field_selector)]

        metadata: dict[str, Any] = {"resourceVersion": str(self._version)}
        if limit := request.query.get("limit"):
            capped = int(limit)
            if len(items) > capped:
                items = items[:capped]
                metadata["continue"] = "fake-continue-token"

        return {
            "apiVersion": resource.group_version,
            "kind": f"{resource.kind}List",
            "metadata": metadata,
            "items": items,
        }

    async def _single(self, request: web.Request) -> web.Response:
        plural = request.match_info["plural"]
        name = request.match_info["name"]
        group = request.match_info.get("group", "")
        version = request.match_info.get("version", "v1")
        namespace = request.match_info.get("namespace")

        if (failure := self._check_failure(request, plural)) is not None:
            return failure

        resource = self._resource(plural, group, version)
        if resource is None:
            return web.json_response(
                _status(
                    f"the server could not find the requested resource ({plural})", "NotFound", 404
                ),
                status=404,
            )

        key = (resource.group_version, resource.kind, namespace, name)
        existing = self.objects.get(key)

        if request.method == "GET":
            if existing is None:
                return web.json_response(
                    _status(
                        f'{plural} "{name}" not found', "NotFound", 404, kind=plural, name=name
                    ),
                    status=404,
                )
            return web.json_response(existing)

        if request.method == "PATCH":
            body = await request.json()
            content_type = request.headers.get("Content-Type", "")
            if existing is None and "apply-patch" not in content_type:
                return web.json_response(
                    _status(
                        f'{plural} "{name}" not found', "NotFound", 404, kind=plural, name=name
                    ),
                    status=404,
                )
            target = (
                copy.deepcopy(existing)
                if existing
                else {
                    "apiVersion": resource.group_version,
                    "kind": resource.kind,
                    "metadata": {"name": name, **({"namespace": namespace} if namespace else {})},
                }
            )
            if isinstance(body, list):
                _apply_json_patch(target, body)
            else:
                _deep_merge(target, body)
            target.setdefault("metadata", {})["resourceVersion"] = str(self._next_version())
            if request.query.get("dryRun"):
                return web.json_response(target)
            self.objects[key] = target
            return web.json_response(target)

        if request.method == "DELETE":
            if existing is None:
                return web.json_response(
                    _status(
                        f'{plural} "{name}" not found', "NotFound", 404, kind=plural, name=name
                    ),
                    status=404,
                )
            if not request.query.get("dryRun"):
                del self.objects[key]
            return web.json_response(
                {"kind": "Status", "apiVersion": "v1", "status": "Success", "code": 200}
            )

        return web.json_response(_status("method not allowed", "MethodNotAllowed", 405), status=405)

    async def _sub(self, request: web.Request) -> web.Response:
        sub = request.match_info["subresource"]
        name = request.match_info["name"]
        namespace = request.match_info.get("namespace")
        plural = request.match_info["plural"]

        if (failure := self._check_failure(request, f"{plural}/{sub}")) is not None:
            return failure

        if sub == "log":
            # The real apiserver negotiates Accept and answers 406 to a bare
            # "text/plain". Reproducing that here keeps the fake honest: an
            # earlier version of the client sent text/plain and only the live
            # e2e run caught it.
            accept = request.headers.get("Accept", "*/*")
            if "text/plain" in accept and "*/*" not in accept:
                return web.json_response(
                    _status(
                        "only the following media types are accepted: application/json, "
                        "application/yaml, application/vnd.kubernetes.protobuf",
                        "NotAcceptable",
                        406,
                    ),
                    status=406,
                )
            container = request.query.get("container")
            previous = request.query.get("previous") == "true"
            text = self.logs.get((namespace or "", name, container, previous))
            if text is None:
                text = self.logs.get((namespace or "", name, None, previous))
            if text is None:
                if previous:
                    return web.json_response(
                        _status(
                            f'previous terminated container "{name}" in pod "{name}" not found',
                            "BadRequest",
                            400,
                        ),
                        status=400,
                    )
                text = ""
            return web.Response(text=text, content_type="text/plain")

        if sub == "exec":
            return await self._exec(request, namespace or "", name)

        if sub == "scale" and request.method == "PATCH":
            group = request.match_info.get("group", "")
            version = request.match_info.get("version", "v1")
            resource = self._resource(plural, group, version)
            if resource is None:
                return web.json_response(_status("not found", "NotFound", 404), status=404)
            key = (resource.group_version, resource.kind, namespace, name)
            existing = self.objects.get(key)
            if existing is None:
                return web.json_response(
                    _status(f'{plural} "{name}" not found', "NotFound", 404), status=404
                )
            body = await request.json()
            replicas = (body.get("spec") or {}).get("replicas")
            existing.setdefault("spec", {})["replicas"] = replicas
            return web.json_response(
                {
                    "apiVersion": "autoscaling/v1",
                    "kind": "Scale",
                    "metadata": {"name": name, "namespace": namespace},
                    "spec": {"replicas": replicas},
                }
            )

        return web.json_response(
            _status(f"subresource {sub} not supported", "NotFound", 404), status=404
        )


# -- helpers -----------------------------------------------------------------


def _matches_labels(obj: dict[str, Any], selector: str) -> bool:
    """Equality and set clauses, enough to make selector tests meaningful."""
    labels = (obj.get("metadata") or {}).get("labels") or {}
    depth, current, clauses = 0, [], []
    for char in selector:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            clauses.append("".join(current))
            current = []
        else:
            current.append(char)
    clauses.append("".join(current))

    for raw in clauses:
        clause = raw.strip()
        if " notin " in clause:
            key, _, values = clause.partition(" notin ")
            allowed = {v.strip() for v in values.strip("() ").split(",")}
            if labels.get(key.strip()) in allowed:
                return False
        elif " in " in clause:
            key, _, values = clause.partition(" in ")
            allowed = {v.strip() for v in values.strip("() ").split(",")}
            if labels.get(key.strip()) not in allowed:
                return False
        elif "!=" in clause:
            key, _, value = clause.partition("!=")
            if labels.get(key.strip()) == value.strip():
                return False
        elif "=" in clause:
            key, _, value = clause.replace("==", "=").partition("=")
            if labels.get(key.strip()) != value.strip():
                return False
        elif clause.startswith("!"):
            if clause[1:].strip() in labels:
                return False
        elif clause and clause not in labels:
            return False
    return True


def _matches_fields(obj: dict[str, Any], selector: str) -> bool:
    for raw in selector.split(","):
        clause = raw.strip()
        negate = "!=" in clause
        key, _, value = clause.partition("!=" if negate else "=")
        key, value = key.strip().lstrip("="), value.strip()
        cursor: Any = obj
        for part in key.split("."):
            cursor = (cursor or {}).get(part) if isinstance(cursor, dict) else None
        matched = str(cursor) == value
        if matched is negate:
            return False
    return True


def _deep_merge(target: dict[str, Any], patch: dict[str, Any]) -> None:
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_merge(target[key], value)
        else:
            target[key] = value


def _apply_json_patch(target: dict[str, Any], operations: list[dict[str, Any]]) -> None:
    for op in operations:
        path = [p.replace("~1", "/").replace("~0", "~") for p in op["path"].split("/")[1:]]
        cursor: Any = target
        for part in path[:-1]:
            cursor = cursor.setdefault(part, {})
        last = path[-1]
        if op["op"] in {"add", "replace"}:
            cursor[last] = op["value"]
        elif op["op"] == "remove":
            cursor.pop(last, None)


def dump(obj: Any) -> str:
    return json.dumps(obj, indent=2, default=str)
