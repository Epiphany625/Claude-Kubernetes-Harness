"""Input validation.

This server deliberately performs no agent-authorization checks, which makes
validation the only guard it applies to its own inputs. So it is total: every
value that reaches the Kubernetes API is checked here first, against the same
rules the apiserver itself enforces.

Two properties worth keeping as this file grows:

* Failures raise :class:`ValidationError` with a message naming the offending
  value and the rule it broke. The caller is a language model, and a message it
  can act on ("namespace 'Foo' must be lowercase") is worth far more than
  "invalid input".
* Selectors are *parsed*, not pattern-matched. A selector is interpolated into
  an apiserver query string, so accepting only structurally valid selectors is
  what keeps malformed or injected clauses out of that string.
"""

from __future__ import annotations

import re
from typing import Any, Final

from mcp.server.mcpserver.exceptions import ToolError

__all__ = [
    "ValidationError",
    "validate_api_version",
    "validate_command",
    "validate_container_name",
    "validate_field_selector",
    "validate_generate_name",
    "validate_json_patch",
    "validate_kind",
    "validate_label_selector",
    "validate_manifest",
    "validate_name",
    "validate_namespace",
]


class ValidationError(ValueError, ToolError):
    """An input failed validation. The message is safe to show the caller.

    The two base classes are both load-bearing, and the combination is what
    makes validation impossible to get wrong at a call site:

    * ``ValueError`` is what pydantic expects a validator to raise, so these
      surface as ordinary argument-validation failures when raised from the
      ``Annotated`` types in :mod:`kubemcp.models`.
    * ``ToolError`` is what the MCP SDK forwards to the caller verbatim.
      Anything else raised inside a tool body is treated as a crash and masked
      to "Error executing tool <name>" -- which would throw away the very
      message that tells the model how to fix its call.

    Without the second base, every tool would have to remember to catch and
    re-raise. With it, a validation failure reaches the caller intact wherever
    it is raised.
    """


# -- Primitives --------------------------------------------------------------
# Mirrors k8s.io/apimachinery/pkg/util/validation.

_DNS1123_LABEL: Final = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_DNS1123_SUBDOMAIN: Final = re.compile(
    r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"
)
_LABEL_KEY_NAME: Final = re.compile(r"^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$")
_LABEL_VALUE: Final = re.compile(r"^([A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?)?$")
_KIND: Final = re.compile(r"^[A-Z][A-Za-z0-9]*$")
_API_GROUP: Final = _DNS1123_SUBDOMAIN
_API_VERSION_PART: Final = re.compile(r"^v[0-9]+((alpha|beta)[0-9]+)?$")

MAX_SUBDOMAIN_LEN: Final = 253
MAX_LABEL_LEN: Final = 63


def _require_str(value: object, what: str) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{what} must be a string, got {type(value).__name__}")
    if "\x00" in value:
        raise ValidationError(f"{what} must not contain NUL bytes")
    return value


def validate_name(value: object, *, what: str = "name") -> str:
    """Validate an object name (RFC 1123 subdomain, <=253 chars)."""
    name = _require_str(value, what)
    if not name:
        raise ValidationError(f"{what} must not be empty")
    if len(name) > MAX_SUBDOMAIN_LEN:
        raise ValidationError(
            f"{what} must be at most {MAX_SUBDOMAIN_LEN} characters, got {len(name)}"
        )
    if not _DNS1123_SUBDOMAIN.match(name):
        raise ValidationError(
            f"{what} {name!r} is not a valid RFC 1123 subdomain: it must be lowercase "
            "alphanumeric, '-' or '.', and must start and end with an alphanumeric character"
        )
    return name


_GENERATE_NAME_PREFIX: Final = re.compile(r"^[a-z0-9]([-a-z0-9.]*)?$")
# The apiserver appends 5 random characters to the prefix, and the result must
# still be a valid subdomain.
MAX_GENERATE_NAME_LEN: Final = MAX_SUBDOMAIN_LEN - 5


def validate_generate_name(value: object, *, what: str = "metadata.generateName") -> str:
    """Validate a ``generateName`` *prefix*.

    A prefix is not a complete name: ``job-`` is valid even though a finished
    name may not end in a dash, because the apiserver appends a random suffix.
    """
    prefix = _require_str(value, what)
    if not prefix:
        raise ValidationError(f"{what} must not be empty")
    if len(prefix) > MAX_GENERATE_NAME_LEN:
        raise ValidationError(
            f"{what} must be at most {MAX_GENERATE_NAME_LEN} characters (the apiserver "
            f"appends a 5-character suffix), got {len(prefix)}"
        )
    if not _GENERATE_NAME_PREFIX.match(prefix):
        raise ValidationError(
            f"{what} {prefix!r} is not a valid name prefix: it must be lowercase "
            "alphanumeric, '-' or '.', and must start with an alphanumeric character"
        )
    return prefix


def validate_namespace(value: object, *, what: str = "namespace") -> str:
    """Validate a namespace (RFC 1123 label, <=63 chars)."""
    ns = _require_str(value, what)
    if not ns:
        raise ValidationError(f"{what} must not be empty")
    if len(ns) > MAX_LABEL_LEN:
        raise ValidationError(f"{what} must be at most {MAX_LABEL_LEN} characters, got {len(ns)}")
    if not _DNS1123_LABEL.match(ns):
        raise ValidationError(
            f"{what} {ns!r} is not a valid RFC 1123 label: it must be lowercase alphanumeric "
            "or '-', and must start and end with an alphanumeric character"
        )
    return ns


def validate_container_name(value: object) -> str:
    """Validate a container name (RFC 1123 label)."""
    return validate_namespace(value, what="container")


def validate_kind(value: object) -> str:
    """Validate a Kind: PascalCase, as the apiserver reports it."""
    kind = _require_str(value, "kind")
    if not kind:
        raise ValidationError("kind must not be empty")
    if len(kind) > MAX_LABEL_LEN:
        raise ValidationError(f"kind must be at most {MAX_LABEL_LEN} characters, got {len(kind)}")
    if not _KIND.match(kind):
        raise ValidationError(
            f"kind {kind!r} is not valid: it must start with an uppercase letter and contain "
            "only letters and digits (for example 'Pod', 'StatefulSet')"
        )
    return kind


def validate_api_version(value: object) -> str:
    """Validate an apiVersion: ``v1`` (core) or ``group/version``.

    Both halves are checked, so ``apps/v1``, ``ckh.io/v1alpha1`` and ``v1`` pass
    while ``apps/`` , ``/v1`` and ``apps/v1/extra`` do not.
    """
    api_version = _require_str(value, "api_version")
    if not api_version:
        raise ValidationError("api_version must not be empty")
    if len(api_version) > MAX_SUBDOMAIN_LEN:
        raise ValidationError(
            f"api_version must be at most {MAX_SUBDOMAIN_LEN} characters, got {len(api_version)}"
        )

    parts = api_version.split("/")
    if len(parts) == 1:
        version = parts[0]
        group = ""
    elif len(parts) == 2:
        group, version = parts
        if not group:
            raise ValidationError(
                f"api_version {api_version!r} has an empty group; use 'v1' for core resources"
            )
        if not _API_GROUP.match(group) or len(group) > MAX_SUBDOMAIN_LEN:
            raise ValidationError(
                f"api_version {api_version!r} has an invalid group {group!r}: the group must be "
                "a lowercase DNS subdomain (for example 'apps' or 'ckh.io')"
            )
    else:
        raise ValidationError(
            f"api_version {api_version!r} must be 'version' or 'group/version', not "
            f"{len(parts)} '/'-separated parts"
        )

    if not _API_VERSION_PART.match(version):
        raise ValidationError(
            f"api_version {api_version!r} has an invalid version {version!r}: expected a form "
            "like 'v1', 'v1beta1' or 'v1alpha1'"
        )
    return api_version


# -- Label selectors ---------------------------------------------------------


def _validate_label_key(key: str, *, selector: str) -> None:
    if not key:
        raise ValidationError(f"label selector {selector!r} contains an empty key")
    prefix, _, name = key.rpartition("/")
    if _ and not prefix:
        raise ValidationError(
            f"label selector {selector!r} has a key with an empty prefix: {key!r}"
        )
    if prefix:
        if len(prefix) > MAX_SUBDOMAIN_LEN or not _DNS1123_SUBDOMAIN.match(prefix):
            raise ValidationError(
                f"label selector {selector!r} has an invalid key prefix {prefix!r}: it must be a "
                "lowercase DNS subdomain"
            )
    if not name or len(name) > MAX_LABEL_LEN or not _LABEL_KEY_NAME.match(name):
        raise ValidationError(
            f"label selector {selector!r} has an invalid key name {name!r}: it must be at "
            f"most {MAX_LABEL_LEN} characters of alphanumerics, '-', '_' or '.', starting "
            "and ending with an alphanumeric character"
        )


def _validate_label_value(value: str, *, selector: str) -> None:
    if len(value) > MAX_LABEL_LEN:
        raise ValidationError(
            f"label selector {selector!r} has a value longer than "
            f"{MAX_LABEL_LEN} characters: {value!r}"
        )
    if not _LABEL_VALUE.match(value):
        raise ValidationError(
            f"label selector {selector!r} has an invalid value {value!r}: it must be "
            "alphanumerics, '-', '_' or '.', starting and ending with an alphanumeric "
            "character"
        )


def _split_top_level(selector: str) -> list[str]:
    """Split on commas that are not inside ``( ... )``.

    ``in``/``notin`` clauses contain commas of their own, so a naive split
    corrupts them.
    """
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for char in selector:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth < 0:
                raise ValidationError(f"label selector {selector!r} has an unbalanced ')'")
        if char == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    if depth != 0:
        raise ValidationError(f"label selector {selector!r} has an unbalanced '('")
    parts.append("".join(current))
    return parts


_SET_CLAUSE: Final = re.compile(r"^(?P<key>[^\s!=(),]+)\s+(?P<op>in|notin)\s*\((?P<values>.*)\)$")


def validate_label_selector(value: object) -> str:
    """Validate a label selector, returning it unchanged.

    Supports the full documented grammar: equality (``k=v``, ``k==v``, ``k!=v``),
    set membership (``k in (a,b)``, ``k notin (a,b)``) and existence (``k``,
    ``!k``), comma-separated.
    """
    selector = _require_str(value, "label_selector")
    if not selector.strip():
        raise ValidationError("label_selector must not be blank; omit it instead")

    for raw_clause in _split_top_level(selector):
        clause = raw_clause.strip()
        if not clause:
            raise ValidationError(f"label selector {selector!r} contains an empty clause")

        if set_match := _SET_CLAUSE.match(clause):
            _validate_label_key(set_match.group("key").strip(), selector=selector)
            values = set_match.group("values")
            if not values.strip():
                raise ValidationError(
                    f"label selector {selector!r} has an empty value list in {clause!r}"
                )
            for item in values.split(","):
                _validate_label_value(item.strip(), selector=selector)
            continue

        if "!=" in clause:
            key, _, val = clause.partition("!=")
        elif "==" in clause:
            key, _, val = clause.partition("==")
        elif "=" in clause:
            key, _, val = clause.partition("=")
        elif clause.startswith("!"):
            _validate_label_key(clause[1:].strip(), selector=selector)
            continue
        else:
            # Bare existence check. Reject stray operators that got this far.
            if any(token in clause for token in ("(", ")", " ")):
                raise ValidationError(
                    f"label selector {selector!r} has a malformed clause {clause!r}"
                )
            _validate_label_key(clause, selector=selector)
            continue

        _validate_label_key(key.strip(), selector=selector)
        _validate_label_value(val.strip(), selector=selector)

    return selector


_FIELD_PATH: Final = re.compile(r"^[A-Za-z0-9_.\[\]'\"-]+$")


def validate_field_selector(value: object) -> str:
    """Validate a field selector (``k=v``, ``k==v``, ``k!=v``, comma-separated)."""
    selector = _require_str(value, "field_selector")
    if not selector.strip():
        raise ValidationError("field_selector must not be blank; omit it instead")

    for raw_clause in selector.split(","):
        clause = raw_clause.strip()
        if not clause:
            raise ValidationError(f"field selector {selector!r} contains an empty clause")
        if "!=" in clause:
            key, _, val = clause.partition("!=")
        elif "==" in clause:
            key, _, val = clause.partition("==")
        elif "=" in clause:
            key, _, val = clause.partition("=")
        else:
            raise ValidationError(
                f"field selector {selector!r} has a clause without an operator: {clause!r}. "
                "Field selectors require '=', '==' or '!=' (existence checks are not supported)."
            )
        key = key.strip()
        if not key or not _FIELD_PATH.match(key):
            raise ValidationError(f"field selector {selector!r} has an invalid field path {key!r}")
        if "\n" in val or "," in val:
            raise ValidationError(
                f"field selector {selector!r} has an invalid value {val.strip()!r}"
            )
    return selector


# -- Manifests and patches ---------------------------------------------------


def validate_manifest(
    manifest: object,
    *,
    namespace: str | None = None,
    require_name: bool = True,
) -> dict[str, Any]:
    """Validate a resource manifest and reconcile it with an explicit namespace.

    A mismatch between ``metadata.namespace`` and the ``namespace`` argument is
    rejected rather than silently resolved: guessing which one the caller meant
    is how an object lands in the wrong namespace.
    """
    if not isinstance(manifest, dict):
        raise ValidationError(f"manifest must be a JSON object, got {type(manifest).__name__}")

    api_version = manifest.get("apiVersion")
    if api_version is None:
        raise ValidationError("manifest is missing required field 'apiVersion'")
    validate_api_version(api_version)

    kind = manifest.get("kind")
    if kind is None:
        raise ValidationError("manifest is missing required field 'kind'")
    validate_kind(kind)

    metadata = manifest.get("metadata", {})
    if not isinstance(metadata, dict):
        raise ValidationError(
            f"manifest 'metadata' must be an object, got {type(metadata).__name__}"
        )

    name = metadata.get("name")
    generate_name = metadata.get("generateName")
    if name is not None:
        validate_name(name, what="metadata.name")
    elif generate_name is not None:
        validate_generate_name(generate_name)
    elif require_name:
        raise ValidationError(
            "manifest requires 'metadata.name' (or 'metadata.generateName' when creating)"
        )

    body_ns = metadata.get("namespace")
    if body_ns is not None:
        validate_namespace(body_ns, what="metadata.namespace")
    if namespace is not None:
        validate_namespace(namespace)
        if body_ns is not None and body_ns != namespace:
            raise ValidationError(
                f"namespace conflict: manifest declares metadata.namespace={body_ns!r} but the "
                f"call passed namespace={namespace!r}. Remove one of them."
            )

    # Server-managed; echoing it back causes spurious apply conflicts.
    if "managedFields" in metadata:
        raise ValidationError(
            "manifest must not set 'metadata.managedFields'; it is managed by the apiserver"
        )

    return manifest


_JSON_PATCH_OPS: Final = frozenset({"add", "remove", "replace", "move", "copy", "test"})


def validate_json_patch(patch: object) -> list[dict[str, Any]]:
    """Validate an RFC 6902 JSON Patch document."""
    if not isinstance(patch, list):
        raise ValidationError(
            f"a json patch must be an array of operations, got {type(patch).__name__}"
        )
    if not patch:
        raise ValidationError("a json patch must contain at least one operation")

    for index, op in enumerate(patch):
        where = f"patch[{index}]"
        if not isinstance(op, dict):
            raise ValidationError(f"{where} must be an object, got {type(op).__name__}")
        operation = op.get("op")
        if operation not in _JSON_PATCH_OPS:
            raise ValidationError(
                f"{where} has an invalid 'op': {operation!r}. "
                f"Expected one of {sorted(_JSON_PATCH_OPS)}."
            )
        path = op.get("path")
        if not isinstance(path, str) or not path.startswith("/"):
            raise ValidationError(f"{where} requires a 'path' string starting with '/'")
        if operation in {"add", "replace", "test"} and "value" not in op:
            raise ValidationError(f"{where} with op {operation!r} requires a 'value'")
        if operation in {"move", "copy"}:
            source = op.get("from")
            if not isinstance(source, str) or not source.startswith("/"):
                raise ValidationError(
                    f"{where} with op {operation!r} requires a 'from' string starting with '/'"
                )
    return patch


def validate_merge_patch(patch: object) -> dict[str, Any]:
    """Validate a strategic-merge or JSON-merge patch body."""
    if not isinstance(patch, dict):
        raise ValidationError(
            f"a merge patch must be a JSON object, got {type(patch).__name__}. "
            "Use patch_type='json' for an RFC 6902 array."
        )
    if not patch:
        raise ValidationError("a merge patch must not be empty")
    return patch


# -- Exec --------------------------------------------------------------------

MAX_COMMAND_ARGS: Final = 64
MAX_COMMAND_ARG_LEN: Final = 4_096


def validate_command(value: object) -> list[str]:
    """Validate an exec argv.

    The command is an argv *list*, never a shell string: it is handed to the
    apiserver's exec subresource as separate arguments and no shell ever
    interprets it, so quoting and metacharacters carry no special meaning. A
    caller wanting shell semantics must ask for them explicitly, e.g.
    ``["sh", "-c", "..."]``.
    """
    if isinstance(value, str):
        raise ValidationError(
            "command must be a list of arguments, not a string. "
            'For a shell pipeline pass ["sh", "-c", "<script>"] explicitly.'
        )
    if not isinstance(value, list):
        raise ValidationError(f"command must be a list of strings, got {type(value).__name__}")
    if not value:
        raise ValidationError("command must not be empty")
    if len(value) > MAX_COMMAND_ARGS:
        raise ValidationError(
            f"command has {len(value)} arguments, exceeding the limit of {MAX_COMMAND_ARGS}"
        )

    argv: list[str] = []
    for index, arg in enumerate(value):
        if not isinstance(arg, str):
            raise ValidationError(f"command[{index}] must be a string, got {type(arg).__name__}")
        if "\x00" in arg:
            raise ValidationError(f"command[{index}] must not contain NUL bytes")
        if len(arg) > MAX_COMMAND_ARG_LEN:
            raise ValidationError(
                f"command[{index}] is {len(arg)} characters, exceeding the limit of "
                f"{MAX_COMMAND_ARG_LEN}"
            )
        argv.append(arg)

    if not argv[0]:
        raise ValidationError("command[0] (the executable) must not be empty")
    return argv
