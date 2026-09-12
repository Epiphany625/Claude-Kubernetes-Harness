"""Validation is the only guard this server applies to its own inputs, so these
tests cover both what must be accepted and what must be refused."""

from __future__ import annotations

import pytest

from kubemcp import validation
from kubemcp.validation import ValidationError


class TestNames:
    @pytest.mark.parametrize(
        "name",
        ["a", "web-0", "my.app.example", "x" * 253, "0abc", "a-b-c.d-e"],
    )
    def test_accepts_valid_subdomains(self, name: str) -> None:
        assert validation.validate_name(name) == name

    @pytest.mark.parametrize(
        ("name", "because"),
        [
            ("", "empty"),
            ("Web-0", "uppercase"),
            ("-web", "leading dash"),
            ("web-", "trailing dash"),
            ("web_0", "underscore"),
            ("x" * 254, "too long"),
            ("web 0", "space"),
            ("web/0", "slash"),
            ("web\x00", "NUL byte"),
        ],
    )
    def test_rejects_invalid(self, name: str, because: str) -> None:
        with pytest.raises(ValidationError):
            validation.validate_name(name)

    def test_rejects_non_string(self) -> None:
        with pytest.raises(ValidationError, match="must be a string"):
            validation.validate_name(42)

    def test_message_names_the_value(self) -> None:
        with pytest.raises(ValidationError, match="Web-0"):
            validation.validate_name("Web-0")


class TestNamespaces:
    @pytest.mark.parametrize("ns", ["default", "kube-system", "a", "x" * 63])
    def test_accepts_valid_labels(self, ns: str) -> None:
        assert validation.validate_namespace(ns) == ns

    @pytest.mark.parametrize("ns", ["", "Default", "my.ns", "x" * 64, "-ns", "ns-"])
    def test_rejects_invalid(self, ns: str) -> None:
        with pytest.raises(ValidationError):
            validation.validate_namespace(ns)

    def test_dots_are_rejected_unlike_names(self) -> None:
        # A namespace is a DNS *label*, not a subdomain: dots are not allowed
        # even though they are fine in an object name.
        validation.validate_name("my.app")
        with pytest.raises(ValidationError):
            validation.validate_namespace("my.app")


class TestApiVersion:
    @pytest.mark.parametrize(
        "value", ["v1", "apps/v1", "ckh.io/v1alpha1", "batch/v1beta1", "v2beta3"]
    )
    def test_accepts_valid(self, value: str) -> None:
        assert validation.validate_api_version(value) == value

    @pytest.mark.parametrize(
        ("value", "because"),
        [
            ("", "empty"),
            ("apps/", "empty version"),
            ("/v1", "empty group"),
            ("apps/v1/extra", "too many parts"),
            ("Apps/v1", "uppercase group"),
            ("apps/1", "version without v"),
            ("apps/valpha", "version without a number"),
            ("apps/v1gamma1", "unknown stability channel"),
        ],
    )
    def test_rejects_invalid(self, value: str, because: str) -> None:
        with pytest.raises(ValidationError):
            validation.validate_api_version(value)


class TestKind:
    @pytest.mark.parametrize("kind", ["Pod", "StatefulSet", "ClusterHarness", "V1Thing"])
    def test_accepts_pascal_case(self, kind: str) -> None:
        assert validation.validate_kind(kind) == kind

    @pytest.mark.parametrize("kind", ["", "pod", "Stateful-Set", "Stateful_Set", "1Pod"])
    def test_rejects_invalid(self, kind: str) -> None:
        with pytest.raises(ValidationError):
            validation.validate_kind(kind)


class TestLabelSelector:
    @pytest.mark.parametrize(
        "selector",
        [
            "app=web",
            "app==web",
            "app!=web",
            "app=web,tier=cache",
            "app in (web,api)",
            "app notin (web, api)",
            "app",
            "!deprecated",
            "app.kubernetes.io/name=web",
            "example.com/team=core,app in (a,b),!legacy",
            "app=",  # an empty value is legal and means "label set to empty"
        ],
    )
    def test_accepts_the_documented_grammar(self, selector: str) -> None:
        assert validation.validate_label_selector(selector) == selector

    @pytest.mark.parametrize(
        ("selector", "because"),
        [
            ("", "blank"),
            ("   ", "blank"),
            ("=web", "no key"),
            ("app in (", "unbalanced paren"),
            ("app in )", "unbalanced paren"),
            ("app in ()", "empty value list"),
            ("app=web,", "trailing comma leaves an empty clause"),
            ("app=UPPER CASE", "space in value"),
            ("/app=web", "empty key prefix"),
            ("app=" + "x" * 64, "value too long"),
            ("a" * 64 + "=web", "key name too long"),
        ],
    )
    def test_rejects_malformed(self, selector: str, because: str) -> None:
        with pytest.raises(ValidationError):
            validation.validate_label_selector(selector)

    def test_set_clause_commas_do_not_split_clauses(self) -> None:
        # The comma inside "(web,api)" must not be treated as a clause separator.
        selector = "app in (web,api),tier=cache"
        assert validation.validate_label_selector(selector) == selector


class TestFieldSelector:
    @pytest.mark.parametrize(
        "selector",
        [
            "status.phase=Running",
            "spec.nodeName!=node-1",
            "metadata.name==web-0",
            "status.phase=Running,spec.nodeName=n1",
        ],
    )
    def test_accepts_valid(self, selector: str) -> None:
        assert validation.validate_field_selector(selector) == selector

    @pytest.mark.parametrize("selector", ["", "status.phase", "=Running", "a b=c"])
    def test_rejects_invalid(self, selector: str) -> None:
        with pytest.raises(ValidationError):
            validation.validate_field_selector(selector)

    def test_existence_clauses_are_rejected_with_an_explanation(self) -> None:
        with pytest.raises(ValidationError, match="existence checks are not supported"):
            validation.validate_field_selector("status.phase")


class TestManifest:
    def _manifest(self, **overrides: object) -> dict:
        base = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "cfg", "namespace": "default"},
        }
        base.update(overrides)  # type: ignore[arg-type]
        return base

    def test_accepts_a_well_formed_manifest(self) -> None:
        manifest = self._manifest()
        assert validation.validate_manifest(manifest, namespace="default") is manifest

    def test_requires_api_version_and_kind(self) -> None:
        with pytest.raises(ValidationError, match="apiVersion"):
            validation.validate_manifest({"kind": "ConfigMap", "metadata": {"name": "c"}})
        with pytest.raises(ValidationError, match="kind"):
            validation.validate_manifest({"apiVersion": "v1", "metadata": {"name": "c"}})

    def test_rejects_a_namespace_conflict_rather_than_guessing(self) -> None:
        # Silently preferring one over the other is how an object lands in the
        # wrong namespace, so this must be an error.
        with pytest.raises(ValidationError, match="namespace conflict"):
            validation.validate_manifest(self._manifest(), namespace="production")

    def test_allows_a_matching_namespace(self) -> None:
        validation.validate_manifest(self._manifest(), namespace="default")

    def test_accepts_generate_name_instead_of_name(self) -> None:
        manifest = self._manifest(metadata={"generateName": "job-"})
        validation.validate_manifest(manifest, require_name=False)

    def test_requires_a_name_when_asked(self) -> None:
        with pytest.raises(ValidationError, match=r"metadata\.name"):
            validation.validate_manifest(self._manifest(metadata={}), require_name=True)

    def test_rejects_managed_fields(self) -> None:
        manifest = self._manifest(metadata={"name": "cfg", "managedFields": [{"manager": "x"}]})
        with pytest.raises(ValidationError, match="managedFields"):
            validation.validate_manifest(manifest)

    def test_rejects_non_object(self) -> None:
        with pytest.raises(ValidationError, match="JSON object"):
            validation.validate_manifest([{"apiVersion": "v1"}])


class TestJsonPatch:
    def test_accepts_valid_operations(self) -> None:
        patch = [
            {"op": "replace", "path": "/spec/replicas", "value": 3},
            {"op": "remove", "path": "/metadata/labels/old"},
            {"op": "move", "from": "/a", "path": "/b"},
        ]
        assert validation.validate_json_patch(patch) is patch

    @pytest.mark.parametrize(
        ("patch", "because"),
        [
            ({}, "not a list"),
            ([], "empty"),
            (["nope"], "operation is not an object"),
            ([{"op": "frobnicate", "path": "/a"}], "unknown op"),
            ([{"op": "replace", "path": "a", "value": 1}], "path lacks leading slash"),
            ([{"op": "replace", "path": "/a"}], "replace without a value"),
            ([{"op": "move", "path": "/b"}], "move without from"),
        ],
    )
    def test_rejects_invalid(self, patch: object, because: str) -> None:
        with pytest.raises(ValidationError):
            validation.validate_json_patch(patch)


class TestCommand:
    def test_accepts_an_argv_list(self) -> None:
        assert validation.validate_command(["ls", "-la"]) == ["ls", "-la"]

    def test_rejects_a_bare_string_and_says_what_to_do(self) -> None:
        # The guidance matters: a caller that wanted a shell must ask for one
        # explicitly, which is what keeps shell semantics visible in the argv.
        with pytest.raises(ValidationError, match=r'\["sh", "-c", "<script>"\]'):
            validation.validate_command("ls -la")

    @pytest.mark.parametrize(
        ("command", "because"),
        [
            ([], "empty"),
            ([""], "empty executable"),
            (["ls", 5], "non-string argument"),
            (["ls\x00"], "NUL byte"),
            (["x"] * 65, "too many arguments"),
            (["x" * 4097], "argument too long"),
            ({"cmd": "ls"}, "not a list"),
        ],
    )
    def test_rejects_invalid(self, command: object, because: str) -> None:
        with pytest.raises(ValidationError):
            validation.validate_command(command)

    def test_shell_metacharacters_are_not_special(self) -> None:
        # They are passed through verbatim as a single argv element; nothing
        # interprets them, so there is no injection surface to escape.
        argv = ["echo", "a; rm -rf /", "$(whoami)", "|", "&&"]
        assert validation.validate_command(argv) == argv
