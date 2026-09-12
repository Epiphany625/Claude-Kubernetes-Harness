"""Error mapping is what turns an apiserver rejection into something the calling
model can act on, so these tests assert on the *content* of the messages."""

from __future__ import annotations

import json
from typing import Any

import pytest
from kubernetes_asyncio.client.exceptions import ApiException
from mcp.shared.exceptions import MCPError

from kubemcp.k8s import errors
from kubemcp.k8s.client import raise_for_status
from kubemcp.k8s.errors import ToolError, describe_api_error, guard
from kubemcp.validation import ValidationError


def api_error(code: int, message: str, reason: str, **details: Any) -> ApiException:
    exc = ApiException(status=code, reason=reason)
    body: dict[str, Any] = {
        "kind": "Status",
        "status": "Failure",
        "message": message,
        "reason": reason,
        "code": code,
    }
    if details:
        body["details"] = details
    exc.body = json.dumps(body)
    return exc


class TestMessages:
    def test_404_tells_the_caller_where_to_look(self) -> None:
        message = describe_api_error(
            api_error(404, 'pods "web-0" not found', "NotFound"), action="get pod default/web-0"
        )
        assert "Not found" in message
        assert 'pods "web-0" not found' in message
        assert "list_api_resources" in message

    def test_403_names_the_resource_so_an_rbac_rule_can_be_written(self) -> None:
        # The harness turns this into a concrete Role rule, so the group and
        # resource have to survive into the message.
        message = describe_api_error(
            api_error(
                403,
                'pods is forbidden: User "system:serviceaccount:ckh:kubemcp" cannot list '
                'resource "pods" in API group ""',
                "Forbidden",
                kind="pods",
                group="",
            ),
            action="list pods in namespace prod",
        )
        assert "Forbidden" in message
        assert "'pods'" in message
        assert "ServiceAccount" in message
        assert "does not grant permissions" in message

    def test_409_already_exists_points_at_apply(self) -> None:
        message = describe_api_error(
            api_error(409, 'pods "web-0" already exists', "AlreadyExists"),
            action="create pod default/web-0",
        )
        assert "apply_resource" in message

    def test_409_conflict_points_at_force_conflicts(self) -> None:
        message = describe_api_error(
            api_error(409, "Operation cannot be fulfilled", "Conflict"),
            action="apply deployment default/api",
        )
        assert "force_conflicts=true" in message

    def test_410_explains_the_stale_continue_token(self) -> None:
        message = describe_api_error(
            api_error(410, "continue parameter is too old", "Expired"), action="list pods"
        )
        assert "continue token" in message

    def test_422_surfaces_the_field_level_causes(self) -> None:
        message = describe_api_error(
            api_error(
                422,
                "Deployment in version v1 cannot be handled",
                "Invalid",
                causes=[{"field": "spec.replicas", "message": "must be non-negative"}],
            ),
            action="apply deployment default/api",
        )
        assert "spec.replicas: must be non-negative" in message

    def test_429_says_to_retry(self) -> None:
        message = describe_api_error(
            api_error(429, "too many requests", "TooManyRequests"), action="list pods"
        )
        assert "retry" in message.lower()

    def test_500_says_it_is_the_cluster_not_the_request(self) -> None:
        message = describe_api_error(
            api_error(500, "internal error", "InternalError"), action="list pods"
        )
        assert "not a problem with the request" in message

    def test_handles_a_missing_or_unparseable_body(self) -> None:
        exc = ApiException(status=418, reason="Teapot")
        exc.body = b"<html>not json</html>"
        message = describe_api_error(exc, action="do a thing")
        assert "418" in message


class TestTranslate:
    def test_validation_errors_become_tool_errors(self) -> None:
        mapped = errors.translate(ValidationError("namespace must be lowercase"), action="x")
        assert isinstance(mapped, ToolError)
        assert "lowercase" in str(mapped)

    def test_401_is_a_protocol_error_not_a_tool_error(self) -> None:
        # Nothing the model can rephrase: the deployment's credentials are bad.
        mapped = errors.translate(
            api_error(401, "Unauthorized", "Unauthorized"), action="list pods"
        )
        assert isinstance(mapped, MCPError)

    def test_403_is_a_tool_error(self) -> None:
        mapped = errors.translate(api_error(403, "forbidden", "Forbidden"), action="list pods")
        assert isinstance(mapped, ToolError)

    def test_timeouts_are_reported_as_such(self) -> None:
        mapped = errors.translate(TimeoutError(), action="list pods")
        assert isinstance(mapped, ToolError)
        assert "Timed out" in str(mapped)

    def test_unrelated_exceptions_pass_through_untouched(self) -> None:
        original = RuntimeError("something else")
        assert errors.translate(original, action="x") is original


class TestGuard:
    def test_maps_inside_the_block(self) -> None:
        with pytest.raises(ToolError, match="Not found"):
            with guard("get pod default/missing"):
                raise api_error(404, 'pods "missing" not found', "NotFound")

    def test_passes_through_unmapped_exceptions(self) -> None:
        with pytest.raises(RuntimeError):
            with guard("do a thing"):
                raise RuntimeError("boom")

    def test_is_transparent_on_success(self) -> None:
        with guard("do a thing"):
            value = 1 + 1
        assert value == 2


class TestRaiseForStatus:
    """The dynamic client returns error bodies as though they were objects.

    Without this check a 404 would reach the caller as a *successful* result
    describing a nonexistent object, which is the failure mode most likely to
    send a diagnosing agent down a false trail.
    """

    def test_raises_on_a_failure_status(self) -> None:
        body = {
            "kind": "Status",
            "status": "Failure",
            "message": 'pods "web-0" not found',
            "reason": "NotFound",
            "code": 404,
        }
        with pytest.raises(ApiException) as caught:
            raise_for_status(body)
        assert caught.value.status == 404
        assert "not found" in json.loads(caught.value.body)["message"]

    def test_lets_a_successful_delete_status_through(self) -> None:
        # A successful DELETE legitimately returns kind: Status.
        body = {"kind": "Status", "status": "Success", "code": 200}
        assert raise_for_status(body) is body

    def test_leaves_ordinary_objects_alone(self) -> None:
        pod = {"kind": "Pod", "metadata": {"name": "web-0"}}
        assert raise_for_status(pod) is pod

    def test_raises_on_a_status_with_only_an_error_code(self) -> None:
        with pytest.raises(ApiException):
            raise_for_status({"kind": "Status", "code": 500})

    def test_handles_objects_exposing_to_dict(self) -> None:
        class Wrapper:
            def to_dict(self) -> dict[str, Any]:
                return {
                    "kind": "Status",
                    "status": "Failure",
                    "code": 403,
                    "reason": "Forbidden",
                    "message": "nope",
                }

        with pytest.raises(ApiException) as caught:
            raise_for_status(Wrapper())
        assert caught.value.status == 403

    def test_end_to_end_produces_an_actionable_tool_error(self) -> None:
        with pytest.raises(ToolError, match="Not found"):
            with guard("get pod default/web-0"):
                raise_for_status(
                    {
                        "kind": "Status",
                        "status": "Failure",
                        "message": 'pods "web-0" not found',
                        "reason": "NotFound",
                        "code": 404,
                    }
                )
