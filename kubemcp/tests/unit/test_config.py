"""Configuration, including the Helm-friendly coercions."""

from __future__ import annotations

import pytest
from pydantic import ValidationError as PydanticValidationError

from kubemcp.config import Settings


def make(**overrides: object) -> Settings:
    return Settings(_env_file=None, **overrides)  # type: ignore[arg-type]


class TestDefaults:
    def test_binds_all_interfaces_for_a_container(self) -> None:
        # 127.0.0.1 would make the server unreachable from another pod.
        assert make().host == "0.0.0.0"

    def test_defaults_to_streamable_http(self) -> None:
        assert make().transport == "streamable-http"

    def test_allowed_hosts_defaults_to_empty(self) -> None:
        assert make().allowed_hosts == []


class TestListCoercion:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("a.svc:8080,b.svc", ["a.svc:8080", "b.svc"]),
            ("a.svc , b.svc ", ["a.svc", "b.svc"]),
            ("single", ["single"]),
            ("", []),
            (",,", []),
        ],
    )
    def test_accepts_comma_separated_strings(self, raw: str, expected: list[str]) -> None:
        # Helm renders values as strings, so a CSV has to work as well as a list.
        assert make(allowed_hosts=raw).allowed_hosts == expected

    def test_still_accepts_a_real_list(self) -> None:
        assert make(allowed_hosts=["a", "b"]).allowed_hosts == ["a", "b"]


class TestValidation:
    @pytest.mark.parametrize("port", [0, -1, 70_000])
    def test_rejects_out_of_range_ports(self, port: int) -> None:
        with pytest.raises(PydanticValidationError):
            make(port=port)

    def test_rejects_a_path_without_a_leading_slash(self) -> None:
        with pytest.raises(PydanticValidationError, match="must start with"):
            make(mcp_path="mcp")

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("request_timeout_seconds", 0),
            ("request_timeout_seconds", 601),
            ("max_concurrent_requests", 0),
            ("max_response_bytes", 10),
            ("exec_timeout_seconds", 0),
        ],
    )
    def test_rejects_out_of_range_budgets(self, field: str, value: object) -> None:
        with pytest.raises(PydanticValidationError):
            make(**{field: value})

    def test_rejects_an_unknown_transport(self) -> None:
        with pytest.raises(PydanticValidationError):
            make(transport="carrier-pigeon")


class TestEnvironment:
    def test_reads_the_kubemcp_prefix(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBEMCP_PORT", "9999")
        monkeypatch.setenv("KUBEMCP_DEFAULT_NAMESPACE", "ckh-system")
        monkeypatch.setenv("KUBEMCP_ALLOWED_HOSTS", "kubemcp.ckh.svc.cluster.local:8080")
        settings = Settings(_env_file=None)
        assert settings.port == 9999
        assert settings.default_namespace == "ckh-system"
        assert settings.allowed_hosts == ["kubemcp.ckh.svc.cluster.local:8080"]

    def test_ignores_unrelated_environment_variables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KUBEMCP_SOMETHING_UNKNOWN", "x")
        Settings(_env_file=None)
