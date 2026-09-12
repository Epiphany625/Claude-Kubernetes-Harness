"""Entrypoint: ``python -m kubemcp`` / ``kubemcp``."""

from __future__ import annotations

from kubemcp.server import run


def main() -> None:
    run()


if __name__ == "__main__":
    main()
