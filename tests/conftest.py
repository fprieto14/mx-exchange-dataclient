"""Pytest configuration for BIVA client tests."""

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="Run integration tests (requires network)",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: mark test as integration test (requires network)"
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--integration"):
        skip_integration = pytest.mark.skip(reason="need --integration option to run")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)
