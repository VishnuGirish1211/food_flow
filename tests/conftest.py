"""Common test fixtures and utilities."""

import os
import pytest

# Integration tests will point directly to the local docker-compose environment
# (so we assume `docker compose up -d` has been run before running integration tests).
API_BASE = os.getenv("API_BASE", "http://localhost:8085")

@pytest.fixture
def api_base():
    return API_BASE
