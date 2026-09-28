"""Tests run against a real PostgreSQL.

CI sets TEST_DATABASE_URL to a Postgres service container. Locally, without it,
an embedded PostgreSQL 16 is started with pgserver, so no Docker is needed.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import psycopg
import pytest
from shopflow_datagen import DirtyConfig, GenConfig, write

SCHEMAS = ("raw", "mart", "meta")


@pytest.fixture(scope="session")
def db_url():
    url = os.environ.get("TEST_DATABASE_URL")
    if url:
        yield url
        return
    try:
        import pgserver  # noqa: PLC0415 - only needed without a provided database
    except ImportError:
        pytest.skip("no embedded PostgreSQL for this Python version; set TEST_DATABASE_URL")
    server = pgserver.get_server(tempfile.mkdtemp(prefix="ecom-pg-"), cleanup_mode="stop")
    yield server.get_uri()


@pytest.fixture
def conn(db_url):
    with psycopg.connect(db_url) as c:
        for schema in SCHEMAS:
            c.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        c.commit()
        yield c


@pytest.fixture(scope="session")
def source(tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("clean")
    write(GenConfig(scale=0.02, chunk_size=1_000), out)
    return out


@pytest.fixture(scope="session")
def dirty_source(tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("dirty")
    dirty = DirtyConfig(duplicate_rate=0.02, orphan_rate=0.01)
    write(GenConfig(scale=0.02, chunk_size=1_000, dirty=dirty), out)
    return out


@pytest.fixture(scope="session")
def drifted_source(tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("drift")
    write(GenConfig(scale=0.02, chunk_size=1_000, dirty=DirtyConfig(schema_drift=True)), out)
    return out
