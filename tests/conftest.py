from __future__ import annotations

import os

import pytest

from scheduler import db

TEST_DSN = os.environ.get(
    "TEST_DATABASE_URL", "postgresql://scheduler:scheduler@localhost:5433/scheduler_test"
)


@pytest.fixture
def dsn() -> str:
    return TEST_DSN


@pytest.fixture
def conn():
    c = db.connect(TEST_DSN)
    db.migrate(c)
    db.reset(c)
    yield c
    c.close()
