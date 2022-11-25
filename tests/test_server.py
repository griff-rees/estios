#!/usr/bin/env python
# -*- coding: utf-8 -*-

from logging import INFO, WARNING
from pathlib import Path
from shutil import rmtree
from typing import Final, Generator

import pytest
from dash import Dash

from estios.models import InterRegionInputOutputTimeSeries
from estios.server.auth import AuthDB
from estios.server.dash_app import DEFAULT_SERVER_PATH, get_server_dash

TEST_AUTH_PATH = Path("tests/a/test/path.json")

test_server: Dash
io_time_series: InterRegionInputOutputTimeSeries


@pytest.fixture
def tmp_json_auth_file(
    test_path: Path = TEST_AUTH_PATH,
) -> Generator[Path, None, None]:
    """Create a temp auth json file and remove after test.

    Note:
     * Worth replacing with a pytest fixture factory
       https://docs.pytest.org/en/latest/how-to/tmp_path.html
    """
    test_path.unlink(missing_ok=True)
    rmtree(test_path.parent, ignore_errors=True)
    yield test_path
    test_path.unlink(missing_ok=True)
    rmtree(test_path.parent)


def server_paths(server) -> Generator[str, None, None]:
    """Return if path is in server.routes"""
    for route in server.routes:
        yield route.path


def test_server_no_auth(caplog) -> None:
    no_auth_log: tuple[str, int, str] = (
        "estios.server.dash_app",
        WARNING,
        "No authentication required.",
    )
    with caplog.at_level(INFO):
        test_server, io_time_series = get_server_dash(auth=False)
    assert no_auth_log in caplog.record_tuples
    assert DEFAULT_SERVER_PATH in list(server_paths(test_server))


def test_server_auth(caplog, tmp_path) -> None:
    tmp_file = tmp_path / "test.db"
    auth_log: tuple[str, int, str] = (
        "estios.server.dash_app",
        INFO,
        f"Adding basic authentication from {tmp_file}.",
    )
    with caplog.at_level(INFO):
        test_server, io_time_series = get_server_dash(auth_db_path=tmp_file)
    assert auth_log in caplog.record_tuples


def test_path_prefix() -> None:
    test_path: str = "/a/path/test"
    test_server, io_time_series = get_server_dash(path_prefix=test_path)
    assert test_path in list(server_paths(test_server))
    assert len(io_time_series) == 12


def test_AuthDB(tmp_json_auth_file) -> None:
    """Test using an AuthDB with empty or arbitrary json paths."""
    correct_auth_dict: Final = {"test_id": {"name": "test", "password": "password"}}
    auth_db = AuthDB(json_db_path=tmp_json_auth_file)  # Path as Path object
    assert auth_db.users == {}
    auth_db.add_user("test_id", "test", "password")
    assert auth_db.users == correct_auth_dict
    auth_db.write()
    auth_db2 = AuthDB(json_db_path=str(tmp_json_auth_file))  # Path as str
    assert auth_db2.users == correct_auth_dict


def test_io_table(caplog) -> None:
    io_table_log: tuple[str, int, str] = (
        "estios.server.dash_app",
        INFO,
        "Appending 'table-div' to layout.",
    )
    with caplog.at_level(INFO):
        test_server, io_time_series = get_server_dash(auth=False, io_table=True)
    assert io_table_log in caplog.record_tuples
