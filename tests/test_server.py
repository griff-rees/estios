#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
from shutil import rmtree
from typing import Generator

import pytest

from regional_input_output.auth import AuthDB
from regional_input_output.dash_app import get_server_dash

TEST_AUTH_PATH = Path("tests/a/test/path.json")


@pytest.fixture
def tmp_json_auth_file(
    test_path: Path = TEST_AUTH_PATH,
) -> Generator[Path, None, None]:
    """Create a temp auth json file and remove after test.

    Note:
     * Worth replacing with pytest fixture:
       https://docs.pytest.org/en/latest/how-to/tmp_path.html
    """
    test_path.unlink(missing_ok=True)
    rmtree(test_path.parent, ignore_errors=True)
    yield test_path
    test_path.unlink(missing_ok=True)
    rmtree(test_path.parent)


def test_server() -> None:
    server = get_server_dash()
    "/dash" in [route.path for route in server.routes]


def test_AuthDB(tmp_json_auth_file) -> None:
    """Test using an AuthDB with empty or arbitrary json paths."""
    correct_auth_dict = {"test_id": {"name": "test", "password": "password"}}
    auth_db = AuthDB(json_db_path=tmp_json_auth_file)  # Path as Path object
    assert auth_db.users == {}
    auth_db.add_user("test_id", "test", "password")
    assert auth_db.users == correct_auth_dict
    auth_db.write()
    auth_db2 = AuthDB(json_db_path=str(tmp_json_auth_file))  # Path as str
    assert auth_db2.users == correct_auth_dict
