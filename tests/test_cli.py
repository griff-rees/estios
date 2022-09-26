#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typer.testing import CliRunner

from estios.cli import app

runner = CliRunner()


def test_year_app():
    result = runner.invoke(app, ["year"])
    assert result.exit_code == 0
    assert "Running IO model for year 2017" in result.stdout
