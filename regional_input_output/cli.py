#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typer import Typer, echo

from .dash_app import run_server_dash
from .input_output_models import (
    InterRegionInputOutput,
    InterRegionInputOutputTimeSeries,
)

app = Typer()


@app.callback()
def callback():
    """Regional Input-Output economic model."""


@app.command()
def server():
    """Run default dash input-output time series."""
    echo("Starting dash server")
    run_server_dash()


@app.command()
def year(year_int: int = 2017):
    """Run IO model for decmber 2017."""
    echo(f"Running IO model for year {year_int}")
    echo(f"Warning: currently this assumes 2017.")
    io_model = InterRegionInputOutput()
    io_model.import_export_convergence()
    echo(io_model.y_ij_m_model)
