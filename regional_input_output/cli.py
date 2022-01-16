#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typer import Typer, echo

from .dash_app import DEFAULT_SERVER_HOST_IP, DEFAULT_SERVER_PORT, run_server_dash
from .input_output_models import (
    InterRegionInputOutput,
    InterRegionInputOutputTimeSeries,
)

app = Typer()


@app.callback()
def callback() -> None:
    """Regional Input-Output economic model."""


@app.command()
def server(
    public: bool = False,
    host: str = DEFAULT_SERVER_HOST_IP,
    port: int = DEFAULT_SERVER_PORT,
) -> None:
    """Run default dash input-output time series."""
    if public:
        host = "0.0.0.0"
        port = 443
    echo(f"Starting dash server with port {port} and ip {host}")
    run_server_dash(host=host, port=port)


@app.command()
def year(year_int: int = 2017) -> None:
    """Run IO model for decmber 2017."""
    echo(f"Running IO model for year {year_int}")
    echo(f"Warning: currently this assumes 2017.")
    io_model = InterRegionInputOutput()
    io_model.import_export_convergence()
    echo(io_model.y_ij_m_model)
