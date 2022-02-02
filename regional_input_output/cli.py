#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typer import Typer, echo

from .dash_app import (
    DEFAULT_SERVER_HOST_IP,
    DEFAULT_SERVER_PATH,
    DEFAULT_SERVER_PORT,
    run_server_dash,
)
from .input_output_models import (
    InterRegionInputOutput,
    InterRegionInputOutputTimeSeries,
)
from .uk_data.utils import EMPLOYMENT_QUARTER_DEC_2017

app = Typer()


@app.callback()
def callback() -> None:
    """Regional Input-Output economic model."""


@app.command()
def server(
    public: bool = False,
    all_cities: bool = False,
    auth: bool = True,
    host: str = DEFAULT_SERVER_HOST_IP,
    port: int = DEFAULT_SERVER_PORT,
    path: str = DEFAULT_SERVER_PATH,
) -> None:
    """Run default dash input-output time series."""
    if public:
        host = "0.0.0.0"
        port = 443
    echo(f"Starting dash server with port {port} and ip {host} at {path}")
    if not auth:
        echo(f"Warning: publicly viewable without authentication.")
    run_server_dash(
        host=host, port=port, all_cities=all_cities, auth=auth, path_prefix=path
    )


@app.command()
def year(year_int: int = EMPLOYMENT_QUARTER_DEC_2017.year) -> None:
    """Run IO model for December 2017."""
    echo(f"Running IO model for year {year_int}")
    echo(f"Warning: {EMPLOYMENT_QUARTER_DEC_2017.year} currently forced.")
    io_model = InterRegionInputOutput()
    io_model.import_export_convergence()
    echo(io_model.y_ij_m_model)
