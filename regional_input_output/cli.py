#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typer import Typer, echo, secho
from typer.colors import GREEN, RED

from .models import InterRegionInputOutput, InterRegionInputOutputTimeSeries
from .server.dash_app import (
    DEFAULT_SERVER_HOST_IP,
    DEFAULT_SERVER_PATH,
    DEFAULT_SERVER_PORT,
    run_server_dash,
)
from .uk_data.utils import EMPLOYMENT_QUARTER_DEC_2017
from .utils import enforce_end_str, enforce_start_str

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
    path = enforce_start_str(path, "/", True)
    path = enforce_end_str(path, "/", False)
    secho(f"Starting dash server with port {port} and ip {host} at {path}", fg=GREEN)
    secho(f"Server running on: http://{host}:{port}{path}", fg=GREEN)
    if not auth:
        secho(f"Warning: publicly viewable without authentication.", fg=RED)
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
