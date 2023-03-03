#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import date
from typing import Final

from typer import Typer, echo, secho
from typer.colors import GREEN, RED

from .models import InterRegionInputOutputTimeSeries
from .server.dash_app import (
    DEFAULT_SERVER_HOST_IP,
    DEFAULT_SERVER_PATH,
    DEFAULT_SERVER_PORT,
    run_server_dash,
)
from .uk.models import InterRegionInputOutputUK2017
from .uk.ons_employment_2017 import (
    CONFIG_2015_TO_2017_QUARTERLY,
    EMPLOYMENT_QUARTER_DEC_2017,
)
from .uk.scenarios import baseline_england_annual_projection
from .utils import DateConfigType, enforce_end_str, enforce_start_str

app = Typer()

DEFAULT_UK_CITY_LIST: Final[list[str]] = [
    "Birmingham",  # BIRMINGHAM & SMETHWICK
    "Bradford",
    # "Coventry",
    # "Bristol",
    "Exeter",
    "Leeds",
    "Liverpool",  # LIVERPOOL & BIRKENHEAD
    "Manchester",  # MANCHESTER & SALFORD
    # Skip because of name inconsistency
    # 'Newcastle upon Tyne':  'North East',  # NEWCASTLE & GATESHEAD'
    "Norwich",
    "Sheffield",
    "Southampton",
    "London",
    "Plymouth",
    "York",
]


@app.callback()
def estios() -> None:
    """Regional Input-Output economic model."""


@app.command()
def server(
    public: bool = False,
    all_cities: bool = False,
    auth: bool = True,
    host: str = DEFAULT_SERVER_HOST_IP,
    port: int = DEFAULT_SERVER_PORT,
    path: str = DEFAULT_SERVER_PATH,
    io_table: bool = False,
    scenario: bool = False,
) -> None:
    """Run default dash input-output time series."""
    input_output_ts: InterRegionInputOutputTimeSeries | None = None
    config_data: DateConfigType | None = None
    default_date: date | None = None
    scenario_cities: list[str] = DEFAULT_UK_CITY_LIST
    if public:
        host = "0.0.0.0"
        port = 443
    path = enforce_start_str(path, "/", True)
    path = enforce_end_str(path, "/", False)
    secho(f"Starting dash server with port {port} and ip {host} at {path}", fg=GREEN)
    secho(f"Server running on: http://{host}:{port}{path}", fg=GREEN)
    if io_table:
        secho(f"Including interactive io_table.", fg=RED)
    else:
        secho(f"Not including interactive io_table.", fg=GREEN)
    if not auth:
        secho(f"Warning: publicly viewable without authentication.", fg=RED)
    if scenario and not input_output_ts:
        input_output_ts = baseline_england_annual_projection(regions=scenario_cities)
    else:
        config_data = CONFIG_2015_TO_2017_QUARTERLY
    run_server_dash(
        input_output_ts=input_output_ts,
        config_data=config_data,
        host=host,
        port=port,
        all_cities=all_cities,
        auth=auth,
        path_prefix=path,
        io_table=io_table,
        default_date=default_date,
    )


@app.command()
def year(year_int: int = EMPLOYMENT_QUARTER_DEC_2017.year) -> None:
    """Run IO model for December 2017."""
    echo(f"Running IO model for year {year_int}")
    echo(f"Warning: {EMPLOYMENT_QUARTER_DEC_2017.year} currently forced.")
    io_model = InterRegionInputOutputUK2017()
    io_model.import_export_convergence()
    echo(io_model.y_ij_m_model)
