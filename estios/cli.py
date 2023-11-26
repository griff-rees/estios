#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import date
from pathlib import Path
from typing import Annotated, Optional

from pandas import Series
from typer import Option, Typer, echo, secho
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
from .uk.regions import EXAMPLE_UK_CITIES_LIST
from .uk.scenarios import baseline_england_annual_projection
from .utils import (
    FINAL_Y_IJ_M_COLUMN_NAME,
    IJ_M_INDEX_NAMES,
    DateConfigType,
    enforce_end_str,
    enforce_start_str,
    load_series_from_csv,
)

app = Typer()

# DEFAULT_UK_CITY_LIST: Final[list[str]] = [
#     "Birmingham",  # BIRMINGHAM & SMETHWICK
#     "Bradford",
#     # "Coventry",
#     # "Bristol",
#     "Exeter",
#     "Leeds",
#     "Liverpool",  # LIVERPOOL & BIRKENHEAD
#     "Manchester",  # MANCHESTER & SALFORD
#     # Skip because of name inconsistency
#     # 'Newcastle upon Tyne':  'North East',  # NEWCASTLE & GATESHEAD'
#     "Norwich",
#     "Sheffield",
#     "Southampton",
#     "London",
#     "Plymouth",
#     "York",
# ]


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
    url_prefix: str = DEFAULT_SERVER_PATH,
    render_io_table: bool = False,
    scenario: bool = False,
    y_ij_m_path: Annotated[Optional[Path], Option(None, readable=True)] = None,
) -> None:
    """Run default dash input-output time series."""
    input_output_ts: InterRegionInputOutputTimeSeries | None = None
    config_data: DateConfigType | None = None
    default_date: date | None = None
    scenario_cities: list[str] = EXAMPLE_UK_CITIES_LIST
    # scenario_cities: list[str] = DEFAULT_UK_CITY_LIST
    if public:
        host = "0.0.0.0"
        port = 443
    url_prefix = enforce_start_str(url_prefix, "/", True)
    url_prefix = enforce_end_str(url_prefix, "/", False)
    secho(
        f"Starting dash server with port {port} and ip {host} at {url_prefix}", fg=GREEN
    )
    if render_io_table:
        secho(f"Including interactive io_table.", fg=RED)
    else:
        secho(f"Not including interactive io_table.", fg=GREEN)
    if not auth:
        secho(f"Warning: publicly viewable without authentication.", fg=RED)
    if y_ij_m_path:
        # Best if init_b_ij^m is also loaded if available

        y_ij_m_series: Series = load_series_from_csv(
            path=y_ij_m_path,
            column_name=FINAL_Y_IJ_M_COLUMN_NAME,
            index_column_names=IJ_M_INDEX_NAMES,
        )
        # y_ij_m: DataFrame = read_csv(y_ij_m_path)
        # # Diff
        # new_index = y_ij_m[['City', 'Other_City', 'Sector']]
        # y_ij_m_series: Series = Series(
        #     y_ij_m['y_ij^m'].to_list(),
        #     index=new_index,
        # #Stashed changes
        # )
        secho(f"Warning: y_ij_m parameter not available.", fg=RED)
        input_output_ts = InterRegionInputOutputTimeSeries()
        input_output_ts[0]._load_convergence_results(
            None,
            y_ij_m_series,
        )
    elif scenario and not input_output_ts:
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
        path_prefix=url_prefix,
        io_table=render_io_table,
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
