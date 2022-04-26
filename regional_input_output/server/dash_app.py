#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass
from datetime import date
from logging import getLogger
from typing import Final, Generator, Optional

import uvicorn
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from dash_auth import BasicAuth
from dotenv import load_dotenv
from fastapi import FastAPI
from flask import Flask
from geopandas import GeoDataFrame
from jupyter_dash import JupyterDash
from plotly.graph_objects import Figure
from starlette.middleware.wsgi import WSGIMiddleware

from ..models import InterRegionInputOutputTimeSeries
from ..uk_data.employment import (
    CITY_SECTOR_AVERAGE_EARNINGS_COLUMN,
    CITY_SECTOR_EDUCATION_COLUMN,
    CITY_SECTOR_POPULATION_COLUMN,
    CONFIG_2015_TO_2017_QUARTERLY,
    CONFIG_2017_QUARTERLY,
    EMPLOYMENT_QUARTER_DEC_2017,
)
from ..uk_data.regions import (
    CENTRE_FOR_CITIES_REGION_COLUMN,
    get_all_centre_for_cities_dict,
)
from ..utils import enforce_end_str, enforce_start_str
from ..visualisation import draw_ego_flows_network
from .auth import DB_PATH, AuthDB, DBPathType  # , set_auth_middleware

logger = getLogger(__name__)
load_dotenv()

EXTERNAL_STYLESHEETS: Final[list[str]] = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]
DEFAULT_SERVER_PORT: Final[int] = 8090
DEFAULT_SERVER_HOST_IP: Final[str] = "127.0.0.1"
DEFAULT_SERVER_PATH: Final[str] = "/uk-cities"
PATH_SPLIT_CHAR: Final[str] = "/"

DEFAULT_MAP_TITLE: Final[str] = "UK City Input-Output Flows"
DEFAULT_REGION: Final[str] = "Manchester"
DEFAULT_SECTOR: Final[str] = "Production"
DEFAULT_DATE_FORMAT: Final[str] = "%b %y"

DEFAULT_TOP_SECTORS: Final[int] = 4
DEFAULT_SECTORS_MARKER_HOPS: Final[int] = 4
DEFAULT_HEATMAP_COLOUR_SCALE: Final[str] = "portland"
DEFAULT_COLOUR_CONFIG: Final[str] = "Education"


@dataclass
class ColourConfig:
    column_name: str
    is_continuous: bool
    legend_label: str


ColourOptionsType = dict[str, ColourConfig]

DEFAULT_COLOUR_OPTIONS: Final[ColourOptionsType] = {
    "Education": ColourConfig(
        column_name=CITY_SECTOR_EDUCATION_COLUMN,
        is_continuous=True,
        legend_label="% 9-4 GCSEs",
    ),
    "Earnings": ColourConfig(
        column_name=CITY_SECTOR_AVERAGE_EARNINGS_COLUMN,
        is_continuous=True,
        legend_label="Avg. Earnings",
    ),
    "Region": ColourConfig(
        column_name=CENTRE_FOR_CITIES_REGION_COLUMN,
        is_continuous=False,
        legend_label="Region",
    ),
}


def generate_markers(
    total: int, minimum: int = 1, marker_hops: int = DEFAULT_SECTORS_MARKER_HOPS
) -> Generator[int, None, None]:
    for i in range(minimum, total, int(total / marker_hops)):
        yield i


def get_dash_app(
    input_output_ts: InterRegionInputOutputTimeSeries,
    external_stylesheets: list[str] = EXTERNAL_STYLESHEETS,
    colour_options: ColourOptionsType = DEFAULT_COLOUR_OPTIONS,
    sector_markers: Optional[list[int]] = None,
    default_date: date = EMPLOYMENT_QUARTER_DEC_2017,
    default_top_sectors: int = DEFAULT_TOP_SECTORS,
    default_sectors_marker_hops: int = DEFAULT_SECTORS_MARKER_HOPS,
    default_region: str = DEFAULT_REGION,
    default_sector: str = DEFAULT_SECTOR,
    default_colour: str = DEFAULT_COLOUR_CONFIG,
    date_fmt: str = DEFAULT_DATE_FORMAT,
    fullscreen: bool = True,
    colour_scale: str = DEFAULT_HEATMAP_COLOUR_SCALE,
    map_title: str = DEFAULT_MAP_TITLE,
    minimum_sector_markers: int = 1,
    **kwargs,
) -> Dash:
    from IPython import get_ipython

    app: Dash = (
        JupyterDash(__name__, external_stylesheets=external_stylesheets, **kwargs)
        if get_ipython()
        else Dash(__name__, external_stylesheets=external_stylesheets, **kwargs)
    )
    if not sector_markers:
        sector_markers = list(
            generate_markers(
                len(input_output_ts.regions),
                minimum_sector_markers,
                default_sectors_marker_hops,
            )
        )
    app.layout = html.Div(
        [
            html.H1(
                map_title,
                id="map-title",
            ),
            dcc.Graph(id="trade"),
            html.Div(
                id="city-div",
                children=[
                    html.H2("City"),
                    dcc.Dropdown(
                        id="dropdown_city",
                        options=[
                            {"label": city, "value": city}
                            for city in input_output_ts.region_names
                        ],
                        # searchable=True,
                        # placeholder="Select a city",
                        value=default_region,
                    ),
                    html.H2("City Colour"),
                    dcc.Dropdown(
                        id="city_colour",
                        options=[
                            {"label": data_type, "value": data_type}
                            for data_type in colour_options
                        ],
                        value=default_colour,
                    ),
                ],
            ),
            html.Div(
                id="sector-div",
                children=[
                    html.H2("Sector"),
                    dcc.Dropdown(
                        id="dropdown_sector",
                        options=[
                            {"label": sector, "value": sector}
                            for sector in input_output_ts.sectors
                        ],  # need to replace this with an automated dictionary at some stage
                        # searchable=True,
                        # placeholder="Select a sector",
                        value=default_sector,
                    ),
                    html.H2("Top Sector Flows"),
                    dcc.Slider(
                        id="n_flows",
                        min=1,
                        max=len(input_output_ts.regions),
                        value=default_top_sectors,
                        step=1,
                        marks={i: f"top {i}" for i in sector_markers},
                        # tooltip={"placement": "bottom", "always_visible": True},
                    ),
                ],
            ),
            html.Div(
                id="date-div",
                children=[
                    html.H2("Date"),
                    dcc.Slider(
                        id="date_index",
                        min=0,
                        max=len(input_output_ts) - 1,
                        step=None,
                        marks={
                            i: date.strftime(date_fmt)
                            for i, date in enumerate(input_output_ts.dates)
                        },
                        value=input_output_ts.dates.index(default_date),
                        included=False,
                    ),
                ],
            ),
            # dcc.Store(id="current_date_index"),
        ]
    )

    # @app.callback(
    #     Output('current_date_index', 'data'),
    #     [Input('date_selected', 'value'),]
    # )
    # def set_date(date_selected: int) -> int:
    #     print(date_selected)
    #     date_obj: date = datetime.strptime(date_selected, '%Y-%m-%d').date()
    #     return input_output_ts.dates.index(date_obj)

    @app.callback(
        Output("trade", "figure"),
        [
            Input("date_index", "value"),
            Input("dropdown_city", "value"),
            Input("dropdown_sector", "value"),
            Input("n_flows", "value"),
            Input("city_colour", "value")
            # Input('in_vs_out_flow', 'value'),
        ],
    )
    def draw_io_map(
        date_index: int,
        selected_city: str,
        selected_sector: str,
        n_flows: int,
        city_colour: str,
        # in_vs_out_flow: bool = True,
    ) -> Figure:
        region_data: GeoDataFrame = input_output_ts[date_index].region_data
        colour_config: ColourConfig = colour_options[city_colour]
        logger.debug(f"Current colour_config column: {colour_config.column_name}")
        # city_colour_column: str = colour_config.column_name
        # if colour_config.is_continuous:
        #     city_colour_column = city_colour_column.replace('YEAR', '2017')
        fig = draw_ego_flows_network(
            input_output_ts[date_index].region_data,
            input_output_ts[date_index].y_ij_m_model,
            selected_city,
            selected_sector,
            n_flows,
            zoom=6,
            colour_column=colour_config.column_name,
        )
        if fullscreen:
            fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        if colour_config.is_continuous:
            fig.update_layout(
                coloraxis=dict(
                    colorscale=colour_scale,
                    colorbar=dict(
                        bgcolor="rgba(0,0,0,0)",
                        bordercolor="rgba(0,0,0,0)",
                        tickfont=dict(family="Courier", size=12, color="white"),
                        # orientation="h",
                        x=0,
                        y=0.5,
                        ypad=300,
                        title=dict(
                            font=dict(color="white"),
                            # side="bottom",
                            # side="right",
                            side="top",
                            text=colour_config.legend_label,
                        ),
                    ),
                )
            )
        return fig

    return app


def get_jupyter_app(
    input_output_ts: InterRegionInputOutputTimeSeries, **kwargs
) -> JupyterDash:
    app: JupyterDash = get_dash_app(input_output_ts)
    # app.run_server(mode='jupyterlab', port = 8090, dev_tools_ui=True, #debug=True,
    #                dev_tools_hot_reload =True, threaded=True)
    app.run_server(mode="inline", dev_tools_hot_reload=True, **kwargs)
    return app


def get_server_dash(
    input_output_ts: Optional[InterRegionInputOutputTimeSeries] = None,
    config_data: Optional[dict] = CONFIG_2015_TO_2017_QUARTERLY,
    auth: bool = True,
    auth_db_path: DBPathType = DB_PATH,
    all_cities: bool = False,
    path_prefix: str = DEFAULT_SERVER_PATH,
    **kwargs,
) -> Dash:
    path_prefix = enforce_start_str(path_prefix, PATH_SPLIT_CHAR, True)
    if not input_output_ts and config_data:
        logger.info("Using default config_data configuration")
        if all_cities:
            logger.info("Using almost all UK cities (currently only England).")
            almost_all_cities: dict[str, str] = get_all_centre_for_cities_dict()
            input_output_ts = InterRegionInputOutputTimeSeries.from_dates(
                config_data, regions=almost_all_cities
            )
        else:
            input_output_ts = InterRegionInputOutputTimeSeries.from_dates(config_data)
    assert input_output_ts, "No InputOuput TimeSeries to visualise"
    logger.warning(
        "Currently runs all InputOutput models irrespective of cached results"
    )
    input_output_ts.calc_models()
    # server = FastAPI()
    # flask_dash_app: Flask = Flask(__name__)
    flask_dash_app: Dash = get_dash_app(
        input_output_ts,  # server=flask_dash_app,
        requests_pathname_prefix=enforce_end_str(path_prefix, PATH_SPLIT_CHAR, True),
        **kwargs,
    )
    if auth:
        logger.info(f"Adding basic authentication from {auth_db_path}.")
        auth_pairs: dict[str, str] = AuthDB(auth_db_path).get_users_dict()
        BasicAuth(flask_dash_app, auth_pairs)
        #     auth_db = AuthDB()
        #     set_auth_middleware(fastapi_server_app, auth_db)
    else:
        logger.warning("No authentication required.")
    fastapi_server_app = FastAPI()
    fastapi_server_app.mount(
        enforce_end_str(path_prefix, PATH_SPLIT_CHAR, False),
        WSGIMiddleware(flask_dash_app.server),
    )
    return fastapi_server_app


def run_server_dash(
    port: int = DEFAULT_SERVER_PORT,
    host: str = DEFAULT_SERVER_HOST_IP,
    **kwargs,
) -> None:
    # dash_app: Dash = get_server_dash(input_output_ts,**kwargs)
    app: FastAPI = get_server_dash(**kwargs)
    # server.mount("/dash", WSGIMiddleware(dash_app.server))

    # [print(route) for route in app.routes]
    uvicorn.run(app, port=port, host=host)


# $ jupyter labextension install @jupyter-widgets/jupyterlab-manager keplergl-jupyter


# app.run_server(mode='jupyterlab', port = 8090, dev_tools_ui=True, #debug=True,
#               dev_tools_hot_reload =True, threaded=True)
# if __name__ == "__main__":
#     run_server_dash()