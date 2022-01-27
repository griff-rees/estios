#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import date
from logging import getLogger
from typing import Final, Optional

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

from regional_input_output.uk_data.utils import (
    generate_employment_quarterly_dates,
    get_all_centre_for_cities_dict,
)

from .auth import AuthDB  # , set_auth_middleware
from .input_output_models import InterRegionInputOutputTimeSeries
from .uk_data.utils import (
    CENTRE_FOR_CITIES_EPSG,
    CENTRE_FOR_CITIES_REGION_COLUMN,
    CONFIG_2017_QUARTERLY,
    EMPLOYMENT_QUARTER_DEC_2017,
)
from .visualisation import draw_ego_flows_network

logger = getLogger(__name__)
load_dotenv()

auth_db = AuthDB()

VALID_USERNAME_PASSWORD_PAIRS: Final[dict[str, str]] = {
    attr["name"]: attr["password"] for user, attr in auth_db.users.items()
}

EXTERNAL_STYLESHEETS: Final[list[str]] = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]
DEFAULT_SERVER_PORT: Final[int] = 8090
DEFAULT_SERVER_HOST_IP: Final[str] = "127.0.0.1"
DEFAULT_SERVER_PATH: Final[str] = "/dash"


def get_dash_app(
    input_output_ts: InterRegionInputOutputTimeSeries,
    external_stylesheets: list[str] = EXTERNAL_STYLESHEETS,
    default_date: date = EMPLOYMENT_QUARTER_DEC_2017,
    default_top_sectors: int = 4,
    default_sectors_marker_hops: int = 2,
    default_region: str = "Manchester",
    default_sector: str = "Production",
    date_fmt: str = "%b %y",
    fullscreen: bool = True,
    **kwargs,
) -> Dash:
    from IPython import get_ipython

    app: Dash = (
        JupyterDash(__name__, external_stylesheets=external_stylesheets, **kwargs)
        if get_ipython()
        else Dash(__name__, external_stylesheets=external_stylesheets, **kwargs)
    )

    app.layout = html.Div(
        [
            html.H1(
                "City-level input-output flows",
                id="map-title",
            ),
            dcc.Graph(id="trade"),
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
            dcc.Slider(
                id="n_flows",
                min=1,
                max=len(input_output_ts.regions),
                value=default_top_sectors,
                step=1,
                marks={
                    i: f"top {i}"
                    for i in range(
                        0, len(input_output_ts.regions), default_sectors_marker_hops
                    )
                },
                # tooltip={"placement": "bottom", "always_visible": True},
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
            # Input('in_vs_out_flow', 'value'),
        ],
    )
    def draw_io_map(
        date_index: int,
        selected_city: str,
        selected_sector: str,
        n_flows: int,
        # in_vs_out_flow: bool = True,
    ) -> Figure:
        region_data: GeoDataFrame = input_output_ts[date_index].region_data
        fig = draw_ego_flows_network(
            input_output_ts[date_index].region_data,
            input_output_ts[date_index].y_ij_m_model,
            selected_city,
            selected_sector,
            n_flows,
            zoom=6,
        )
        if fullscreen:
            fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
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
    config_data: Optional[dict] = CONFIG_2017_QUARTERLY,
    auth: bool = True,
    all_cities: bool = False,
    path_prefix: str = DEFAULT_SERVER_PATH,
    **kwargs,
) -> Dash:
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
        requests_pathname_prefix=path_prefix,
        **kwargs,
    )
    if auth:
        logger.info("Adding basic authentication.")
        BasicAuth(flask_dash_app, VALID_USERNAME_PASSWORD_PAIRS)
        #     auth_db = AuthDB()
        #     set_auth_middleware(fastapi_server_app, auth_db)
    else:
        logger.warning("No authentication required.")
    fastapi_server_app = FastAPI()
    fastapi_server_app.mount(path_prefix, WSGIMiddleware(flask_dash_app.server))
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
