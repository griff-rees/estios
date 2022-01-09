#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import date
from logging import getLogger
from typing import Final

from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from fastapi import FastAPI
from flask import Flask
from jupyter_dash import JupyterDash
from starlette.middleware.wsgi import WSGIMiddleware
import uvicorn

from .input_output_models import InterRegionInputOutputTimeSeries
from .visualisation import draw_ego_flows_network
from .uk_data.utils import (
    CENTRE_FOR_CITIES_EPSG,
    CENTRE_FOR_CITIES_REGION_COLUMN,
    CONFIG_2017_QUARTERY,
    EMPLOYMENT_QUARTER_DEC_2017,
)

EXTERNAL_STYLESHEETS: Final[list[str]] = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]

logger = getLogger(__name__)


def get_dash_app(
    input_output_ts: InterRegionInputOutputTimeSeries,
    external_stylesheets: list[str] = EXTERNAL_STYLESHEETS,
    default_date: date = EMPLOYMENT_QUARTER_DEC_2017,
    default_top_sectors: int = 4,
    default_sectors_marker_hops: int = 2,
    default_region: str = "Manchester",
    default_sector: str = "Production",
    date_fmt: str = "%b %y",
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
                style={"text-align": "center"},  # 'color':'#012e67' ,
            ),
            dcc.Graph(id="trade"),
            dcc.Dropdown(
                id="dropdown_city",
                options=[
                    {"label": city, "value": city}
                    for city in input_output_ts.region_names
                ],
                searchable=True,
                # placeholder="Select a city",
                value=default_region,
            ),
            dcc.Dropdown(
                id="dropdown_sector",
                options=[
                    {"label": sector, "value": sector}
                    for sector in input_output_ts.sectors
                ],  # need to replace this with an automated dictionary at some stage
                searchable=True,
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
    input_output_ts: InterRegionInputOutputTimeSeries, **kwargs
) -> Dash:
    return get_dash_app(input_output_ts, requests_pathname_prefix="/dash/", **kwargs)


def run_server_dash(
    input_output_ts: InterRegionInputOutputTimeSeries, **kwargs
) -> None:
    server = FastAPI()
    app: Dash = get_server_dash(InterRegionInputOutputTimeSeries, **kwargs)
    server.mount("/dash", WSGIMiddleware(app.server))
    uvicorn.run(server)


# $ jupyter labextension install @jupyter-widgets/jupyterlab-manager keplergl-jupyter


# app.run_server(mode='jupyterlab', port = 8090, dev_tools_ui=True, #debug=True,
#               dev_tools_hot_reload =True, threaded=True)
# if __name__ == "__main__":
#     run_server_dash()
