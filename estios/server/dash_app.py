#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass
from datetime import date
from logging import getLogger
from typing import Final, Optional, Type, Union

import uvicorn
from dash import Dash, dcc, html
from dash.dash_table import DataTable, FormatTemplate
from dash.dash_table.Format import Symbol
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate
from dash_auth import BasicAuth
from dotenv import load_dotenv
from fastapi import FastAPI
from jupyter_dash import JupyterDash
from pandas import DataFrame
from plotly.graph_objects import Figure, layout
from starlette.middleware.wsgi import WSGIMiddleware

from ..models import InterRegionInputOutput, InterRegionInputOutputTimeSeries
from ..temporal import date_io_time_series
from ..uk.models import InterRegionInputOutputUK2017
from ..uk.ons_employment_2017 import (
    CITY_SECTOR_AVERAGE_EARNINGS_COLUMN,
    CITY_SECTOR_EDUCATION_COLUMN,
    CONFIG_2015_TO_2017_QUARTERLY,
    EMPLOYMENT_QUARTER_DEC_2017,
)
from ..uk.regions import CENTRE_FOR_CITIES_REGION_COLUMN, get_all_centre_for_cities_dict
from ..utils import (
    DateConfigType,
    InputOutputConfigType,
    enforce_end_str,
    enforce_start_str,
)
from ..visualisation import (
    DEFAULT_REGION_PALATE,
    FontConfig,
    draw_ego_flows_network,
    generate_colour_scheme,
    sector_flows_bar_chart,
)
from .auth import DB_PATH, AuthDB, DBPathType  # , set_auth_middleware

logger = getLogger(__name__)
load_dotenv()

CoordTupleTyple = dict[str, float]

EXTERNAL_STYLESHEETS: Final[list[str]] = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]
DEFAULT_SERVER_PORT: Final[int] = 8090
DEFAULT_SERVER_HOST_IP: Final[str] = "127.0.0.1"
DEFAULT_SERVER_PATH: Final[str] = "/uk-cities"
PATH_SPLIT_CHAR: Final[str] = "/"

DEFAULT_MAP_TITLE: Final[str] = "UK City Input-Output Economic Trade Flows"
DEFAULT_REGION: Final[str] = "Manchester"
DEFAULT_SECTOR: Final[str] = "Production"
DEFAULT_DATE_FORMAT: Final[str] = "%b %y"

DEFAULT_TOP_SECTORS: Final[int] = 4
DEFAULT_SECTORS_MARKER_HOPS: Final[int] = 3
DEFAULT_HEATMAP_COLOUR_SCALE: Final[str] = "portland"
DEFAULT_COLOUR_CONFIG: Final[str] = "Education"

DEFAULT_UK_MAP_CENTRE: Final[CoordTupleTyple] = {"lat": 52.630886, "lon": 1.297355}


DEFAULT_PLOT_BACKGROUND_COLOUR: Final[str] = "rgba(0, 0, 0, 0)"
DEFAULT_AXIS_COLOUR: Final[str] = "white"


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


# def generate_markers(
#     total: int, minimum: int = 0, marker_hops: int = DEFAULT_SECTORS_MARKER_HOPS
# ) -> Generator[int, None, None]:
#     for i in range(minimum, total, int(total / marker_hops)):
#         yield i


def io_table_layout(io_table: DataFrame, index_column_name: str = "I-O") -> DataTable:
    """Render Dash input-output table.

    Todo:
        * Enable fixed left column with covered background.
    """
    io_table = io_table.reset_index().rename(columns={"index": index_column_name})
    return DataTable(
        io_table.to_dict("records"),
        columns=[
            {
                "name": i,
                "id": i,
                "type": "numeric",
                "format": FormatTemplate.money(0).symbol(Symbol.yes).symbol_prefix("£"),
            }
            for i in io_table.columns
        ],
        id="io-table",
        # fixed_columns = {"headers": True, 'data': 1},
        style_table={
            "overflowX": "auto",
        },
        style_cell={
            "backgroundColor": "transparent",
            "whiteSpace": "normal",
            "height": "auto",
        },
    )


def get_dash_app(
    input_output_ts: InterRegionInputOutputTimeSeries,
    external_stylesheets: list[str] = EXTERNAL_STYLESHEETS,
    colour_options: ColourOptionsType = DEFAULT_COLOUR_OPTIONS,
    # sector_markers: Optional[list[int]] = None,
    default_date: date | None = EMPLOYMENT_QUARTER_DEC_2017,
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
    io_table: bool = True,
    axis_colour: str = DEFAULT_AXIS_COLOUR,
    plot_background_colour: str = DEFAULT_PLOT_BACKGROUND_COLOUR,
    region_colour_palette: Optional[
        Union[list[str], dict[str, str]]
    ] = DEFAULT_REGION_PALATE,
    font_config: FontConfig = FontConfig(),
    **kwargs,
) -> Dash:
    """Generate `dash` web app for visualisation.

    Args:
        input_output_ts: `InterRegionInputOutputTimeSeries` to visualise.
        external_stylesheets: `list` of `urls` to extra stylesheets (`.css`)
        colour_options: Settings for colouring city icons by demographics (education etc.)
        default_date: Which date to render `input_output_ts` when initially loaded
        default_top_sectors: How man top sectors to reder by default.
        default_sectors_marker_hops: How many sectors to hop markers between.
        default_region: region to select on when loaded.
        default_sector: sector to select when loaded.
        default_colour: default config for city colours.
        date_fmt: format `str` for dates
        fullscreen: whether to render in full screen mode
        colour_scale: which `plotly` colour scheme to apply
        map_title: title to display over map
        minimum_sector_markers: minimum number of top sectors to display by default
        io_table: whether to also show the estimated Input-Output table
        axis_colour: colour for flow chart axis
        plot_background_colour: background colour for overlayed plots
        region_colour_palette: colour pallet for English regions
        font_config: what fonts to use
        **kwargs: args to pass to `Dash` or `JupyterDash`

    Returns:
        A `Dash` or `JupyterDash` instance configured by passed parameters.
    """
    if not input_output_ts.is_calculated:
        logger.warning(
            "Running all InputOutput models due to no provided cached results"
        )
        input_output_ts.calc_models()
    from IPython import get_ipython

    app: Dash = (
        JupyterDash(__name__, external_stylesheets=external_stylesheets, **kwargs)
        if get_ipython()
        else Dash(__name__, external_stylesheets=external_stylesheets, **kwargs)
    )
    # if not sector_markers:
    #     sector_markers = list(
    #         generate_markers(
    #             len(input_output_ts.regions),
    #             minimum_sector_markers,
    #             default_sectors_marker_hops,
    #         )
    #     )
    if not default_date or default_date not in input_output_ts.dates:
        logger.warning(f"Setting default date to {input_output_ts[0].date}")
        default_date = input_output_ts[0].date
        assert isinstance(default_date, date)
    if region_colour_palette and isinstance(region_colour_palette, list):
        region_colour_palette = generate_colour_scheme(
            input_output_ts.regions, region_colour_palette
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
                    html.Div(
                        id="city-select-div",
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
                        ],
                    ),
                    html.Div(
                        id="city-colour-div",
                        children=[
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
                    # html.H2("City Sector Trade Flows"),
                    dcc.Graph(id="flows-bar-chart"),
                    dcc.RangeSlider(
                        id="n_flows",
                        min=0,
                        max=len(input_output_ts.regions) - 2,
                        # value=default_top_sectors,
                        value=(
                            len(input_output_ts.regions) - default_top_sectors,
                            len(input_output_ts.regions) - 1,
                        ),
                        step=1,  # Needed because continuous if not speficied
                        # marks={i: None for i in range(len(input_output_ts.regions) - 2)},
                        # tooltip={"placement": "bottom", "always_visible": True},
                        # marks=None,
                        marks={i: "" for i in range(len(input_output_ts.regions) - 1)},
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
                        max=len(input_output_ts)
                        - 1,  # avoid excess index outside time points
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
    if io_table:
        logger.info("Appending 'table-div' to layout.")
        app.layout.children.append(
            html.Div(
                id="table-div",
                # children=[
                #     # io_table_layout(input_output_ts[0].regional_io_projections["Manchester"])
                # ],
            )
        )

    @app.callback(
        Output("flows-bar-chart", "figure"),
        [
            Input("date_index", "value"),
            Input("dropdown_city", "value"),
            Input("dropdown_sector", "value"),
        ],
    )
    def plot_flows_distribution(
        date_index: int,
        selected_city: str,
        selected_sector: str,
        # city_colour: str,
    ) -> Figure:
        if isinstance(region_colour_palette, list):
            raise TypeError(
                "`region_colour_palette` can only be passed to `sector_flows_bar_chart` as a dict or None"
            )
        else:
            return sector_flows_bar_chart(
                input_output_ts[date_index].y_ij_m_model,
                selected_city,
                selected_sector,
                axis_colour=axis_colour,
                plot_background_colour=plot_background_colour,
                colour_column="index",  # Replace this literally in future
                dash_render=True,
                colour_palette=region_colour_palette,
                dash_font_config=font_config,
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
        Output("table-div", "children"),
        [
            Input("date_index", "value"),
            Input("dropdown_city", "value"),
            Input("io_table", "value"),
        ],
    )
    def render_io_table(
        date_index: int,
        selected_city: str,
        io_table: bool,
    ) -> DataTable:
        if io_table:
            return [
                io_table_layout(
                    input_output_ts[date_index].regional_io_projections[selected_city]
                ),
            ]
        else:
            raise PreventUpdate

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
        """Generate an ego-alter network diagram filtering on ordered ranges of alters."""
        # region_data: GeoDataFrame = input_output_ts[date_index].region_data
        colour_config: ColourConfig = colour_options[city_colour]
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
            ui_slider_index_fix=1,  # Suit slider interaction
            plot_background_colour=plot_background_colour,
            colour_palette=region_colour_palette,
            font_config=font_config,
        )
        if fullscreen:
            fig.update_layout(
                margin={"r": 0, "t": 0, "l": 0, "b": 0},
                mapbox={
                    "center": layout.mapbox.Center(
                        lat=DEFAULT_UK_MAP_CENTRE["lat"],
                        lon=DEFAULT_UK_MAP_CENTRE["lon"],
                    )
                },
            )
        if colour_config.is_continuous:
            fig.update_layout(
                coloraxis=dict(
                    colorscale=colour_scale,
                    colorbar=dict(
                        bgcolor=plot_background_colour,
                        bordercolor=plot_background_colour,
                        tickfont=dict(
                            family=font_config.font_face,
                            size=font_config.font_size,
                            color=font_config.font_colour,
                        ),
                        # orientation="h",
                        x=0,
                        y=0.5,
                        ypad=300,
                        title=dict(
                            font=dict(color=font_config.font_colour),
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
    """Wrapper for running `get_dash_app` as a `JupyterDash` instance.

    Args:
        input_output_ts: `InterRegionInputOutputTimeSeries` to visualise.
        **kwargs: parameters to add to `JupyterDash` config.

    Returns:
        `JupyterDash` instance
    """
    app: JupyterDash = get_dash_app(input_output_ts, **kwargs)
    # app.run_server(mode='jupyterlab', port = 8090, dev_tools_ui=True, #debug=True,
    #                dev_tools_hot_reload =True, threaded=True)
    app.run_server(
        mode="inline",
        dev_tools_hot_reload=True,
    )
    # app.run_server(mode="inline", dev_tools_hot_reload=True)
    return app


def get_server_dash(
    input_output_ts: Optional[InterRegionInputOutputTimeSeries] = None,
    config_data: InputOutputConfigType
    | DateConfigType
    | None = CONFIG_2015_TO_2017_QUARTERLY,
    auth: bool = True,
    auth_db_path: DBPathType = DB_PATH,
    all_cities: bool = False,
    path_prefix: str = DEFAULT_SERVER_PATH,
    input_output_model_cls: Type[InterRegionInputOutput] = InterRegionInputOutputUK2017,
    **kwargs,
) -> tuple[Dash, InterRegionInputOutputTimeSeries]:
    """Wrapper for running `get_dash_app` with authentication.

    Args:
        input_output_ts: `InterRegionInputOutputTimeSeries` to visualise.
        config_data: Configuration to use is `input_output_ts` is `None`.
        auth: whether to require a password to access the visualisation.
        auth_db_path: path to authetication credentials file.
        all_cities: whether to use all known, available cities.
        path_prefix: url path to to app.
        input_output_model_cls: exact config of `InterRegionInputOutput`
            (inheritance etc.) to use
        **kwargs: parameters to add to `get_dash_app` call.

    Returns:
        `tuple` of `Dash` app instance and `InterRegionInputOutputTimeSeries`
        (or `subclass`) config.
    """
    path_prefix = enforce_start_str(path_prefix, PATH_SPLIT_CHAR, True)
    if not input_output_ts:
        if not config_data:
            logger.error(
                f"No config data specified. Returning to default, this may be removed in future."
            )
            config_data = CONFIG_2015_TO_2017_QUARTERLY
        if config_data == CONFIG_2015_TO_2017_QUARTERLY:
            logger.info("Using default config_data configuration")
        if all_cities:
            logger.info("Using almost all UK cities (currently only England).")
            almost_all_cities: dict[str, str] = get_all_centre_for_cities_dict()
            input_output_ts = date_io_time_series(
                date_conf=config_data,
                regions=almost_all_cities,  # type: ignore
                input_output_model_cls=input_output_model_cls,
            )
        else:
            input_output_ts = date_io_time_series(
                date_conf=config_data,
                input_output_model_cls=input_output_model_cls,
            )  # type: ignore
    assert input_output_ts, "No InputOuput TimeSeries to visualise"
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
    return fastapi_server_app, input_output_ts


def run_server_dash(
    port: int = DEFAULT_SERVER_PORT,
    host: str = DEFAULT_SERVER_HOST_IP,
    **kwargs,
) -> None:
    """Run `get_server_dash` as a `FastAPI` app via `uvicorn`.

    Args:
        port: what port app is run on.
        host: `IP` address of app server.

    Returns:
        None
    """
    # dash_app: Dash = get_server_dash(input_output_ts,**kwargs)
    app: FastAPI
    io_time_series: InterRegionInputOutputTimeSeries
    app, io_time_series = get_server_dash(**kwargs)
    # yield io_time_series

    # server.mount("/dash", WSGIMiddleware(dash_app.server))

    # [print(route) for route in app.routes]
    uvicorn.run(app, port=port, host=host)


# $ jupyter labextension install @jupyter-widgets/jupyterlab-manager keplergl-jupyter


# app.run_server(mode='jupyterlab', port = 8090, dev_tools_ui=True, #debug=True,
#               dev_tools_hot_reload =True, threaded=True)
# if __name__ == "__main__":
#     run_server_dash()
