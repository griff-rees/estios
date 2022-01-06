#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

# These can be uncommented to use Mapbox's native vector format via a key in a local .env file
from datetime import date
from logging import getLogger
from typing import Callable, Final, Optional, Union

from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from dotenv import load_dotenv
from geopandas import GeoDataFrame
from jupyter_dash import JupyterDash
from pandas import DataFrame, Series
from plotly.express import bar, scatter_geo, scatter_mapbox, set_mapbox_access_token
from plotly.graph_objects import Figure, Scattermapbox

from .input_output_func import LATEX_e_i_m, LATEX_m_i_m, LATEX_y_ij_m
from .input_output_models import InterRegionInputOutputTimeSeries
from .uk_data.utils import (
    CENTRE_FOR_CITIES_EPSG,
    CENTRE_FOR_CITIES_REGION_COLUMN,
    EMPLOYMENT_QUARTER_DEC_2017,
)
from .utils import OTHER_CITY_COLUMN, filter_y_ij_m_by_city_sector, log_x_or_return_zero

logger = getLogger(__name__)

load_dotenv()
try:
    set_mapbox_access_token(os.environ["MAPBOX"])
except KeyError:
    logger.warning("MAPBOX access token not found in local .env file.")

# MAPBOX_STYLE: Final[str] = "dark"
MAPBOX_STYLE: Final[str] = "carto-darkmatter"
JOBS_COLUMN: Final[str] = "Total Jobs 2017"
ZOOM_DEFAULT: Final[float] = 4.7
EXTERNAL_STYLESHEETS: Final[list[str]] = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]

MODEL_APPREVIATIONS: Final[dict[str, str]] = {
    "export": LATEX_e_i_m,
    "import": LATEX_m_i_m,
    "flows": LATEX_y_ij_m,
}


def plot_iterations(
    df: DataFrame,
    model_variable: str,
    model_abbreviations: dict[str, str] = MODEL_APPREVIATIONS,
    **kwargs,
) -> Figure:
    """Plot iterations of exports (e) or imports (m)."""
    if model_variable in model_abbreviations:
        column_char: str = model_abbreviations[model_variable]
        columns: list[str] = [col for col in df.columns.values if column_char in col]
    else:
        print(model_variable, "not implemented for plotting.")
        return
    plot_df = df[columns]
    plot_df.index = [" ".join(label) for label in plot_df.index.values]
    region_names: list[str] = list(df.index.get_level_values(0).unique().values)
    if len(region_names) < 4:
        regions_title_str = f'{", ".join(region_names[:-1])} and {region_names[-1]}'
    else:
        regions_title_str = f"{len(region_names)} Cities"
    print(plot_df.columns)
    return plot_df.transpose().plot(
        title=f"Iterations of {model_variable}s between {regions_title_str}", **kwargs
    )


def mapbox_cities_fig(
    cities: GeoDataFrame,
    colour_column: str = CENTRE_FOR_CITIES_REGION_COLUMN,
    size_column: str = JOBS_COLUMN,
    zoom: float = ZOOM_DEFAULT,
    mapbox_style: str = MAPBOX_STYLE,
    **kwargs,
) -> Figure:
    mapbox_cities = convert_geom_for_mapbox(cities)
    return scatter_mapbox(
        mapbox_cities,
        lat="lat",
        lon="lon",
        color=colour_column,
        hover_name=mapbox_cities.index,
        size=size_column,
        zoom=zoom,
        mapbox_style=mapbox_style,
        **kwargs,
    )


def convert_geom_for_mapbox(
    geo_df: Union[GeoDataFrame, Series],
    epsg_code: int = 4326,
    initial_crs: str = CENTRE_FOR_CITIES_EPSG,
) -> GeoDataFrame:
    if type(geo_df) is Series:
        geo_df = GeoDataFrame(DataFrame(geo_df).T, crs=CENTRE_FOR_CITIES_EPSG)
    mapbox_geo_df: GeoDataFrame = geo_df.copy()
    mapbox_geo_df["lon"] = mapbox_geo_df.to_crs(epsg=epsg_code).geometry.x
    mapbox_geo_df["lat"] = mapbox_geo_df.to_crs(epsg=epsg_code).geometry.y
    return mapbox_geo_df


def add_mapbox_edges(
    origin_city: Series,
    cities: GeoDataFrame,
    fig: Optional[Figure] = None,
    weight: Optional[Series] = None,
    flow_type: str = "->",
    plot_line_scaling_func: Callable[[float], Optional[float]] = log_x_or_return_zero,
    render_below: bool = True,
    # round_flows: int = 2,
    **kwargs,
) -> Figure:
    if fig is None:
        fig = Figure()
    mapbox_origin: GeoDataFrame = convert_geom_for_mapbox(origin_city)
    mapbox_destinations: GeoDataFrame = convert_geom_for_mapbox(cities)
    logger.warning("Check add_mapbox_edges weight indexing by values")
    mapbox_destinations["weight"] = weight.values if weight is not None else 1.0
    # else:
    # mapbox_destinations['weight'] = [2*x + 1 for x in range(len(mapbox_destinations.index))]
    mapbox_destinations.apply(
        lambda dest_city_row, fig: fig.add_trace(
            Scattermapbox(
                lon=[mapbox_origin.iloc[0]["lon"], dest_city_row["lon"]],
                lat=[mapbox_origin.iloc[0]["lat"], dest_city_row["lat"]],
                mode="lines",
                line={"width": plot_line_scaling_func(dest_city_row["weight"])},
                # name=f"{mapbox_origin.iloc[0].index} {flow_type} {dest_city_row.index}"
                # name=f"{dest_city_row.name} {flow_type} £{dest_city_row['weight']:,.2f}",
                name=f"{dest_city_row.name} £{dest_city_row['weight']:,.2f}",
                # text=f"{dest_city_row.name}: {dest_city_row['weight']}",
                # hoverinfo="text",
                # below=fig.data[0]['name'],
                **kwargs,
            )
        ),
        args=(fig,),
        axis=1,
    )
    if render_below:
        fig.data = (
            fig.data[-len(mapbox_destinations.index) :]
            + fig.data[: -len(mapbox_destinations.index)]
        )
    fig.update_layout(
        legend=dict(title=f"Flows from {mapbox_origin.index[0]} (and Regions)")
    )
    return fig


def draw_ego_flows_network(
    region_data: GeoDataFrame,
    y_ij_m_results: DataFrame,
    selected_city: str,
    selected_sector: str,
    n_flows: Optional[int] = 0,
    fig: Optional[Figure] = None,
    # in_vs_out_flow: bool = True,
    other_city_column_name: str = OTHER_CITY_COLUMN,
    **kwargs,
) -> Figure:
    # region_data: GeoDataFrame = input_output_ts[current_year_index].region_data
    if fig is None:
        fig = mapbox_cities_fig(region_data)
    selected_city_data: Series = region_data.loc[selected_city]
    flows: Series = filter_y_ij_m_by_city_sector(
        y_ij_m_results, selected_city, selected_sector
    )
    if n_flows:
        flows = flows.sort_values()[-n_flows:]
    flows_city_data: GeoDataFrame = region_data.loc[
        flows.index.get_level_values(other_city_column_name)
    ]
    fig = add_mapbox_edges(selected_city_data, flows_city_data, fig, flows, **kwargs)
    title_prefix: str = f"Top {n_flows} " if n_flows else "No "
    title: str = title_prefix + f"{selected_sector} flows from {selected_city}"
    fig.update_layout(title=title)
    return fig


def get_dash_app(
    input_output_ts: InterRegionInputOutputTimeSeries,
    external_stylesheets: list[str] = EXTERNAL_STYLESHEETS,
    default_date: date = EMPLOYMENT_QUARTER_DEC_2017,
    default_top_sectors: int = 4,
    default_sectors_marker_hops: int = 2,
    default_region: str = "Manchester",
    default_sector: str = "Production",
    date_fmt: str = "%b %y",
) -> Dash:
    from IPython import get_ipython

    app: Dash = (
        JupyterDash(__name__, external_stylesheets=external_stylesheets)
        if get_ipython()
        else Dash(__name__, external_stylesheets=external_stylesheets)
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
            dcc.Store(id="current_date_index"),
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
) -> None:
    app: JupyterDash = get_dash_app(input_output_ts)
    # app.run_server(mode='jupyterlab', port = 8090, dev_tools_ui=True, #debug=True,
    #                dev_tools_hot_reload =True, threaded=True)
    app.run_server(mode="inline", dev_tools_hot_reload=True, **kwargs)
    return app


# $ jupyter labextension install @jupyter-widgets/jupyterlab-manager keplergl-jupyter


# app.run_server(mode='jupyterlab', port = 8090, dev_tools_ui=True, #debug=True,
#               dev_tools_hot_reload =True, threaded=True)
