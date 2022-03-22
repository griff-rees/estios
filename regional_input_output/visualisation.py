#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from logging import getLogger
from typing import Callable, Final, Optional, Union

from dotenv import load_dotenv
from geopandas import GeoDataFrame
from pandas import DataFrame, Series
from plotly.express import bar, scatter_geo, scatter_mapbox, set_mapbox_access_token
from plotly.graph_objects import Figure, Scattermapbox

from .calc import LATEX_e_i_m, LATEX_m_i_m, LATEX_y_ij_m
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

# These can be uncommented to use Mapbox's native vector format via a key in a local .env file
# MAPBOX_STYLE: Final[str] = "dark"
MAPBOX_STYLE: Final[str] = "carto-darkmatter"
JOBS_COLUMN: Final[str] = "Total Jobs 2017"
ZOOM_DEFAULT: Final[float] = 4.7

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
    fig.update_layout(
        legend=dict(
            title=f"Flows from {mapbox_origin.index[0]}",
            x=0,
            y=1,
            # traceorder="reversed",
            # title_font_family="Times New Roman",
            font=dict(family="Courier", size=12, color="white"),
            bgcolor="rgba(0,0,0,0)",
            bordercolor="rgba(0,0,0,0)",
            # "white",
            borderwidth=10,
        )
    )
    if render_below:
        fig.data = (
            fig.data[-len(mapbox_destinations.index) :]
            + fig.data[: -len(mapbox_destinations.index)]
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
    zoom: float = ZOOM_DEFAULT,
    colour_column: str = CENTRE_FOR_CITIES_REGION_COLUMN,
    **kwargs,
) -> Figure:
    # region_data: GeoDataFrame = input_output_ts[current_year_index].region_data
    if fig is None:
        fig = mapbox_cities_fig(region_data, zoom=zoom, colour_column=colour_column)
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
