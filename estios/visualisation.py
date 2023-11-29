#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from dataclasses import dataclass
from itertools import cycle
from logging import getLogger
from typing import Callable, Final, Iterable, Optional, Union

from dotenv import load_dotenv
from geopandas import GeoDataFrame
from pandas import DataFrame, MultiIndex, Series
from plotly.colors import qualitative
from plotly.express import bar, line, scatter_mapbox, set_mapbox_access_token
from plotly.graph_objects import Figure, Scattermapbox

from .calc import LATEX_e_i_m, LATEX_m_i_m, LATEX_y_ij_m
from .uk.regions import CENTRE_FOR_CITIES_EPSG, CENTRE_FOR_CITIES_REGION_COLUMN
from .utils import OTHER_CITY_COLUMN, filter_y_ij_m_by_city_sector, log_x_or_return_zero

logger = getLogger(__name__)

load_dotenv()
try:
    set_mapbox_access_token(os.environ["MAPBOX"])
except KeyError:
    logger.warning("MAPBOX access token not found in local .env file.")

# These can be uncommented to use Mapbox's native vector format via a key in a local .env file
# MAPBOX_STYLE: Final[str] = "dark"
MAPBOX_DARKMODE_MAP_CONFIG: Final[str] = "carto-darkmatter"
MAPBOX_STYLE: Final[str] = MAPBOX_DARKMODE_MAP_CONFIG
JOBS_COLUMN: Final[str] = "Total Jobs 2017"
ZOOM_DEFAULT: Final[float] = 4.7

MODEL_APPREVIATIONS: Final[dict[str, str]] = {
    "export": LATEX_e_i_m,
    "import": LATEX_m_i_m,
    "flows": LATEX_y_ij_m,
}
DEFAULT_REGION_PALATE: Final[list[str]] = qualitative.Plotly

DEFAULT_FONT_COLOUR: Final[str] = "white"
DEFAULT_FONT_FACE: Final[str] = "Georgia"
DEFAULT_FONT_SIZE: Final[int] = 12


@dataclass
class FontConfig:
    font_face: str = DEFAULT_FONT_FACE
    font_size: int = DEFAULT_FONT_SIZE
    font_colour: str = DEFAULT_FONT_COLOUR


def generate_colour_scheme(
    regions: Iterable[str],
    scheme_options: Iterable[str] = DEFAULT_REGION_PALATE,
) -> dict[str, str]:
    """Return a `dict` of region names and allocated colour.

    Args:
        regions: iterable of region names.
        scheme_options: colour cycle to apply to regions.

    Returns:
        A `dict` of region names to colour values (in `str`).
    """
    colour_cycle: cycle[str] = cycle(scheme_options)
    return {region: next(colour_cycle) for region in regions}


def plot_iterations(
    df: DataFrame,
    model_variable: str,
    model_abbreviations: dict[str, str] = MODEL_APPREVIATIONS,
    **kwargs,
) -> Figure | None:
    """Plot iterations of exports (e) or imports (m)."""
    if model_variable in model_abbreviations:
        column_char: str = model_abbreviations[model_variable]
        columns: list[str] = [col for col in df.columns.values if column_char in col]
    else:
        logger.error(model_variable, "not implemented for plotting.")
        return None
    plot_df = df[columns]
    plot_df.index = [" ".join(label) for label in plot_df.index.values]
    region_names: list[str] = list(df.index.get_level_values(0).unique().values)
    if len(region_names) < 4:
        regions_title_str = f'{", ".join(region_names[:-1])} and {region_names[-1]}'
    else:
        regions_title_str = f"{len(region_names)} Cities"
    # print(plot_df.columns)
    return plot_df.transpose().plot(
        title=f"Iterations of {model_variable} between {regions_title_str}", **kwargs
    )


def mapbox_cities_fig(
    cities: GeoDataFrame,
    colour_column: str = CENTRE_FOR_CITIES_REGION_COLUMN,
    size_column: str = JOBS_COLUMN,
    zoom: float = ZOOM_DEFAULT,
    mapbox_style: str = MAPBOX_STYLE,
    **kwargs,
) -> Figure:
    """Return a `scatter_mapbox` layer of cities scaled by `size_column`.

    Args:
        cities: `GeoDataFrame` of cities with coordinates and attributes.
        colour_column: which `cities` column to set city colour by.
        size_column: which column in `cities` indicates city size.
        zoom: how far zoomed in the map should render by default.
        mapbox_style: style configuration for map colours etc.
        **kwargs: any additional parameters to pass to `scatter_mapbox`.

    Returns:
        Configured instances of `scatter_mapbox`, including with city
        coordinates converted via `convert_geom_for_mapbox`.
    """
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
        # mapbox_center=mapbox_center,
        **kwargs,
    )


def convert_geom_for_mapbox(
    geo_df: GeoDataFrame | Series,
    epsg_code: int = 4326,
    series_crs: str = CENTRE_FOR_CITIES_EPSG,
) -> GeoDataFrame:
    """Convert `geo_df` geometry as necessry for `mapbox`.

    Args:
        geo_df: `GeoDataFrame` or `Series` to convert `geometry`.
        epsg_code: setting of `epsg` for final results.
        series_crs: `crs` to set first if passed as `Series`.

    Returns:
        `GeoDataFrame` with `geometry` converted for `mapbox`.
    """
    if type(geo_df) is Series:
        geo_df = GeoDataFrame(DataFrame(geo_df).T, crs=series_crs)
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
    selected_sector: Optional[str] = None,
    plot_background_colour: Optional[str] = None,
    # round_flows: int = 2,
    # reverse_render_order: bool = True,
    colour_palette: Optional[dict[str, str]] = None,
    font_config: FontConfig = FontConfig(),
    # font_colour: Optional[str] = None,
    # font_face: Optional[str] = None,
    # font_size: Optional[str] = None,
    **kwargs,
) -> Figure:
    """Add line segments to a map layer for visualising trade flows.

    Args:
        origin_city: City line (flow) starts from.
        cities: All other citeis (not `orgin_city`) for lines (flows) to go to.
        fig: An existing `plotly` `Figure` instance to add this layer to.
        weight: `Series` of weights to apply to each line/flow.
        flow_type: Configuration `str` for rendering flow configuration
            (like arrow for direction).
        plot_line_scaling_func: Function to scale line width
        render_below: Whether to render lines below or above another element
            (likely city layer)
        selected_sector: Which sector is selected for rendering.
        plot_background_colour: Background colour for plot.
        colour_palette: Plot colour palette
        font_config: Font configuration
        **kwargs: Additional parameters to pass to `mapbox_destinations`

    Returns:
        `Plotly` `Figure` with `mabpox` lines rendered.
    """
    if fig is None:
        fig = Figure()
    # if reverse_render_order:
    #     cities = cities.iloc[::-1]
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
                # mode="lines+text",
                mode="lines",
                line={
                    "width": plot_line_scaling_func(dest_city_row["weight"]),
                    "color": colour_palette[dest_city_row.name]
                    if colour_palette
                    else None,
                },
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
    logger.warning(f"Updating plot of {selected_sector} flows from {origin_city.name}")
    fig.update_layout(
        legend=dict(
            title=(
                f"{selected_sector + ' f' if selected_sector else 'F'}"
                f"lows from {mapbox_origin.index[0]}"
            ),
            x=0,
            y=1,
            # traceorder="reversed",
            # title_font_family="Times New Roman",
            font=dict(
                family=font_config.font_face,
                size=font_config.font_size,
                color=font_config.font_colour,
            ),
            bgcolor=plot_background_colour,
            bordercolor=plot_background_colour,
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
    n_flows: Optional[Union[int, tuple[int, int]]] = 0,
    fig: Optional[Figure] = None,
    # in_vs_out_flow: bool = True,
    other_city_column_name: str = OTHER_CITY_COLUMN,
    zoom: float = ZOOM_DEFAULT,
    colour_column: str = CENTRE_FOR_CITIES_REGION_COLUMN,
    ui_slider_index_fix: int = 0,
    **kwargs,
) -> Figure:
    """Return a Figure drawing flows from selected_city in selected_sector.

    Todo:
        * Remove and rename int n_flows filter to filter_flows tuple
    """
    # region_data: GeoDataFrame = input_output_ts[current_year_index].region_data
    logger.info(f"{n_flows} flows of {selected_sector} for {selected_city}")
    if fig is None:
        fig = mapbox_cities_fig(region_data, zoom=zoom, colour_column=colour_column)
    selected_city_data: Series = region_data.loc[selected_city]
    flows: Series = filter_y_ij_m_by_city_sector(
        y_ij_m_results, selected_city, selected_sector
    )
    if n_flows:
        if isinstance(n_flows, int):
            # This will probably be deprecated
            flows = flows.sort_values()[-n_flows:]
        else:
            flows = flows.sort_values()[n_flows[0] : n_flows[1] + ui_slider_index_fix]
    flows_city_data: GeoDataFrame = region_data.loc[
        flows.index.get_level_values(other_city_column_name)
    ]
    fig = add_mapbox_edges(
        selected_city_data,
        flows_city_data,
        fig,
        flows,
        selected_sector=selected_sector,
        **kwargs,
    )
    title_prefix: str = f"Top {n_flows} " if n_flows else "No "
    title: str = title_prefix + f"{selected_sector} flows from {selected_city}"
    fig.update_layout(title=title)
    return fig


def sector_flows_bar_chart(
    y_ij_m_results: Series,
    selected_city: str,
    selected_sector: str,
    other_city_column_name: str = OTHER_CITY_COLUMN,
    flow_type: str = "Export",
    dash_render: bool = False,
    sort_regions: Optional[bool] = True,
    y_axis_type: Optional[str] = "log",
    text_auto: str = ".2s",
    axis_colour: Optional[str] = "white",
    plot_background_colour: Optional[str] = None,
    colour_column: Optional[Union[str, Series]] = None,
    colour_palette: Optional[dict[str, str]] = None,
    dash_font_config: Optional[FontConfig] = FontConfig(),
    # font_colour: Optional[str] = None,
    # font_face: Optional[str] = None,
    # font_size: Optional[str] = None,
) -> Figure:
    """Plot a bar chart of ordered sector flows from a region.

    Todo:
        * Factor out the filter call.
        * Better solution for setting colour_column index
    """
    flows: Series = filter_y_ij_m_by_city_sector(
        y_ij_m_results, selected_city, selected_sector
    )
    if isinstance(flows.index, MultiIndex):
        flows.index = flows.index.get_level_values(other_city_column_name)
    if sort_regions:
        flows = flows.sort_values(ascending=sort_regions)
    if colour_column == "index":
        logger.warning("Using `index` for colour selection, will be refactored")
        colour_column = flows.index
    # flow_proposition: str = 'from' if flow_type == 'Export' else 'to'
    # title: str = f"{selected_city} economic flows {flow_proposition} {selected_city}"
    title: str = (
        "Economic Flows "
        + ("From " if flow_type == "Export" else "To ")
        + selected_city
    )
    x_axis_label: str = (
        "Importing Cities" if flow_type == "Export" else "Exporting Cities"
    )
    y_axis_label: str = f"{flow_type}s of {selected_sector} (£)"
    chart: Figure = bar(
        flows,
        text_auto=text_auto,
        color=colour_column,
        title=title,
        color_discrete_map=colour_palette,
    )
    chart.update_xaxes(title_text=x_axis_label)
    if y_axis_type:
        y_axis_label = f"{y_axis_type} of " + y_axis_label
        chart.update_yaxes(title_text=y_axis_label, type=y_axis_type, color=axis_colour)
    else:
        chart.update_yaxes(title_text=y_axis_label)
    if dash_render and dash_font_config:
        chart.update_xaxes(title_text=None, color=axis_colour)
        # chart.update_xaxes(visible=False)
        # fig.update_yaxes(visible=False, showticklabels=False)
        # chart.update_xaxes(visible=False, color=axis_colour)
        chart.update_layout(
            font_family=dash_font_config.font_face,
            title_font_color=dash_font_config.font_colour,
            paper_bgcolor=plot_background_colour,
            plot_bgcolor=plot_background_colour,
            showlegend=False,
            margin=dict(t=60),
        )
        chart.update_traces(
            textfont_color=dash_font_config.font_colour,
        )
    return chart


def time_series_line(
    time_series: DataFrame,
    # attribute: str = "employment",
    # region: str = "Manchester",
    # dates: Optional[list[Union[int, date]]] = None,
    transpose: bool = True,
    labels: dict = {"AREA_NAME": "Region"},
    **kwargs,
) -> Figure:
    """Return a `plotly` `Figure` with a `line` plot of a `time_series`.

    Args:
        time_series: `DataFrame` of values per region over time.
        transpose: whether to transpose `times_series` prior to plotting.
        labels: `dict` of plot label configuration.
        **kwargs: additional parameters to apply to the `plotly` `line`
            function call.

    Returns:
        A `plotly` `Figure` with a `plotly.line`, primarily designed for a
        time series.
    """
    if transpose:
        time_series = time_series.T
    return line(time_series, labels=labels, **kwargs)
    # return line(time_series, labels=labels)


def flow_plot_pe(flow_result_list, geolist, year=None, color="red", save=False):
    """Flow plot using `matplotlib`.

    Warning:
        Requires `matplotlib` imported as `plt` to work.
    """
    color_index = 0
    for i in flow_result_list.Sector.unique():
        fig, ax = plt.subplots(figsize=(20, 30))
        Sector_df_temp = flow_result_list[flow_result_list["Sector"] == i]
        Sector_df_temp.loc[:, "Linewidth"] = (
            Sector_df_temp["yij_pe"] / (Sector_df_temp["yij_pe"].max())
        ) * 5
        Sector_df_temp.plot(
            linewidths=Sector_df_temp["Linewidth"],
            ax=ax,
            color=color[color_index],
            alpha=0.8,
        )
        color_index = color_index + 1
        text_size_temp = (Sector_df_temp.groupby("City")["Linewidth"].agg("sum")) + (
            Sector_df_temp.groupby("Other_City")["Linewidth"].agg("sum")
        )
        Marker_size = (np.log(Markek_size / Markek_size.min()) + 2) * 3
        geolist_IO_temp = geolist.join(Marker_size, on="NAME1")
        boundary[boundary["CTRY22NM"] == "England"].boundary.plot(
            color="black", alpha=0.3, linestyle="-.", ax=ax
        )
        geolist_IO_temp.apply(
            lambda x: ax.annotate(
                text=x["NAME1"],
                xy=x.geometry.coords[0],
                ha="right",
                size=x.Linewidth,
                alpha=0.8,
            ),
            axis=1,
        )
        plt.title(i + " Per Employment", size=20)
        if save:
            plt.savefig("fig/" + i + "per_employment.jpg")
