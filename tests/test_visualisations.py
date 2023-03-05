#!/usr/bin/env python
# -*- coding: utf-8 -*-

from geopandas import GeoDataFrame
from matplotlib.axes import Axes
from plotly.graph_objects import Figure

from estios.models import InterRegionInputOutput
from estios.visualisation import (
    add_mapbox_edges,
    convert_geom_for_mapbox,
    draw_ego_flows_network,
    generate_colour_scheme,
    plot_iterations,
    sector_flows_bar_chart,
)


def test_convert_single_location(three_cities_results: InterRegionInputOutput) -> None:
    manchester = three_cities_results.region_data.loc["Manchester"]
    geo_manchester = convert_geom_for_mapbox(manchester)
    assert type(geo_manchester) == GeoDataFrame


def test_add_mapbox_edges(three_cities_results: InterRegionInputOutput) -> None:
    manchester = three_cities_results.region_data.loc["Manchester"]
    other_cities = three_cities_results.region_data.loc[
        three_cities_results.region_data.index != "Manchester"
    ]
    fig = add_mapbox_edges(manchester, other_cities)
    assert fig.layout["legend"]["title"]["text"] == "Flows from Manchester"


class TestGenerateColourScheme:

    """Test generating colour schemes for region visualisation harmony."""

    def test_default(self, three_cities) -> None:
        CORRECT_COLOUR_SCHEME = {
            "Manchester": "#00CC96",
            "Leeds": "#636EFA",
            "Liverpool": "#EF553B",
        }
        scheme: dict[str, str] = generate_colour_scheme(three_cities)
        assert scheme == CORRECT_COLOUR_SCHEME

    def test_colour_cycles_if_more_cities_than_colours(self, three_cities) -> None:
        CORRECT_COLOUR_SCHEME = {
            "Manchester": "red",
            "Liverpool": "blue",
            "Leeds": "red",
        }
        scheme: dict[str, str] = generate_colour_scheme(three_cities, ["red", "blue"])
        assert scheme == CORRECT_COLOUR_SCHEME


class TestDrawEgoFlowsNetwork:

    """Test using draw_ego_flows_network edges with model results."""

    def test_draw_default(self, three_cities_results: InterRegionInputOutput) -> None:
        fig = draw_ego_flows_network(
            three_cities_results.region_data,
            three_cities_results.y_ij_m_model,
            "Manchester",
            "Agriculture",
        )
        assert (
            fig.layout["legend"]["title"]["text"] == "Agriculture flows from Manchester"
        )

    def test_draw_city_colours(
        self, three_cities_results: InterRegionInputOutput
    ) -> None:
        colour_palete: dict[str, str] = generate_colour_scheme(
            three_cities_results.regions
        )
        fig = draw_ego_flows_network(
            three_cities_results.region_data,
            three_cities_results.y_ij_m_model,
            "Manchester",
            "Agriculture",
        )
        assert (
            fig.layout["legend"]["title"]["text"] == "Agriculture flows from Manchester"
        )


class TestPlotIterations:

    """Test plotting model iterations.

    Todo:
        * Expand the import and export plot tests
    """

    # @pytest.mark.skip
    # def test_exports_plot(self, three_cities_io) -> None:
    #     """Test plotting exports iterations and title."""
    #     pass

    # @pytest.mark.skip
    # def test_imports_plot(self, three_cities_io) -> None:
    #     """Test plotting imports iterations and title."""
    #     pass

    def test_flows_iterations_plot(self, three_cities_results) -> None:
        """Test plotting flows iterations and title."""
        correct_title: str = (
            "Iterations of flows between Leeds, Liverpool and Manchester"
        )
        plot: Axes = plot_iterations(three_cities_results.y_ij_m_model, "flows")
        assert plot.get_title() == correct_title
        assert isinstance(plot, Axes)


class TestSectorFlowsBarChart:

    """Test plotting Flows Bar Chart"""

    def test_MultiIndex(self, three_cities_results) -> None:
        """Test managing MultiIndex plotting flows bar chart."""
        bar_chart: Figure = sector_flows_bar_chart(
            three_cities_results.y_ij_m_model, "Manchester", "Agriculture"
        )
        assert bar_chart.layout["xaxis"]["title"]["text"] == "Importing Cities"
        assert (
            bar_chart.layout["yaxis"]["title"]["text"]
            == "log of Exports of Agriculture (£)"
        )
