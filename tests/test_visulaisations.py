#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from geopandas import GeoDataFrame

from regional_input_output.models import InterRegionInputOutput
from regional_input_output.visualisation import (
    add_mapbox_edges,
    convert_geom_for_mapbox,
    draw_ego_flows_network,
    plot_iterations,
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


def test_draw_ego_flows_network(three_cities_results: InterRegionInputOutput) -> None:
    fig = draw_ego_flows_network(
        three_cities_results.region_data,
        three_cities_results.y_ij_m_model,
        "Manchester",
        "Agriculture",
    )
    assert fig.layout["legend"]["title"]["text"] == "Agriculture flows from Manchester"


class TestPlotIterations:

    """Test plotting model iterations."""

    def test_exports_plot(self, three_cities_io) -> None:
        """Test plotting exports iterations and title."""
        pass

    def test_imports_plot(self, three_cities_io) -> None:
        """Test plotting imports iterations and title."""
        pass

    def test_flows_plot(self, three_cities_results) -> None:
        """Test plotting flows iterations and title."""
        with pytest.raises(ImportError):
            plot = plot_iterations(three_cities_results.y_ij_m_model, "flows")
