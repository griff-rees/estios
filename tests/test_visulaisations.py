#!/usr/bin/env python
# -*- coding: utf-8 -*-

from geopandas import GeoDataFrame

from regional_input_output.input_output_models import InterRegionInputOutput
from regional_input_output.visualisation import (
    add_mapbox_edges,
    convert_geom_for_mapbox,
    draw_ego_flows_network,
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
    assert (
        fig.layout["legend"]["title"]["text"] == "Flows from Manchester (and Regions)"
    )


def test_draw_ego_flows_network(three_cities_results: InterRegionInputOutput) -> None:
    fig = draw_ego_flows_network(
        three_cities_results.region_data,
        three_cities_results.y_ij_m_model,
        "Manchester",
        "Agriculture",
    )
    assert (
        fig.layout["legend"]["title"]["text"] == "Flows from Manchester (and Regions)"
    )
