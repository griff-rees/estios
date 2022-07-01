#!/usr/bin/env python
# -*- coding: utf-8 -*-

from geopandas import GeoDataFrame
from pandas import DataFrame

from regional_input_output.uk_data.regions import (
    UK_CITY_REGIONS,
    get_all_centre_for_cities_dict,
    load_and_join_centre_for_cities_data,
    load_centre_for_cities_csv,
    load_centre_for_cities_gis,
)


class TestLoadingCentreForCitiesData:

    SECTION_OF_COLUMNS: tuple[str, ...] = (
        "Commuting by Bicycle 2001  (%)",
        "Commuting by Bicycle 2011  (%)",
        "Commuting by Bus, Train or Metro 2001  (%)",
        "Commuting by Bus, Train or Metro 2011  (%)",
        "Commuting by Other Methods 2001  (%)",
        "Commuting by Other Methods 2011  (%)",
    )

    def test_load_centre_for_cities_csv(self) -> None:
        """Test loading default Centre for Cities csv from the local package."""
        centre_for_cities: DataFrame = load_centre_for_cities_csv()
        for section in self.SECTION_OF_COLUMNS:
            assert section in centre_for_cities.columns

    def test_load_centre_for_cities_geojson(self) -> None:
        """Test loading Centre for Cities GeoJSON as a GeoDataFrame."""
        cities_geo: GeoDataFrame = load_centre_for_cities_gis()
        assert "Leeds" in cities_geo["NAME1"].values

    def test_load_and_join(self) -> None:
        cities_geo: GeoDataFrame = load_and_join_centre_for_cities_data()
        assert "Leeds" in cities_geo.index
        for section in self.SECTION_OF_COLUMNS:
            assert section in cities_geo.columns


def test_get_all_cities() -> None:
    """Test generating city: region dictionary from Centre for Cities.

    Note:
        * Currently this filters Blackburn, Newcastle and all cities from
        Scotland and Wales,
        * Total English cities 50
    """
    test_dict: dict[str, str] = get_all_centre_for_cities_dict()
    assert len(test_dict) == 48
    for city, region in UK_CITY_REGIONS.items():
        assert test_dict[city] == region
