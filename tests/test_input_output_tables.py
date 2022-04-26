#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from pandas import DataFrame

from regional_input_output.input_output_tables import (
    COVID_FLAGS_COLUMN,
    AggregatedSectorDictType,
    InputOutputExcelTable,
    load_employment_by_region_and_sector_csv,
    load_io_table_excel,
    load_region_employment_excel,
)
from regional_input_output.utils import aggregate_rows, filter_by_region_name_and_type


@pytest.fixture
def test_ons_io_table() -> InputOutputExcelTable:
    return InputOutputExcelTable()


FINANCIAL_AGG: str = "Financial and insurance"
REAL_EST_AGG: str = "Real estate"


class TestLoadingONSIOTableData:
    def test_load_ons_io_table(self) -> None:
        """Test loading a UK 2017 economic input-outpute table."""
        io_2017: DataFrame = load_io_table_excel()
        assert "Taxes less subsidies on production" in io_2017.index

    def test_ons_io_table_export(self, test_ons_io_table) -> None:
        """Test loading and managing an ONS Input Output excel file."""
        assert test_ons_io_table.sectors.tail().index[0] == "CPA_R93"
        assert len(test_ons_io_table.sectors) == 105
        assert (
            test_ons_io_table.code_io_table.loc["CPA_A02", "CPA_C101"]
            == 1.512743278316663e-06
        )

    def test_aggregated_sectors_dict(self, test_ons_io_table) -> None:
        """Test creating a dictionay to aggregate sectors."""
        TEST_SECTORS: list[str] = ["CPA_K64", "CPA_K65.1-2 & K65.3", "CPA_K66"]
        sectors_aggregated: AggregatedSectorDictType = (
            test_ons_io_table._aggregated_sectors_dict()
        )
        assert sectors_aggregated[FINANCIAL_AGG] == TEST_SECTORS

    def test_ons_io_table_aggregation(self, test_ons_io_table) -> None:
        """Test loading and manaing an ONS Input Output excel file."""
        FIN_REAL_IO: float = 29562.858422906436
        aggregated_io_table: DataFrame = test_ons_io_table.get_aggregated_io_table()
        assert aggregated_io_table.loc[FINANCIAL_AGG, REAL_EST_AGG] == FIN_REAL_IO

    @pytest.mark.xfail(reason="requires an external data file")
    def test_ons_io_2015(self) -> None:
        """Test loading 2015 IO table data."""
        io_2015: DataFrame = load_io_table_excel("data/2015detailedioatsbb18(1).xls")


@pytest.fixture
def national_jobs() -> DataFrame:
    return load_region_employment_excel("15. United Kingdom")


@pytest.fixture
def aggregated_city_sector() -> DataFrame:
    city_sector: DataFrame = load_employment_by_region_and_sector_csv()
    return aggregate_rows(city_sector, True)


THREE_CITY_REGIONS: dict[str, str] = {
    "Leeds": "Yorkshire and the Humber",
    "Liverpool": "North West",
    "Manchester": "North West",
}


class TestLoadingEmploymentData:

    DATE_1997: str = "1997-03-01"
    DATE_2021: str = "2021-06-01"
    GREATER_MANCHESTER: str = "combauth:Greater Manchester"

    def test_load_ons_jobs(self, national_jobs) -> None:
        """Test importing National data from an ONS export."""
        assert not national_jobs[COVID_FLAGS_COLUMN][self.DATE_1997]
        assert national_jobs[COVID_FLAGS_COLUMN][self.DATE_2021]

    def test_aggregate_rows(self, national_jobs) -> None:
        """Test aggregating jobs data via rows in a time series."""
        aggregate_jobs: DataFrame = aggregate_rows(national_jobs)
        assert aggregate_jobs[FINANCIAL_AGG][self.DATE_1997] == 1085
        assert aggregate_jobs[REAL_EST_AGG][self.DATE_2021] == 647

    def test_load_nomis_city_sector(self, aggregated_city_sector) -> None:
        """Test loading and aggregating employment by city and sector."""
        assert aggregated_city_sector[REAL_EST_AGG][self.GREATER_MANCHESTER] == 28000

    def test_filtering_for_specific_regions(self, aggregated_city_sector) -> None:
        """Test filtering for specific regions."""
        filtered_aggregate_city: DataFrame = filter_by_region_name_and_type(
            aggregated_city_sector, THREE_CITY_REGIONS.keys()
        )
        for city in THREE_CITY_REGIONS:
            assert city in filtered_aggregate_city.index