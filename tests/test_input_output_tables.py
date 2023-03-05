#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path

import pytest
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from pymrio import MRIOMetaData

from estios.input_output_tables import (
    ACQUISITION_NET_VALUABLES_DISPOAL_COLUMN_NAME,
    COVID_FLAGS_COLUMN,
    AggregatedSectorDictType,
    InputOutputCPATable,
    InputOutputTable,
    InputOutputTableOECD,
    _pymrio_download_wrapper,
)
from estios.sources import (
    DOI_URL_PREFIX,
    AutoDownloadPermissionError,
    pandas_from_path_or_package,
)
from estios.uk import io_table_1841
from estios.uk.input_output_tables import InputOutputTableUK1841, InputOutputTableUK2017
from estios.uk.ons_employment_2017 import (
    CITY_SECTOR_REGION_PREFIX,
    load_employment_by_region_and_sector_csv,
    load_region_employment_excel,
)
from estios.utils import aggregate_rows, filter_by_region_name_and_type


@pytest.fixture
def ons_io_2017_table() -> InputOutputCPATable | InputOutputTableUK2017:
    return InputOutputTableUK2017()


@pytest.fixture
def io_1841_table() -> InputOutputTable:
    return InputOutputTableUK1841()


FINANCIAL_AGG: str = "Financial and insurance"
REAL_EST_AGG: str = "Real estate"


class TestLoadingONSIOTableData:

    """Test loading and manipulating an InputOutputCPATable from ONS data."""

    def test_repr(self, ons_io_2017_table) -> None:
        """Test correct repr format."""
        assert repr(ons_io_2017_table) == "InputOutputTableUK2017(sectors_count=105)"

    def test_load_ons_io_2017_table(self, ons_io_2017_table) -> None:
        """Test loading a UK 2017 economic input-outpute table.

        Todo:
            * Expand to more comprehensive test
        """
        assert len(ons_io_2017_table.all_input_row_labels) == len(
            ons_io_2017_table.all_input_rows
        )
        assert len(ons_io_2017_table.all_output_column_labels) == len(
            ons_io_2017_table.all_output_columns
        )
        assert (
            "Taxes less subsidies on production"
            in ons_io_2017_table.all_input_row_labels
        )
        assert (
            ACQUISITION_NET_VALUABLES_DISPOAL_COLUMN_NAME
            in ons_io_2017_table.all_output_column_labels
        )

    def ons_io_2017_table_export(self, ons_io_2017_table) -> None:
        """Test loading and managing an ONS Input Output excel file."""
        assert ons_io_2017_table.sectors.tail().index[0] == "CPA_R93"
        assert len(ons_io_2017_table.sectors) == 105
        assert (
            ons_io_2017_table.code_io_table.loc["CPA_A02", "CPA_C101"]
            == 1.512743278316663e-06
        )

    def test_aggregated_sectors_dict(self, ons_io_2017_table) -> None:
        """Test creating a dictionay to aggregate sectors."""
        TEST_SECTORS: list[str] = ["CPA_K64", "CPA_K65.1-2 & K65.3", "CPA_K66"]
        sectors_aggregated: AggregatedSectorDictType = (
            ons_io_2017_table._aggregated_sectors_dict
        )
        assert sectors_aggregated[FINANCIAL_AGG] == TEST_SECTORS

    def test_ons_io_2017_table_aggregation(self, ons_io_2017_table) -> None:
        """Test loading and manaing an ONS Input Output excel file.

        Todo:
            * Expand test to incoprate pre-scaling
        """
        FIN_REAL_IO: float = 295628584229.06436
        aggregated_io_table: DataFrame = ons_io_2017_table.get_aggregated_io_table()
        assert aggregated_io_table.loc[FINANCIAL_AGG, REAL_EST_AGG] == FIN_REAL_IO

    @pytest.mark.xfail(reason="requires an external data file")
    def test_ons_io_2015(self, tmp_path_factory) -> None:
        """Test loading 2015 IO table data."""
        io_2015: DataFrame = pandas_from_path_or_package(
            url_or_path="https://www.ons.gov.uk/file?uri=/economy/nationalaccounts/supplyandusetables/datasets/ukinputoutputanalyticaltablesdetailed/2015detailed/2015detailedioatsbb18.xls",
            local_path=Path(tmp_path_factory.mktemp("test-ons-2015") / "test_2015.xls"),
        )
        assert io_2015.year == 2015

    # def ons_cpa_io_table_export(self, ons_io_2017_table) -> None:
    #     """Test loading and managing an ONS Input Output excel file."""
    #     assert ons_io_2017_table.sectors.tail().index[0] == "CPA_R93"
    #     assert len(ons_io_2017_table.sectors) == 105
    #     assert (
    #         ons_io_2017_table.code_io_table.loc["CPA_A02", "CPA_C101"]
    #         == 1.512743278316663e-06
    #     )

    # def test_aggregated_sectors_dict(self, ons_io_2017_table) -> None:
    #     """Test creating a dictionay to aggregate sectors."""
    #     TEST_SECTORS: list[str] = ["CPA_K64", "CPA_K65.1-2 & K65.3", "CPA_K66"]
    #     sectors_aggregated: AggregatedSectorDictType = (
    #         ons_io_2017_table._aggregated_sectors_dict
    #     )
    #     assert sectors_aggregated[FINANCIAL_AGG] == TEST_SECTORS

    # def ons_io_2017_table_aggregation(self, ons_io_2017_table) -> None:
    #     """Test loading and manaing an ONS Input Output excel file."""
    #     FIN_REAL_IO: float = 29562.858422906436
    #     aggregated_io_table: DataFrame = ons_io_2017_table.get_aggregated_io_table()


class TestLoadingCSVIOTable:

    """Test loading a csv for an InputOutputTable.

    Table from An input-output table for 1841 by
    SARA HORRELL, JANE HUMPHRIES, MARTIN WEALE
    in The Economic History Review, August 1994 see:
    https://doi.org/10.1111/j.1468-0289.1994.tb01390.x
    """

    empty_column: str = "Unnamed: 21"

    # def test_csv_io_table_export(self, test_csv_io_table) -> None:
    #     """Test loading and managing a csv Input Output file."""
    #     assert test_csv_io_table.sectors.tail().index[0] == "CPA_R93"
    #     assert len(ons_cpa_io_table.sectors) == 105

    def test_load_cvs_io_table(self, io_1841_table) -> None:
        """Test default `load_io_table_csv()`."""
        assert self.empty_column in io_1841_table.raw_io_table.columns
        assert set(io_table_1841.HISTORIC_UK_SECTORS).issubset(
            io_1841_table.raw_io_table.columns
        )
        assert set(io_table_1841.HISTORIC_UK_SECTORS).issubset(
            io_1841_table.raw_io_table.index
        )
        assert "Value added" in io_1841_table.full_io_table.index
        assert "Total" in io_1841_table.full_io_table.index
        assert self.empty_column not in io_1841_table.full_io_table.columns
        assert set(io_table_1841.HISTORIC_UK_SECTORS).issubset(
            io_1841_table.full_io_table.columns
        )
        assert set(io_table_1841.HISTORIC_UK_SECTORS).issubset(
            io_1841_table.full_io_table.index
        )
        assert io_1841_table.all_sectors == io_table_1841.HISTORIC_UK_SECTORS
        assert io_1841_table.all_regions == ("UK",)

    def test_1841_io_table_meta_data(self, io_1841_table) -> None:
        """Test loading meta data."""
        assert io_1841_table._raw_io_table_meta_data.name == io_table_1841.NAME
        assert io_1841_table._raw_io_table_meta_data.year == io_table_1841.YEAR
        assert io_1841_table._raw_io_table_meta_data.region == io_table_1841.REGION
        assert io_1841_table._raw_io_table_meta_data.authors == io_table_1841.AUTHORS
        assert io_1841_table._raw_io_table_meta_data.doi == io_table_1841.DOI
        assert (
            io_1841_table._raw_io_table_meta_data.url
            == DOI_URL_PREFIX + io_table_1841.DOI
        )

    def test_pymrio_csv(self, io_1841_table) -> None:
        """Test generated pyrmio table.

        Todo:
            * Fix the index name difference between `pymrio_table.Y`
              and `_pymrio_Z`
        """
        assert_frame_equal(io_1841_table.pymrio_table.Z, io_1841_table.pymrio_table._Z)
        assert_frame_equal(io_1841_table.pymrio_table.Y, io_1841_table.pymrio_table._Y)
        assert_frame_equal(
            io_1841_table.pymrio_table.Y_by_region("UK"),
            io_1841_table.core_final_demand,
        )
        assert_frame_equal(
            io_1841_table.pymrio_table.Z_by_region("UK"),
            io_1841_table.core_io_table,
        )

    # def io_1841_table_export(self, io_1841_table) -> None:
    #     """Test loading and managing a 1841 Input Output file."""
    #     assert io_1841_table.sectors.tail().index[0] == "CPA_R93"
    #     assert len(ons_io_2017_table.sectors) == 105
    #     assert (
    #         ons_cpa_io_table.code_io_table.loc["CPA_A02", "CPA_C101"]
    #         == 1.512743278316663e-06
    #     )

    # def test_aggregated_sectors_dict(self, ons_cpa_io_table) -> None:
    #     """Test creating a dictionay to aggregate sectors."""
    #     TEST_SECTORS: list[str] = ["CPA_K64", "CPA_K65.1-2 & K65.3", "CPA_K66"]
    #     sectors_aggregated: AggregatedSectorDictType = (
    #         ons_cpa_io_table._aggregated_sectors_dict()
    #     )
    #     assert sectors_aggregated[FINANCIAL_AGG] == TEST_SECTORS

    # def test_ons_cvs_table_aggregation(self, ons_cpa_io_table) -> None:
    #     """Test loading and manaing an csv Input Output excel file."""
    # def test_ons_cvs_table_aggregation(self, _ons_io_2017_table) -> None:
    #     """Test loading and manaing an 1841 Input Output excel file."""
    #     FIN_REAL_IO: float = 29562.858422906436
    #     aggregated_io_table: DataFrame = ons_cpa_io_table.get_aggregated_io_table()
    #     assert aggregated_io_table.loc[FINANCIAL_AGG, REAL_EST_AGG] == FIN_REAL_IO


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
            aggregated_city_sector,
            THREE_CITY_REGIONS.keys(),
            region_type_prefix=CITY_SECTOR_REGION_PREFIX,
        )
        for city in THREE_CITY_REGIONS:
            assert city in filtered_aggregate_city.index


@pytest.mark.remote_data
class TestInputOutputOECD:
    @pytest.mark.slow("Very slow by default.")
    def test_oecd_pymrio_wrapper_default(self) -> None:
        meta_data: MRIOMetaData = _pymrio_download_wrapper()
        assert meta_data.name == "OECD-ICIO"
        assert meta_data.version == "v2021"

    @pytest.mark.xfail
    def test_oecd_pymrio_wrapper_2018(self) -> None:
        meta_data: MRIOMetaData = _pymrio_download_wrapper(version="v2018")
        assert meta_data.name == "OECD-ICIO"
        assert meta_data.version == "v2018"
        # test_example: InputOutputTable = InputOutputTableOECD()

    def test_autoload_permission_error(self) -> None:
        with pytest.raises(AutoDownloadPermissionError):
            InputOutputTableOECD()
