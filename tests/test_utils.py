#!/usr/bin/env python
# -*- coding: utf-8 -*-

from logging import DEBUG
from os import PathLike
from pathlib import Path

import pytest
from pandas import DataFrame, MultiIndex

from estios.server.dash_app import DEFAULT_SERVER_PATH, PATH_SPLIT_CHAR
from estios.sources import (
    FilePathType,
    download_and_save_file,
    extract_file_name_from_url,
)
from estios.uk.ons_population_projections import (
    ONS_ENGLAND_POPULATION_PROJECTIONS_FILE_NAME,
    ONS_ENGLAND_POPULATIONS_PROJECTION_2018_ZIP_URL,
)
from estios.uk.ons_uk_population_projections import ONS_UK_POPULATION_META_DATA
from estios.utils import (  # download_and_extract_zip_file,
    SECTOR_10_CODE_DICT,
    THREE_UK_CITY_REGIONS,
    enforce_end_str,
    enforce_start_str,
    generate_i_m_index,
    generate_ij_index,
    generate_ij_m_index,
    invert_dict,
    name_converter,
)


class TestMultiIndexeGenerators:

    """Test i_m, ij and ij_m MultiIndex generator functions."""

    def test_i_m_index(self) -> None:
        """Test correct hierarchical dimensions for an im index."""
        default_i_m_index: MultiIndex = generate_i_m_index()
        assert len(default_i_m_index) == len(THREE_UK_CITY_REGIONS) * len(
            SECTOR_10_CODE_DICT
        )
        assert set(default_i_m_index.get_level_values(0)) == set(THREE_UK_CITY_REGIONS)
        assert set(default_i_m_index.get_level_values(1)) == set(SECTOR_10_CODE_DICT)

    def test_ij_index(self) -> None:
        """Test correct hierarchical dimensions for an ij index."""
        default_i_m_index: MultiIndex = generate_ij_index()
        assert len(default_i_m_index) == len(THREE_UK_CITY_REGIONS) * len(
            THREE_UK_CITY_REGIONS
        )
        assert set(default_i_m_index.get_level_values(0)) == set(THREE_UK_CITY_REGIONS)
        assert set(default_i_m_index.get_level_values(1)) == set(THREE_UK_CITY_REGIONS)

    def test_ij_m_index(self) -> None:
        """Test correct hierarchical dimensions for an ij_m index."""
        default_i_m_index: MultiIndex = generate_ij_m_index()
        assert len(default_i_m_index) == (
            # Note i=j is excluded
            len(THREE_UK_CITY_REGIONS)
            * (len(THREE_UK_CITY_REGIONS) - 1)
            * len(SECTOR_10_CODE_DICT)
        )
        assert set(default_i_m_index.get_level_values(0)) == set(THREE_UK_CITY_REGIONS)
        assert set(default_i_m_index.get_level_values(1)) == set(THREE_UK_CITY_REGIONS)
        assert set(default_i_m_index.get_level_values(2)) == set(SECTOR_10_CODE_DICT)


class TestEnforcingStrPrefixSuffix:

    """Test enforcing prefix and suffix of strings."""

    def test_add_start_str(self) -> None:
        assert (
            enforce_start_str(DEFAULT_SERVER_PATH, PATH_SPLIT_CHAR, True)
            == DEFAULT_SERVER_PATH
        )

    def test_remove_start_str(self) -> None:
        assert (
            enforce_start_str(DEFAULT_SERVER_PATH, PATH_SPLIT_CHAR, False)
            == DEFAULT_SERVER_PATH[1:]
        )

    def test_add_tail_str(self) -> None:
        assert (
            enforce_end_str(DEFAULT_SERVER_PATH, PATH_SPLIT_CHAR, True)
            == DEFAULT_SERVER_PATH + PATH_SPLIT_CHAR
        )

    def test_remove_tail_str(self) -> None:
        assert (
            enforce_end_str(DEFAULT_SERVER_PATH, PATH_SPLIT_CHAR, False)
            == DEFAULT_SERVER_PATH
        )


def test_name_converter() -> None:
    FINAL_NAMES: list[str] = ["dog", "cat, the", "hare"]
    test_conversion_dict = {"cat": "cat, the"}
    test_names = ["dog", "cat", "hare"]
    assert name_converter(test_names, test_conversion_dict) == FINAL_NAMES


def test_invert_dict() -> None:
    test_dict = {"cat": 4, "dog": 3}
    correct_inversion = {4: "cat", 3: "dog"}
    assert invert_dict(test_dict)


@pytest.mark.remote_data
class TestDownloadingDataFiles:

    """Test downloading and storing datafiles, skipping if no internet connection."""

    jpg_url: str = "https://commons.wikimedia.org/wiki/File:Wassily_Leontief_1973.jpg"
    input_output_example_zip: str = ONS_ENGLAND_POPULATIONS_PROJECTION_2018_ZIP_URL
    zip_file_path: PathLike = ONS_ENGLAND_POPULATION_PROJECTIONS_FILE_NAME

    def test_extract_file_name_from_url(self) -> None:
        """Test a simple extractiong of a filename from a URL."""
        correct_file_name: str = self.jpg_url.split("/")[-1]
        assert extract_file_name_from_url(self.jpg_url) == correct_file_name

    @pytest.mark.xfail
    def test_download_extract_zip_custom_local_name(self, tmp_path) -> None:
        """Test downloading and extracting remote zip file to custom local path.

        Note:
            Original example:
                https://www.oecd.org/industry/ind/
                input-outputtableslatesteditionaccesstodata.htm

            raised http.client.IncompleteRead errors
        """
        # input_output_example_zip: str = "https://www.oecd.org/sti/ind/42163955.zip"
        # local_path: PathLike = "zaf2005.xls"
        local_file_name: str = "test_extract.csv"
        download_and_save_file(
            self.input_output_example_zip,
            tmp_path / local_file_name,
            zip_file_path=self.zip_file_path,
        )
        with open(tmp_path / local_file_name) as test_saved_file:
            assert test_saved_file.name.endswith(local_file_name)

    @pytest.mark.xfail
    def test_download_extract_zip(self, tmp_path, caplog, monkeypatch) -> None:
        """Test downloading and extracting remote zip file to same name."""
        monkeypatch.chdir(tmp_path)  # Enforce location to fit tmp_path
        with caplog.at_level(DEBUG):
            download_and_save_file(
                self.input_output_example_zip,
                zip_file_path=self.zip_file_path,
            )
            with open(self.zip_file_path) as test_saved_file:
                assert test_saved_file.name == str(self.zip_file_path)
        assert caplog.records[1].message == (
            f"'local_path' not specified, setting to '{self.zip_file_path}'"
        )

    def test_download_file_with_local_path(self, tmp_path) -> None:
        local_path: FilePathType = "leontief.jpg"
        download_and_save_file(self.jpg_url, tmp_path / local_path)
        with open(tmp_path / local_path) as test_saved_file:
            assert test_saved_file.name.endswith(str(local_path))

    def test_download_file_no_local_path(self, tmp_path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)  # Enforce location to fit tmp_path
        download_and_save_file(self.jpg_url)
        assert (
            Path(extract_file_name_from_url(self.jpg_url)).stat().st_size == 60983
        )  # Previous result: 60978

    def test_extract_file_name_from_url_query_path(self, caplog) -> None:
        assert ONS_UK_POPULATION_META_DATA.path
        assert isinstance(ONS_UK_POPULATION_META_DATA.path, Path)
        assert isinstance(ONS_UK_POPULATION_META_DATA.url, str)
        assert ONS_UK_POPULATION_META_DATA.path.name == extract_file_name_from_url(
            ONS_UK_POPULATION_META_DATA.url
        )

    def test_download_query_url_file_no_local_path(self, tmp_path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)  # Enforce location to fit tmp_path
        assert isinstance(ONS_UK_POPULATION_META_DATA.url, str)
        download_and_save_file(ONS_UK_POPULATION_META_DATA.url)
        assert (
            Path(extract_file_name_from_url(ONS_UK_POPULATION_META_DATA.url))
            .stat()
            .st_size
            == 371200  # Previous result: 367616
        )

    def test_register_and_read_file(self) -> None:
        if ONS_UK_POPULATION_META_DATA.is_local:
            ONS_UK_POPULATION_META_DATA.delete_local()
        assert not ONS_UK_POPULATION_META_DATA.is_local
        ONS_UK_POPULATION_META_DATA.save_local()
        assert ONS_UK_POPULATION_META_DATA.is_local
        df: DataFrame = ONS_UK_POPULATION_META_DATA.read()
        assert ONS_UK_POPULATION_META_DATA.dates
        assert ([str(d) for d in ONS_UK_POPULATION_META_DATA.dates] == df.columns).all()
        assert df["2018"]["All ages"][0] == 66435.55  # Previous result: 64553.909

        ONS_UK_POPULATION_META_DATA.delete_local()
