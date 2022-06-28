#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pandas import MultiIndex

from regional_input_output.server.dash_app import DEFAULT_SERVER_PATH, PATH_SPLIT_CHAR
from regional_input_output.utils import (
    SECTOR_10_CODE_DICT,
    THREE_UK_CITY_REGIONS,
    enforce_end_str,
    enforce_start_str,
    generate_i_m_index,
    generate_ij_index,
    generate_ij_m_index,
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
