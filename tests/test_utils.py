#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass, field
from itertools import product
from logging import DEBUG

import pytest
from pandas import DataFrame, MultiIndex

from estios.server.dash_app import DEFAULT_SERVER_PATH, PATH_SPLIT_CHAR
from estios.uk.utils import THREE_UK_CITY_REGIONS, UK_NATIONAL_COLUMN_NAME
from estios.utils import (  # download_and_extract_zip_file,
    REGION_COLUMN_NAME,
    SECTOR_10_CODE_DICT,
    SECTOR_COLUMN_NAME,
    GetAttrStrictError,
    enforce_end_str,
    enforce_start_str,
    gen_region_attr_multi_index,
    generate_i_m_index,
    generate_ij_index,
    generate_ij_m_index,
    get_attr_from_str,
    invert_dict,
    match_df_cols_rows,
    match_ordered_iters,
    name_converter,
)


class TestMultiIndexGenerators:

    """Test i_m, ij and ij_m MultiIndex generator functions."""

    def test_i_m_index(self, three_city_names, ten_sector_aggregation_names) -> None:
        """Test correct hierarchical dimensions for an im index."""
        default_i_m_index: MultiIndex = generate_i_m_index(
            three_city_names,
            ten_sector_aggregation_names,
            national_column_name=UK_NATIONAL_COLUMN_NAME,
        )
        assert len(default_i_m_index) == len(THREE_UK_CITY_REGIONS) * len(
            SECTOR_10_CODE_DICT
        )
        assert set(default_i_m_index.get_level_values(0)) == set(THREE_UK_CITY_REGIONS)
        assert set(default_i_m_index.get_level_values(1)) == set(SECTOR_10_CODE_DICT)

    def test_ij_index(self, three_city_names) -> None:
        """Test correct hierarchical dimensions for an ij index."""
        default_ij_index: MultiIndex = generate_ij_index(
            three_city_names, three_city_names
        )
        assert len(default_ij_index) == len(THREE_UK_CITY_REGIONS) * len(
            THREE_UK_CITY_REGIONS
        )
        assert set(default_ij_index.get_level_values(0)) == set(THREE_UK_CITY_REGIONS)
        assert set(default_ij_index.get_level_values(1)) == set(THREE_UK_CITY_REGIONS)

    def test_ij_m_index(self, three_city_names, ten_sector_aggregation_names) -> None:
        """Test correct hierarchical dimensions for an ij_m index."""
        default_ij_m_index: MultiIndex = generate_ij_m_index(
            three_city_names,
            ten_sector_aggregation_names,
            national_column_name=UK_NATIONAL_COLUMN_NAME,
        )
        assert len(default_ij_m_index) == (
            # Note i=j is excluded
            len(THREE_UK_CITY_REGIONS)
            * (len(THREE_UK_CITY_REGIONS) - 1)
            * len(SECTOR_10_CODE_DICT)
        )
        assert set(default_ij_m_index.get_level_values(0)) == set(THREE_UK_CITY_REGIONS)
        assert set(default_ij_m_index.get_level_values(1)) == set(THREE_UK_CITY_REGIONS)
        assert set(default_ij_m_index.get_level_values(2)) == set(SECTOR_10_CODE_DICT)


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
    assert invert_dict(test_dict) == correct_inversion


class TestMatchItersColsRows:
    test_x: tuple = ("cat", "frog", 4, 7, (3, 4))
    test_y: list = ["cat", "cat", 4.0, 7, (3, 4)]
    CORRECT_MATCHES: tuple = ("cat", 4, 7, (3, 4))

    def test_get_matched_ordered_iters(self) -> None:
        """Test extracting matched tuple and list values."""
        matches: tuple = match_ordered_iters(self.test_x, self.test_y)
        assert matches == self.CORRECT_MATCHES

    def test_get_matched_df_cols_rows(self) -> None:
        """Test extracting matched rows and columns."""
        test_df: DataFrame = DataFrame(columns=self.test_x, index=self.test_y)
        matches: tuple[str] = match_df_cols_rows(test_df)
        assert matches == self.CORRECT_MATCHES


class TestPyMRIOIndexes:
    sectors: tuple[str, ...] = ("aggriculture", "manufacturing")

    def test_gen_region_attr_multi_index(self, three_city_names) -> None:
        index: MultiIndex = gen_region_attr_multi_index(
            regions=three_city_names, attrs=self.sectors
        )
        assert index.names[0] == REGION_COLUMN_NAME
        assert index.names[1] == SECTOR_COLUMN_NAME
        for region, sector in product(three_city_names, self.sectors):
            assert (region, sector) in index

    def test_gen_region_attr_change_names(self, three_city_names) -> None:
        names: tuple[str, str] = ("cat", "dog")
        index: MultiIndex = gen_region_attr_multi_index(
            regions=three_city_names,
            attrs=self.sectors,
            names=names,
        )
        assert index.names == names
        for region, sector in product(three_city_names, self.sectors):
            assert (region, sector) in index


@dataclass
class ExampleTestClass:
    a: str = "cat"
    b: dict = field(default_factory=lambda: {"dog": "bark", "fish": "swim"})


@pytest.fixture
def example_test_class() -> ExampleTestClass:
    return ExampleTestClass()


class TestGetAttrFromAttrStr:
    @pytest.fixture(autouse=True)
    def set_caplog_level(self, caplog):
        caplog.set_level(DEBUG)
        self._caplog = caplog

    def test_get_base_attr(self, example_test_class) -> None:
        assert get_attr_from_str(example_test_class, "a") == "cat"
        assert self._caplog.messages == [
            f"Extracted 'a' from {example_test_class}, returning 'cat'",
        ]

    def test_get_absent_attr(self, example_test_class) -> None:
        assert get_attr_from_str(example_test_class, "ball") == "ball"
        assert self._caplog.messages == [
            f"Attribute 'ball' not part of {example_test_class}",
            f"Parameter `strict` set to False, returning 'ball'",
        ]

    def test_get_absent_strict_attr(self, example_test_class) -> None:
        with pytest.raises(GetAttrStrictError) as err:
            get_attr_from_str(example_test_class, "ball", strict=True)
        assert str(err.value) == (
            "Parameter `strict` set to True and 'ExampleTestClass' "
            "object has no attribute 'ball'"
        )
        assert self._caplog.messages == [
            f"Attribute 'ball' not part of {example_test_class}",
        ]

    def test_get_base_attr_with_self(self, example_test_class) -> None:
        assert get_attr_from_str(example_test_class, "self.a", self_str="self") == "cat"
        assert self._caplog.messages == [
            "Dropped self, `attr_str` set to: 'a'",
            f"Extracted 'a' from {example_test_class}, returning 'cat'",
        ]

    def test_get_absent_attr_with_self(self, example_test_class) -> None:
        assert (
            get_attr_from_str(example_test_class, "self.ball", self_str="self")
            == "ball"
        )
        assert self._caplog.messages == [
            "Dropped self, `attr_str` set to: 'ball'",
            f"Attribute 'ball' not part of {example_test_class}",
            "Parameter `strict` set to False, returning 'ball'",
        ]

    # @pytest.mark.xfail(reason="edge case of using `self` not as a class reference")
    def test_get_base_attr_with_self_no_dot(self, example_test_class) -> None:
        assert (
            get_attr_from_str(example_test_class, "selfa", self_str="self") == "selfa"
        )
        assert self._caplog.messages == [
            "Keeping 'self' in `attr_str`: 'selfa'",
            f"Attribute 'selfa' not part of {example_test_class}",
            f"Parameter `strict` set to False, returning 'selfa'",
        ]

    def test_get_base_attr_with_self_no_dot_strict(
        self, example_test_class, strict=True
    ) -> None:
        with pytest.raises(GetAttrStrictError) as err:
            get_attr_from_str(example_test_class, "selfa", strict=True)
        assert self._caplog.messages == [
            f"Attribute 'selfa' not part of {example_test_class}",
        ]
        assert str(err.value) == (
            "Parameter `strict` set to True and 'ExampleTestClass' "
            "object has no attribute 'selfa'"
        )

    def test_get_base_attr_to_self(self, example_test_class) -> None:
        assert (
            get_attr_from_str(example_test_class, "self", self_str="self")
            == example_test_class
        )
        assert self._caplog.messages == [
            f"`attr_str`: 'self' == `self_str`: 'self', returning {example_test_class}",
        ]
