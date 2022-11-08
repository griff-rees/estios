#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
from pandas import DataFrame, Series
from pandas.testing import assert_series_equal

from estios.uk.utils import (
    PUAS,
    REGION_ALTERNATE_NAMES,
    generate_uk_puas,
    match_name_or_alt_names,
)


def test_load_contemp_ons_pop(pop_recent) -> None:
    CORRECT_CODE_HEAD: Series = Series(
        {
            "UNITED KINGDOM": "K02000001",
            "GREAT BRITAIN": "K03000001",
            "ENGLAND AND WALES": "K04000001",
            "ENGLAND": "E92000001",
            "NORTH EAST": "E12000001",
        },
        name="Code",
    )
    contemp_pops: DataFrame = pop_recent
    assert_series_equal(
        contemp_pops["Code"].head(), CORRECT_CODE_HEAD, check_names=False
    )


class TestMatchNameAltNames:

    """Test and exceptions of name matching accross data sources."""

    @pytest.mark.xfail
    def test_aberdeen(self, pop_recent, caplog) -> None:
        result: str | None = match_name_or_alt_names("Aberdeen City", pop_recent.index)
        assert result == REGION_ALTERNATE_NAMES["Aberbeen"]


@pytest.mark.xfail
def test_generate_uk_puas(caplog) -> None:
    uk_regions: PUAS = generate_uk_puas()
    assert uk_regions["York"].code == "E06000014"
    assert uk_regions["London"].code == "E06000014"
    assert False
