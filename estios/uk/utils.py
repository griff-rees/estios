#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import UserDict
from dataclasses import dataclass, field
from datetime import date
from logging import getLogger
from typing import Final, Sequence

from pandas import DataFrame, Series

from ..utils import MetaData
from .centre_for_cities_puas import CENTRE_FOR_CITIES_2022_CITY_PUAS
from .ons_population_estimates import ONS_CONTEMPORARY_POPULATION_META_DATA

logger = getLogger(__name__)


def load_contemporary_ons_population(
    ons_region_data: MetaData = ONS_CONTEMPORARY_POPULATION_META_DATA,
) -> DataFrame:
    """Load ONS contemporary data on regional meta data."""
    if not ons_region_data.is_local:
        ons_region_data.save_local()
    return ons_region_data.read()


@dataclass
class Region:

    name: str
    code: str
    geography_type: str
    alternate_names: list[str] = field(default_factory=list)
    date: date | int | None = None

    def __str__(self) -> str:
        return f"{self.geography_type} {self.name}"


@dataclass
class PrimaryUrbanArea(Region):

    local_authorities: dict[str, Region] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"PUA {self.name}"

    @property
    def la_codes(self) -> list[str]:
        return list(region.code for region in self.local_authorities.values())

    @property
    def la_names(self) -> list[str]:
        return list(region for region in self.local_authorities)


class UKRegions(UserDict[str, Region]):
    def __str__(self) -> str:
        return f"{len(self)} UK regions"


class PUAS(UserDict[str, PrimaryUrbanArea]):
    def __str__(self) -> str:
        return f"{len(self)} UK Primary Urban Areas"


AltNamesMapperType = dict[str, tuple[str, ...]]

REGION_ALTERNATE_NAMES: Final[AltNamesMapperType] = {
    "Aberdeen": ("Aberdeen City",),
}

PUA_ALTERNATE_NAMES: Final[AltNamesMapperType] = {
    "Manchester": (
        "Greater Manchester",
        "Greater Manchester (Met County)",
    ),
    "Bristol": ("Bristol, City of",),
    "Hull": (
        "Kingston upon Hull",
        "Kingston upon Hull, City of",
    ),
}


def match_name_or_alt_names(
    name: str,
    other_dataset_names: Sequence[str],
    alt_names: AltNamesMapperType = REGION_ALTERNATE_NAMES,
) -> str | None:
    if name in other_dataset_names:
        return name
    else:
        for canonical_name, other_names in alt_names.items():
            if name in other_names or name == canonical_name:
                if canonical_name in other_dataset_names:
                    return canonical_name
                else:
                    logger.error(
                        f"{canonical_name} match but not in provided `match_list`."
                    )
                    return None
    logger.error(f"No match for {name} found.")
    return None


def generate_uk_puas(
    puas: PUAS | None = None,
    uk_region_df: DataFrame | None = None,
    puas_dict: dict[str, tuple[str, ...]] = CENTRE_FOR_CITIES_2022_CITY_PUAS,
    regional_alt_names: dict[str, tuple[str, ...]] = REGION_ALTERNATE_NAMES,
    code_col: str = "Code",
    geo_col: str = "Geography",
) -> PUAS:
    if not puas:
        puas = PUAS()
    assert puas is not None
    if not uk_region_df:
        uk_region_df = load_contemporary_ons_population()
    assert uk_region_df is not None
    for pua, local_authorities in puas_dict.items():
        pua_name: str | None = match_name_or_alt_names(
            pua, uk_region_df.index, regional_alt_names
        )
        if pua_name:
            pua_data: Series = uk_region_df.loc[pua_name]
        else:
            logger.error(f"No match for Primary Urban Area (PUA) {pua}. Skipping...")
            continue
        regions: dict[str, Region] = {}
        for region in local_authorities:
            region_name: str | None = match_name_or_alt_names(
                region, uk_region_df.index, regional_alt_names
            )
            # region_name: str | None = region if region in uk_region_df.index else None
            # if not region_name:
            #     for alt_name in regional_alt_names[region]:
            #         if alt_name in uk_region_df.index:
            #             region_name = alt_name
            #             break
            # if not region_name:
            #     logger.error(f"{region} not found in")
            #     continue
            if region_name:
                region_data: Series = uk_region_df.loc[region_name]
                regions[region] = Region(
                    name=region,
                    code=region_data[code_col],
                    geography_type=region_data[geo_col],
                )
        puas[pua] = PrimaryUrbanArea(
            name=pua,
            code=pua_data[code_col],
            geography_type=pua_data[geo_col],
            local_authorities=regions,
        )
    return puas


def sum_for_regions_by_attr(
    df: DataFrame,
    region_names: Sequence[str],
    column_names: Sequence[str | int],
    attr: str = "la_codes",
    uk_regions: PUAS | UKRegions | None = None,
) -> dict[str, float | Series]:
    """Sum columns for passed pua_names from df.

    Todo:
        * Basic unit tests
        * Potentially generalise for different number of sum calls.
    """
    if uk_regions is None:
        uk_regions = generate_uk_puas()
    assert hasattr(uk_regions[region_names[0]], attr)
    return {
        region: df.loc[getattr(uk_regions[region], attr), column_names]
        .sum()
        .sum()  # .sum()
        for region in region_names
    }

    # dtype: float64, 'Manchester': 16    27593.0
    # 17    28329.0
    # 18    29752.0
    # 19    31489.0
    # 20    34381.0
    # 21    34721.0

    # last_working_age_dict = {
    #     region: ons_2017_pop_df.loc[uk_regions[region].la_codes,
    #                                 working_age_columns].sum()
    #     for region in regions
    # }

    # def __init__(self, mapping=None, /, **kwargs):
    #     for
    #     if mapping is not None:
    #         mapping = {
    #             str(key).upper(): value for key, value in mapping.items()
    #         }
    #     else:
    #         mapping = {}
    #     if kwargs:
    #         mapping.update(
    #             {str(key).upper(): value for key, value in kwargs.items()}
    #         )
    #     super().__init__(mapping)

    # def _load_
