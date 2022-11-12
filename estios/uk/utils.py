#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import UserDict
from dataclasses import dataclass, field
from datetime import date
from logging import getLogger
from typing import Final, Sequence

from pandas import DataFrame, Series

from ..sources import MetaData
from ..spatial import Region
from .centre_for_cities_puas import (
    CENTRE_FOR_CITIES_2022_CITY_PUAS,
    CENTRE_FOR_CITIES_2022_CITY_REGIONS_METADATA,
)
from .ons_population_estimates import ONS_CONTEMPORARY_POPULATION_META_DATA

logger = getLogger(__name__)


def load_contemporary_ons_population(
    ons_region_data: MetaData = ONS_CONTEMPORARY_POPULATION_META_DATA,
) -> DataFrame:
    """Load ONS contemporary data on regional meta data.

    Todo:
        * Abstract and refactor usage of is_local save_local and read
    """
    if not ons_region_data.is_local:
        ons_region_data.save_local()
    return ons_region_data.read()


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


class PUASManager(UserDict[str, PrimaryUrbanArea]):

    """Custom RegionsManager for PrimaryUrbanArea classes."""

    source: MetaData = CENTRE_FOR_CITIES_2022_CITY_REGIONS_METADATA

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
    "London": ("LONDON",),
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
    puas: PUASManager | None = None,
    uk_region_df: DataFrame | None = None,
    puas_dict: dict[str, tuple[str, ...]] = CENTRE_FOR_CITIES_2022_CITY_PUAS,
    regional_alt_names: dict[str, tuple[str, ...]] = PUA_ALTERNATE_NAMES,
    code_col: str = "Code",
    geo_col: str = "Geography",
) -> PUASManager:
    if not puas:
        puas = PUASManager()
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


def generate_base_regions(
    ons_region_df: DataFrame | None = None,
    ons_region_data: MetaData = ONS_CONTEMPORARY_POPULATION_META_DATA,
    regions_date: date | None = None,
) -> PUASManager:
    if not ons_region_df:
        ons_region_df = load_contemporary_ons_population(
            ons_region_data=ons_region_data
        )
    regions_manager = PUASManager()
    for region in ons_region_df.itertuples():
        regions_manager[region.Index] = PrimaryUrbanArea(
            name=region.Index,
            code=region.Code,
            geography_type=region.Geography,
            date=regions_date,
        )
    return regions_manager

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
