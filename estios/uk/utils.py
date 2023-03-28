#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import UserDict
from dataclasses import dataclass, field
from datetime import date, datetime
from logging import getLogger
from typing import Final, Generator, Iterable, NamedTuple, Sequence, Type

from pandas import DataFrame, Series

from ..sources import MetaData
from ..spatial import (
    GenericRegionsManager,
    NullCodeException,
    Region,
    RegionsManager,
    RegionsManagerMixin,
    sum_for_regions_by_attr,
)
from ..utils import DateType, filled_or_empty_dict
from .centre_for_cities_puas import (
    CENTRE_FOR_CITIES_2022_CITY_PUAS,
    CENTRE_FOR_CITIES_2022_CITY_REGIONS_METADATA,
)
from .ons_population_estimates import ONS_CONTEMPORARY_POPULATION_META_DATA
from .ons_population_projections import (
    NATIONAL_RETIREMENT_AGE,
    RETIREMENT_AGE_INCREASE_YEAR,
    WORKING_AGE_MINIMUM,
)
from .regions import SKIP_CITIES

logger = getLogger(__name__)


UK_NATIONAL_COLUMN_NAME: Final[str] = "UK"
UK_NAME: Final[str] = "United Kingdom"
UK_NATION_NAMES: Final[tuple[str, ...]] = (
    "England",
    "Northern Ireland",
    "Scotland",
    "United Kingdom",
    "Wales",
)
ONS_AREA_CODE_COLUMN_NAME: Final[str] = "AREA_CODE"
ONS_AGES_COLUMN_NAME: Final[str] = "AGE_GROUP"

RegionInfoTypes = str | bool | int
RegionInfoMapper = dict[str, dict[str, RegionInfoTypes]]

AltNameType = dict[str, str]

AltNamesMapperType = dict[str, AltNameType]

assert isinstance(
    CENTRE_FOR_CITIES_2022_CITY_REGIONS_METADATA.dict_key_appreviation, str
)
PUA_KEY: Final[str] = CENTRE_FOR_CITIES_2022_CITY_REGIONS_METADATA.dict_key_appreviation

assert isinstance(ONS_CONTEMPORARY_POPULATION_META_DATA.dict_key_appreviation, str)
CONTMEPORARY_KEY: Final[
    str
] = ONS_CONTEMPORARY_POPULATION_META_DATA.dict_key_appreviation
REGION_MAPPER_KEYS: Final[tuple[str, ...]] = (PUA_KEY, CONTMEPORARY_KEY)

NO_CONTEMPORARY_KEY: Final[str] = f"no {CONTMEPORARY_KEY}"
YEAR_CHANGED_KEY: Final[str] = "year changed"
PUA_GEOGRAPHY_TYPE: Final[str] = "Public Urban Area"
LAD_GEOGRAPHY_TYPE: Final[str] = "Local Authority District"

UNITED_KINGDOM_CONTEMPORARY_INDEX: str = "UNITED KINGDOM"


THREE_UK_CITY_REGIONS: Final[dict[str, str]] = {
    "Leeds": "Yorkshire and the Humber",
    "Liverpool": "North West",  # LIVERPOOL & BIRKENHEAD
    "Manchester": "North West",  # MANCHESTER & SALFORD
}

LA_CODES_COLUMN: Final[str] = "la_codes"


def enforce_ons_date_format(cell: str) -> str:
    """Set convert date strings for consistent formatting."""
    if cell.endswith("00:00"):
        return cell.split()[0]
    else:
        cell = cell.strip()
        if cell.endswith(")") or len(cell.split()) > 2:
            # Remove flags of the form " (r)", " (p)" and " 4 (p)" 20 4 (4 is a footnote)
            logger.info(f"Date {cell} has extra section. Trimming.")
            cell = " ".join(cell.split()[:2])
        return str(datetime.strptime(cell, "%b %y")).split()[0]


def generate_employment_quarterly_dates(
    years: Iterable[int], reverse: bool = False
) -> Iterable[DateType]:
    """Return quaterly dates for UK employment data in reverse chronological order."""
    for year in years:
        if reverse:
            for month in range(12, 0, -3):
                yield date(year, month, 1)
        else:
            for month in range(3, 13, 3):
                yield date(year, month, 1)


def sum_for_regions_by_la_code(
    df: DataFrame,
    region_names: Sequence[str],
    column_names: Sequence[str | int],
    regions: GenericRegionsManager,
    set_index_to_column: str | None = None,
    ignore_key_errors: bool = False,
) -> dict[str, float | Series]:
    return {
        region: value
        for region, value in sum_for_regions_by_attr(
            df=df,
            region_names=region_names,
            column_names=column_names,
            regions=regions,
            attr=LA_CODES_COLUMN,
            set_index_to_column=set_index_to_column,
            ignore_key_errors=ignore_key_errors,
        )
    }


THREE_UK_CITY_REGIONS: Final[dict[str, str]] = {
    "Leeds": "Yorkshire and the Humber",
    "Liverpool": "North West",  # LIVERPOOL & BIRKENHEAD
    "Manchester": "North West",  # MANCHESTER & SALFORD
}


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

    """UK Primary Urban Area Region."""

    local_authorities: RegionsManager = field(default_factory=lambda: RegionsManager())
    region_name: str | None = "UK"

    def __str__(self) -> str:
        return f"{self.region_name} PUA {self.name} of {self.la_codes_count} Local Authorities"

    @property
    def la_codes(self) -> tuple[str, ...]:
        return tuple(self.la_codes_generator())

    @property
    def la_names(self) -> tuple[str, ...]:
        return tuple(self.la_names_generator())

    def __repr__(self) -> str:
        """Return a str indicated `RegionsManager` type and `local_authority` metrics."""
        repr: str = f"{self.__class__.__name__}("
        repr += f"name={self.name}, "
        repr += f"la_codes_count={self.la_codes_count})"
        return repr

    def la_names_generator(self) -> Generator[str, None, None]:
        for local_authority_name in self.local_authorities.names:
            if not local_authority_name:  # Case of `name` == ""
                raise ValueError(
                    f"Invalid `local_authority_name` for {local_authority_name}"
                )
            yield local_authority_name

    def la_codes_generator(self) -> Generator[str, None, None]:
        for region_code in self.local_authorities.codes:
            if isinstance(region_code, str):
                yield region_code
            else:
                raise NullCodeException(f"{self} has no code set.")

    @property
    def la_names_count(self) -> int:
        return len(self.la_names)

    @property
    def la_codes_count(self) -> int:
        return len(self.la_codes)


PUA_ACRONYM_NAME: Final[str] = "Primary Urban Area"
PUAS_MANAGER_REGION_NAME: Final[str] = f"{UK_NATIONAL_COLUMN_NAME} {PUA_ACRONYM_NAME}"


@dataclass(repr=False)
class PUASManager(RegionsManagerMixin, UserDict[str, PrimaryUrbanArea]):

    """Custom RegionsManager for PrimaryUrbanArea classes.

    Todo:
        * Add local_authority dict query to return hierarchy
    """

    def __init__(
        self,
        meta_data: MetaData = CENTRE_FOR_CITIES_2022_CITY_REGIONS_METADATA,
        region_name: str = PUAS_MANAGER_REGION_NAME,
    ) -> None:
        super().__init__()
        self.meta_data = meta_data
        self.region_name = region_name

    def __str__(self) -> str:
        return f"{len(self)} {self.region_name}s"

    def __repr__(self) -> str:
        """Return a str indicated class type and number of sectors.

        Note:
            * `codes` currently ignored to avoid
        """
        repr: str = f"{self.__class__.__name__}("
        repr += f"count={self.names_count})"
        return repr


FLAG_KEYS: tuple[str, ...] = (NO_CONTEMPORARY_KEY, YEAR_CHANGED_KEY)

REGION_ALTERNATE_NAMES: Final[RegionInfoMapper] = {
    "Aberdeen": {
        PUA_KEY: "Aberdeen",
        CONTMEPORARY_KEY: "Aberdeen City",
    },
    "Bournemouth Christchurch and Poole": {
        PUA_KEY: "Bournemouth, Christchurch and Poole",
        NO_CONTEMPORARY_KEY: True,
        "code": "E06000058",  # As of 2019, see https://geoportal.statistics.gov.uk/datasets/ons::lad-dec-2019-names-and-codes-in-the-united-kingdom
        YEAR_CHANGED_KEY: 2019,
    },
    "City of Bristol": {
        "pua": "City of Bristol",
        CONTMEPORARY_KEY: "Bristol, City of",
    },
    "Dundee": {
        PUA_KEY: "Dundee",
        CONTMEPORARY_KEY: "Dundee City",
    },
    "Edinburgh": {
        PUA_KEY: "Edinburgh",
        CONTMEPORARY_KEY: "City of Edinburgh",
    },
    "Glasgow": {
        PUA_KEY: "Glasgow",
        CONTMEPORARY_KEY: "Glasgow City",
    },
    "Kingston upon Hull": {
        PUA_KEY: "Kingston upon Hull",
        CONTMEPORARY_KEY: "Kingston upon Hull, City of",
    },
    "West Northamptonshire": {
        PUA_KEY: "West Northamptonshire",
        CONTMEPORARY_KEY: "West Northamptonshire\xa0",
    },
    "Castlepoint": {PUA_KEY: "Castlepoint", CONTMEPORARY_KEY: "Castle Point"},
}

PUA_ALTERNATE_NAMES: Final[RegionInfoMapper] = {
    "Manchester": {
        PUA_KEY: "Manchester",
        NO_CONTEMPORARY_KEY: True,
        # "Greater Manchester (Met County)",
        # "Greater Manchester",
    },
    "Bristol": {
        "pua": "Bristol",
        NO_CONTEMPORARY_KEY: True,
    },
    "Hull": {
        # "Kingston upon Hull",
        PUA_KEY: "Hull",
        CONTMEPORARY_KEY: "Kingston upon Hull, City of",
    },
    "London": {
        PUA_KEY: "London",
        CONTMEPORARY_KEY: "LONDON",
    },
    "Aberdeen": {
        PUA_KEY: "Aberdeen",
        CONTMEPORARY_KEY: "Aberdeen City",
    },
    "Aldershot": {PUA_KEY: "Aldershot", NO_CONTEMPORARY_KEY: True},
    "Birkenhead": {PUA_KEY: "Birkenhead", NO_CONTEMPORARY_KEY: True},
    "Blackburn": {PUA_KEY: "Blackburn", NO_CONTEMPORARY_KEY: True},
    # Only "Bournemouth, Christchurch and Poole" in ONS_CONTEMPORARY_POPULATION_META_DATA
    "Bournemouth": {PUA_KEY: "Bournemouth", NO_CONTEMPORARY_KEY: True},
    "Brighton": {PUA_KEY: "Brighton", NO_CONTEMPORARY_KEY: True},
    "Chatham": {PUA_KEY: "Chatham", NO_CONTEMPORARY_KEY: True},
    "Dundee": {
        PUA_KEY: "Dundee",
        CONTMEPORARY_KEY: "Dundee City",
    },
    "Edinburgh": {
        PUA_KEY: "Edinburgh",
        CONTMEPORARY_KEY: "City of Edinburgh",
    },
    "Glasgow": {PUA_KEY: "Glasgow", NO_CONTEMPORARY_KEY: True},
    "Huddersfield": {PUA_KEY: "Huddersfield", NO_CONTEMPORARY_KEY: True},
    "Newcastle": {PUA_KEY: "Newcastle", NO_CONTEMPORARY_KEY: True},
    "Northampton": {PUA_KEY: "Northampton", NO_CONTEMPORARY_KEY: True},
    "Southend": {PUA_KEY: "Southend", NO_CONTEMPORARY_KEY: True},
    "Stoke": {PUA_KEY: "Stoke", NO_CONTEMPORARY_KEY: True},
    "Telford": {PUA_KEY: "Telford", NO_CONTEMPORARY_KEY: True},
}


class NamesMatchedTuple(NamedTuple):
    canonical: str
    alt: str | None = None
    alt_name_key: str | None = None


def name_or_alt(
    name: str,
    alt_name_key: str,
    alt_names: RegionInfoMapper = REGION_ALTERNATE_NAMES,
) -> NamesMatchedTuple:
    """Return a NamesMatchedTuple with an alt set if availabe, else alt is None."""
    if name in alt_names and alt_name_key in alt_names[name]:
        alt_name: RegionInfoTypes = alt_names[name][alt_name_key]
        try:
            assert isinstance(alt_name, str)
        except AssertionError:
            raise TypeError(f"alt_name: {alt_name} must be a str.")
        return NamesMatchedTuple(
            canonical=name, alt=alt_name, alt_name_key=alt_name_key
        )
    else:
        return NamesMatchedTuple(canonical=name, alt_name_key=alt_name_key)


def region_by_name_or_alt(
    names_match: NamesMatchedTuple, regions: GenericRegionsManager
) -> Region:
    """Return a Region object using the matched alt name if available, else the canonical name."""
    if names_match.alt:
        return regions[names_match.alt]
    else:
        return regions[names_match.canonical]


# def match_name_or_alt_names(
#     name: str,
#     other_dataset_names: Sequence[str],
#     alt_names: AltNamesMapperType = REGION_ALTERNATE_NAMES,
#     alt_names_key: str | None = None,
# ) -> NamesMatchedTuple | None:
#     if name in other_dataset_names:
#         return NamesMatchedTuple(pua=name)
#     else:
#         # for canonical_name, other_names in alt_names.items():
#         #     if name in other_names:
#         #         if canonical_name in other_dataset_names:
#         #             return canonical_name
#
#         # if name in alt_names.keys() and name in other_dataset_names:
#         #
#         for canonical_name, other_names in alt_names.items():
#             if alt_names_key:
#                 assert isinstance(other_names, dict)
#                 other_names = tuple(other_names[alt_names_key])
#             matched_name: set[str] = set((canonical_name, *other_names)) & {name}
#             if matched_name:  # Return final version
#                 assert len(matched_name) == 1
#                 assert not canonical_name == "Aberdeen"
#                 return NamesMatchedTuple(pua=name, alt=matched_name.pop())
#             # if name in set(canonical_name, *alt_names):
#             # if name in other_names or name == canonical_name:
#             #     if canonical_name in other_dataset_names:
#             #         return canonical_name
#             #     else:
#             #         logger.error(
#             #             f"{canonical_name} match but not in provided `match_list`."
#             #         )
#             #         assert False
#             #         return None
#     logger.error(f"No match for {name} found.")
#     assert False
#     return None


def region_from_alt_names(
    region_name: str,
    alt_names: RegionInfoMapper,
    regions_manager: GenericRegionsManager,
    region_class: Type[Region] = Region,
    alt_name_keys: tuple[str, ...] = REGION_MAPPER_KEYS,
    geography_type: str = LAD_GEOGRAPHY_TYPE,
) -> Region:
    region_names_tuple: NamesMatchedTuple = name_or_alt(
        name=region_name,
        alt_names=alt_names,
        alt_name_key=CONTMEPORARY_KEY,
    )
    if (
        region_name in alt_names
        and NO_CONTEMPORARY_KEY in alt_names[region_name]
        and alt_names[region_name][NO_CONTEMPORARY_KEY]
    ):
        logger.warning(f"No ONS data directly available on {region_name}")
        code = (
            alt_names[region_name]["code"]
            if "code" in alt_names[region_name].keys()
            else None
        )
        assert code is None or isinstance(code, str)
        region = region_class(
            name=region_name,
            code=code,
            geography_type=geography_type,
        )
        # matched_name: set[str] = set((canonical_name, *other_names)) & {name}
        if (
            set(FLAG_KEYS) & alt_names[region_name].keys()
        ):  # Check if any FLAG_KEYS included
            for key in FLAG_KEYS:
                if key in alt_names[region_name].keys():
                    logger.warning(f"Adding {key} flag to {alt_names[region_name]}")
                    region.flags[key] = alt_names[region_name][key]
    else:
        region = region_by_name_or_alt(region_names_tuple, regions_manager)
    if region_names_tuple.alt:
        for key in alt_name_keys:
            if key in alt_names[region_name]:
                alt_name: RegionInfoTypes = alt_names[region_name][key]
                assert isinstance(alt_name, str)
                region.alternate_names[key] = alt_name
    return region


def generate_uk_puas(
    # puas_manager: PUASManager | None = None,
    uk_regions: GenericRegionsManager | None = None,
    puas_dict: dict[str, tuple[str, ...]] = CENTRE_FOR_CITIES_2022_CITY_PUAS,
    pua_alt_names: RegionInfoMapper = PUA_ALTERNATE_NAMES,
    region_alt_names: RegionInfoMapper = REGION_ALTERNATE_NAMES,
    # code_col: str = "Code",
    # geo_col: str = "Geography",
    # alternate_names: AltNamesMapperType = PUA_ALTERNATE_NAMES,
) -> GenericRegionsManager:
    """Return a PUARegionManager using definitions of UK Public Urban Areas

    Todo:
        * Refactor for better ways to manage specific flags
        * Consider shifting to 2015 definitions of Centre for Cities
    """
    # if not puas_manager:
    #     puas_manager: PUASManager = PUASManager()
    # assert puas_manager is not None
    if not uk_regions:
        uk_regions = generate_base_regions()
    assert uk_regions is not None
    puas_manager: PUASManager = PUASManager()
    for pua, local_authorities in puas_dict.items():
        pua_region: Region = region_from_alt_names(
            region_name=pua,
            alt_names=pua_alt_names,
            regions_manager=uk_regions,
            # alt_name_keys=CONTMEPORARY_KEY,
            geography_type=PUA_GEOGRAPHY_TYPE,
        )
        # regions[region] = region_from_alt_names(
        #         region_name=region,
        #         alt_names=region_alt_names,
        #         regions_manager=uk_regions,
        # )
        # pua_re#         gion: Region
        # pua_names_tuple: NamesMatchedTuple = name_or_alt(
        #     name=pua, alt_names=pua_alt_names, alt_name_key=CONTMEPORARY_KEY
        # )
        # if (
        #     pua in pua_alt_names
        #     and NO_CONTEMPORARY_KEY in pua_alt_names[pua]
        #     and pua_alt_names[pua][NO_CONTEMPORARY_KEY]
        # ):
        #     logger.error(f"No ONS data directly available on {pua}")
        #     pua_region = Region(
        #         name=pua,
        #         code=None,
        #         geography_type=PUA_GEOGRAPHY_TYPE,
        #         flags={NO_CONTEMPORARY_KEY: True},
        #     )
        # else:
        #     # pua_names_match: NamesMatchedTuple | None = match_name_or_alt_names(
        #     #     pua, list(uk_regions.keys()), regional_alt_names, alt_names_key="pua"
        #     # )
        #     # assert pua_names_match
        #     pua_region = region_by_name_or_alt(pua_names_tuple, uk_regions)
        #     if pua_names_tuple.alt:
        #         pua_region.alternate_names = pua_alt_names[pua]
        # pua_data = uk_regions[pua_names_match.alt] if pua_names_match.alt else uk_regions[pua_names_match.pua]
        # if names_match and names_match.alt:
        #     pua_data: Region = uk_regions[pua_name]
        # else:
        #     logger.error(f"No match for Primary Urban Area (PUA) {pua}. Skipping...")
        #     continue
        regions = RegionsManager()
        for region in local_authorities:
            regions[region] = region_from_alt_names(
                region_name=region,
                alt_names=region_alt_names,
                regions_manager=uk_regions,
            )
            # region_names_tuple: NamesMatchedTuple = name_or_alt(
            #     name=region,
            #     alt_names=region_alt_names,
            #     alt_name_key=CONTMEPORARY_KEY,
            # )
            # if (
            #     region in region_alt_names
            #     and NO_CONTEMPORARY_KEY in region_alt_names[region]
            #     and region_alt_names[region][NO_CONTEMPORARY_KEY]
            # ):
            #     logger.error(f"No ONS data directly available on {region}")
            #     code = (
            #         region_alt_names[region]["code"]
            #         if "code" in region_alt_names[region].keys()
            #         else None
            #     )
            #     assert code is None or isinstance(code, str)
            #     regions[region] = Region(
            #         name=region, code=code, geography_type=LAD_GEOGRAPHY_TYPE
            #     )
            #     # matched_name: set[str] = set((canonical_name, *other_names)) & {name}
            #     if (
            #         set(FLAG_KEYS) & region_alt_names[region].keys()
            #     ):  # Check if any FLAG_KEYS included
            #         for key in FLAG_KEYS:
            #             if key in region_alt_names[region].keys():
            #                 logger.warning(
            #                     f"Adding {key} flag to {region_alt_names[region]}"
            #                 )
            #                 regions[region].flags[key] = region_alt_names[region][key]
            # else:
            #     regions[region] = region_by_name_or_alt(region_names_tuple, uk_regions)
            #     if region_names_tuple.alt:
            #         regions[region].alternate_names = region_alt_names[region]
            # region_names_match: NamesMatchedTuple | None = match_name_or_alt_names(
            #     region, list(uk_regions.keys()), regional_alt_names, alt_names_key="pua"
            # )
            # region_name: str | None = region if region in uk_region_df.index else None
            # if not region_name:
            #     for alt_name in regional_alt_names[region]:
            #         if alt_name in uk_region_df.index:
            #             region_name = alt_name
            #             break
            # regions[region] = uk_regions[region_names_match.alt] if region_names_match.alt else uk_regions[region_names_match.pua]
            # if not region_names_match:
            #     logger.error(f"{region} not found in")
            #     continue
            # else:
            #     regions[region] = uk_regions[region_names_match.alt] if region_names_match.alt else uk_regions[region_names_match.pua]
            # region_data: Series = uk_regions[region_name]
            # regions[region] = Region(
            #     name=region,
            #     code=region_data[code_col],
            #     geography_type=region_data[geo_col],
            # )
        # alt_names_tuple: tuple[str, ...] = filled_or_empty_tuple(alternate_names, pua)
        # if alt_names_tuple:
        #     assert len(alt_names_tuple) == 1
        #     alt_names: AltNameType = {'pua': alt_names_tuple[0]}
        puas_manager[pua] = PrimaryUrbanArea(
            name=pua,
            code=pua_region.code,
            geography_type=pua_region.geography_type,
            local_authorities=regions,
            alternate_names=pua_region.alternate_names,
        )
    return puas_manager


def generate_base_regions(
    ons_region_df: DataFrame | None = None,
    ons_region_meta_data: MetaData = ONS_CONTEMPORARY_POPULATION_META_DATA,
    regions_date: DateType | int | None = None,
    alternate_names: RegionInfoMapper = REGION_ALTERNATE_NAMES,
) -> GenericRegionsManager:
    if not ons_region_df:
        ons_region_df = load_contemporary_ons_population(
            ons_region_data=ons_region_meta_data
        )
    assert isinstance(ons_region_df, DataFrame)
    if not regions_date and ons_region_meta_data.canonical_date:
        regions_date = ons_region_meta_data.canonical_date
    regions_manager = RegionsManager(meta_data=ons_region_meta_data)
    for region in ons_region_df.itertuples():
        alt_names: dict[str, str] = filled_or_empty_dict(alternate_names, region.Index)
        regions_manager[region.Index] = Region(
            name=region.Index,
            code=region.Code,
            geography_type=region.Geography,
            date=regions_date,
            alternate_names=alt_names,
        )
    return regions_manager


# <<<<<<< HEAD

# <<<<<<< Updated upstream
# =======


def working_ages(
    year: int,
    min_working_age: int = WORKING_AGE_MINIMUM,
    max_working_age: int = NATIONAL_RETIREMENT_AGE,
    retirement_age_increase_year: int = RETIREMENT_AGE_INCREASE_YEAR,
) -> Generator[int, None, None]:
    assert year > 2000
    if (
        max_working_age == NATIONAL_RETIREMENT_AGE
        and year >= retirement_age_increase_year
    ):
        max_working_age += 1
        logger.debug(
            f"Setting `max_working_age` to {max_working_age} as `year` "
            f"{year} >= `retirement_age_increase_year` {retirement_age_increase_year}"
        )
    for age in range(min_working_age, max_working_age + 1):
        yield age


def get_working_cities_puas_manager(
    puas_manager: PUASManager | GenericRegionsManager | None = None,
    skip_regions: Iterable = SKIP_CITIES,
) -> PUASManager | GenericRegionsManager:
    if not puas_manager:
        puas_manager = generate_uk_puas()
    for region in skip_regions:
        puas_manager.pop(region, None)
    return puas_manager

    # >>>>>>> Stashed changes
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
