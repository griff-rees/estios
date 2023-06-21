#!/usr/bin/env python
# -*- coding: utf-8 -*-

from logging import getLogger
from os import PathLike
from pathlib import Path
from typing import Final, Iterable, Optional

from geopandas import GeoDataFrame, read_file
from pandas import DataFrame, read_csv

from ..sources import DataLicense, FilePathType, MetaData, path_or_package_data

logger = getLogger(__name__)

UK_EPSG_GEO_CODE: Final[str] = "EPSG:27700"  # UK Coordinate Reference System (CRS)


ONS_2017_REGION_GEOJSON_URL: Final[str] = (
    "http://geoportal1-ons.opendata.arcgis.com/datasets/"
    "a267b55f601a4319a9955b0197e3cb81_0.geojson"
)
ONS_2017_REGION_GEOJSON_FILE_NAME: Final[str] = (
    "Local_Authority_Districts_(December_2017)_Names_and_Codes_"
    "in_the_United_Kingdom.geojson"
)

ONS_GEOGRAPHY_LICENSE_URL: Final[
    str
] = "https://www.ons.gov.uk/methodology/geography/licences"

ONSGeographyLicense: Final = DataLicense(
    name="Open Geography License",
    url=ONS_GEOGRAPHY_LICENSE_URL,
    version=2023,
)

NEWCASTLE_ALT_NAME: Final[str] = "Newcastle upon Tyne"
HULL_ALT_NAME: Final[str] = "Kingston upon Hull"

CENTRE_FOR_CITIES_NAME_FIX_DICT: dict[str, str] = {
    NEWCASTLE_ALT_NAME: "Newcastle",
    HULL_ALT_NAME: "Hull",
}

ONS_CITY_GEOMETRY_META_DATA: Final[MetaData] = MetaData(
    name="2017 Cities",
    year=2017,
    region="UK",
    url=ONS_2017_REGION_GEOJSON_URL,
    path=ONS_2017_REGION_GEOJSON_FILE_NAME,
    license=ONSGeographyLicense,
    _package_data=True,
)


# @dataclass
# class CentreForCitiesCity:
#
#     canonical_name: str
#     alternate_names: list[str]
#     codes: list[str]
#     # geometry
#     # Data sources
#
#     def __post_init__(self):
#         if not self.codes:
#             raise ValueError("Codes required")


EXAMPLE_UK_CITIES_LIST: Final[list[str]] = [
    "Birmingham",  # BIRMINGHAM & SMETHWICK
    "Bradford",
    # "Coventry",
    # "Bristol",
    "Exeter",
    "Leeds",
    "Liverpool",  # LIVERPOOL & BIRKENHEAD
    "Manchester",  # MANCHESTER & SALFORD
    # Skip because of name inconsistency
    # 'Newcastle upon Tyne':  'North East',  # NEWCASTLE & GATESHEAD'
    "Norwich",
    "Sheffield",
    "Southampton",
    "London",
    "Plymouth",
    "York",
]


TEN_UK_CITY_REGIONS: Final[dict[str, str]] = {
    "Birmingham": "West Midlands",  # BIRMINGHAM & SMETHWICK
    "Bradford": "Yorkshire and the Humber",
    "Bristol": "South West",
    "Derby": "East Midlands",
    "Leeds": "Yorkshire and the Humber",
    "Liverpool": "North West",  # LIVERPOOL & BIRKENHEAD
    "Manchester": "North West",  # MANCHESTER & SALFORD
    # Skip because of name inconsistency
    # 'Newcastle upon Tyne':  'North East',  # NEWCASTLE & GATESHEAD'
    "Nottingham": "East Midlands",
    "Southampton": "South East",
    "London": "London",
}

# Todo: Fix incorporating these in model
SKIP_CITIES: Final[tuple[str, ...]] = (
    "Aberdeen",
    "Aldershot",
    "Cardiff",
    "Dundee",
    "Edinburgh",
    "Glasgow",
    # "Bournemouth",
    # "Newcastle",  # In England, issues with name variation
    "Newport",
    "Swansea",
    "Blackburn",  # 2 in Scotland
)

ENGLISH_CITIES: Final[tuple[str, ...]] = (
    "Barnsley",
    "Basildon",
    "Birmingham",
    "Birkenhead",
    "Blackpool",
    "Bournemouth",
    "Bradford",
    "Bristol",
    "Burnley",
    "Cambridge",
    "Chatham",
    "Coventry",
    "Crawley",
    "Derby",
    "Doncaster",
    "Exeter",
    "Gloucester",
    "Huddersfield",
    "Hull",
    "Ipswich",
    "Leeds",
    "Leicester",
    "Liverpool",
    "London",
    "Luton",
    "Manchester",
    "Mansfield",
    "Middlesbrough",
    "Milton Keynes",
    "Newcastle",
    "Norwich",
    "Northampton",
    "Nottingham",
    "Oxford",
    "Peterborough",
    "Plymouth",
    "Portsmouth",
    "Preston",
    "Reading",
    "Sheffield",
    "Slough",
    "Southampton",
    "Sunderland",
    "Swindon",
    "Telford",
    "Wakefield",
    "Warrington",
    "Wigan",
    "Worthing",
    "York",
)

WORKING_ENGLISH_CITIES: Final[tuple[str, ...]] = tuple(
    city for city in ENGLISH_CITIES if city not in SKIP_CITIES
)

# Centre For Cities Data

CENTRE_FOR_CITIES_CSV_FILE_NAME: Final[PathLike] = Path(
    "centre-for-cities-data-tool.csv"
)
CITIES_TOWNS_GEOJSON_FILE_NAME: Final[PathLike] = Path("cities_towns.geojson")
CENTRE_FOR_CITIES_INDEX_COL: Final[str] = "City"
CENTRE_FOR_CITIES_NROWS: Final[int] = 63
CENTRE_FOR_CITIES_DROP_COL_NAME: Final[str] = "Unnamed: 708"
CENTRE_FOR_CITIES_NA_VALUES: Final[str] = " "
CENTRE_FOR_CITIES_REGION_COLUMN: Final[str] = "REGION"
CENTRE_FOR_CITIES_EPSG: Final[str] = "EPSG:27700"


def load_centre_for_cities_csv(
    path: FilePathType = CENTRE_FOR_CITIES_CSV_FILE_NAME,
    index_col: Optional[str] = CENTRE_FOR_CITIES_INDEX_COL,
    nrows: Optional[int] = CENTRE_FOR_CITIES_NROWS,
    na_values: Optional[str] = CENTRE_FOR_CITIES_NA_VALUES,
    drop_col_name: Optional[str] = CENTRE_FOR_CITIES_DROP_COL_NAME,
    **kwargs,
) -> DataFrame:
    """Load a Centre for Cities data tool export csv file."""
    path = path_or_package_data(path, CENTRE_FOR_CITIES_CSV_FILE_NAME)
    base_centre_for_cities_df: DataFrame = read_csv(
        path, index_col=index_col, nrows=nrows, na_values=na_values, **kwargs
    )
    if drop_col_name:
        return base_centre_for_cities_df.drop(drop_col_name, axis=1)
    else:
        return base_centre_for_cities_df


def load_centre_for_cities_gis(
    path: FilePathType = CITIES_TOWNS_GEOJSON_FILE_NAME,
    driver: str = "GeoJSON",
    **kwargs,
) -> GeoDataFrame:
    """Load a Centre for Cities Spartial file (defualt GeoJSON)."""
    path = path_or_package_data(path, CITIES_TOWNS_GEOJSON_FILE_NAME)
    return read_file(path, driver=driver, **kwargs)


def load_and_join_centre_for_cities_data(
    region_path: PathLike = CENTRE_FOR_CITIES_CSV_FILE_NAME,
    spatial_path: PathLike = CITIES_TOWNS_GEOJSON_FILE_NAME,
    region_column: str = CENTRE_FOR_CITIES_REGION_COLUMN,
    fix_newcastle_and_hull: bool = True,
    **kwargs,
) -> GeoDataFrame:
    """Import and join Centre for Cities data (demographics and coordinates)."""
    cities: DataFrame = load_centre_for_cities_csv(region_path, **kwargs)
    cities_spatial: GeoDataFrame = load_centre_for_cities_gis(spatial_path, **kwargs)
    if fix_newcastle_and_hull:
        for name, replacement in CENTRE_FOR_CITIES_NAME_FIX_DICT.items():
            index_id = cities_spatial[cities_spatial["NAME1"].str.contains(name)].index[
                0
            ]
            cities_spatial["NAME1"].at[index_id] = replacement
    return GeoDataFrame(
        cities.join(
            cities_spatial.set_index("NAME1")[[region_column, "COUNTRY", "geometry"]],
            how="inner",
        )
    )


def get_all_centre_for_cities_dict(
    skip_cities: Iterable = SKIP_CITIES,
    region_column: str = CENTRE_FOR_CITIES_REGION_COLUMN,
    **kwargs,
) -> dict[str, str]:
    """Return a dict of all centre for cities with region.

    Todo:
        * Currently only works for England and skips Newcastle.
        * Try filtering by "COUNTRY" and "REGION"
    """
    cities_df: DataFrame = load_and_join_centre_for_cities_data(**kwargs)
    cities_dict: dict[str, str] = cities_df[region_column].to_dict()
    return {
        city: region for city, region in cities_dict.items() if city not in skip_cities
    }
