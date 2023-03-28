#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import date
from os import PathLike
from pathlib import Path
from typing import Any, Final, Sequence

# from data import io_table_1841
from pandas import DataFrame, Index

from ..input_output_tables import post_read_io_table_wrapper
from ..sources import MetaData, pandas_from_path_or_package
from ..utils import SECTOR_COLUMN_NAME

NAME: Final[str] = "1841 Input-Output Table"
YEAR: Final[int] = 1841
REGION: Final[str] = "United Kingdom"
AUTHORS: Final[dict[str, str]] = {
    "Sara Horrell": "https://www.lse.ac.uk/Economic-History/People/Faculty-and-teachers/Professor-Sara-Horrell",
    "Jane Humphries": "https://www.asc.ox.ac.uk/person/professor-jane-humphries",
    "Martin Weale": "https://www.kcl.ac.uk/people/martin-weale-1",
}
DOI: Final[str] = "10.2307/2597593"

ABSTRACT: Final[
    str
] = "An input-output table for the UK for 1841 shows the country's economic structure and the interdependence and linkages between industries. Using estimates of the capital and labour employed, the productivity of each industry can be identified. The early industrial economy exhibited considerable interrelatedness between industries, and the importance of the linkages of the metal industries is confirmed. But the economy was bifurcated into agriculture-based and mineral-based industries with the latter having less important backward linkages and lower productivity than the former."

CSV_FILE_NAME: Final[PathLike] = Path("uk-input-output-1841-Horrell-Humphries-Weal.csv")
FIRST_CODE_ROW: Final[int] = 0
SKIPROWS: Final[list[int]] = [21, 22]  # Skip rows 17 and 18
INDEX_COL: Final[str] = "Sectors"

HISTORIC_UK_SECTORS: Final[list[str]] = [
    "Agriculture",
    "Mining and quarrying",
    "Food, drink, and tobacco",
    "Metal manufacture",
    "Soap, candles, and dyes",
    "Textiles, clothing, and leather goods",
    "Metal goods",
    "Bricks, pottery, and glass",
    "Other manufacturing",
    "Construction",
    "Gas and water",
    "Transport",
    "Distribution",
    "Domestic service",
    "Other service",
    "Public adminstration and defence",
    "Housing services",
]

FINAL_DEMAND_COLUMN_NAMES: Final[list[str]] = ["Consumption", "Investment"]
FINAL_COLUMNS: Final[list[str]] = FINAL_DEMAND_COLUMN_NAMES + ["Exports", "Total"]

COLUMNS: Final[list[str]] = (
    [
        INDEX_COL,
    ]
    + HISTORIC_UK_SECTORS
    + FINAL_COLUMNS
)


def fix_empty_col_and_na_values(
    df: DataFrame,
    sector_names: Sequence[str] = HISTORIC_UK_SECTORS,
    drop_col: str = "Unnamed: 21",
    sector_col_name: str = SECTOR_COLUMN_NAME,
    return_tuple: bool = False,
) -> DataFrame | tuple[DataFrame, Index, Index]:
    """Drop column `Unnamed: 21`, replace `na` with 0.0 and `Sectors` -> `Sector`."""
    assert df[drop_col].isnull().all()
    df = df.drop(drop_col, axis=1)
    df[sector_names] = df[sector_names].fillna(0.0)
    df.loc[sector_names] = df.loc[sector_names].fillna(0.0)
    df.index = df.index.rename(sector_col_name)
    if return_tuple:
        return df, df.index, df.columns
    else:
        return df


# TABLE_AND_INDEXES_

# def fix_empty_col_metadat_wrapper(df: DataFrame, func: Callable, **kwargs) -> tuple[DataFrame, Index, Index]:
#                 self.all_input_row_labels,
#                 self.all_output_column_labels,
#
#     return {'raw_io_table'}

POST_READ_KWARGS: dict[str, Any] = dict(
    func=fix_empty_col_and_na_values,
    sector_names="self.sector_names",
    return_tuple=True,
)


METADATA: Final[MetaData] = MetaData(
    name=NAME,
    year=YEAR,
    date_published=date(1994, 8, 1),
    authors=AUTHORS,
    region=REGION,
    doi=DOI,
    description=ABSTRACT,
    path=CSV_FILE_NAME,
    file_name_from_url=False,
    auto_download=False,
    # Todo: add an "only_local_file" option
    needs_scaling=True,
    _package_data=True,
    # _save_func=download_and_extract_zip_file,  # type: ignore
    # _save_kwargs=dict(zip_file_path=ONS_UK_2018_FILE_NAME),
    _reader_func=pandas_from_path_or_package,
    _reader_kwargs=dict(skiprows=SKIPROWS, index_col=INDEX_COL),
    _post_read_func=post_read_io_table_wrapper,
    _post_read_kwargs=POST_READ_KWARGS,
)
