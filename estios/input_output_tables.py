#!/usr/bin/env python
# -*- coding: utf-8 -*-

from copy import deepcopy
from dataclasses import dataclass, field
from logging import getLogger
from typing import Any, Callable, Final, Iterable, Optional, Sequence, Union

from pandas import DataFrame, Index, Series, read_csv, read_excel

from .sources import FilePathType, MetaData, path_or_package_data
from .uk import io_table_1841, ons_IO_2017
from .uk.employment import (
    DATE_COLUMN_NAME,
    UK_JOBS_BY_SECTOR_XLS_FILE_NAME,
    UK_NATIONAL_EMPLOYMENT_SHEET,
)
from .utils import SECTOR_10_CODE_DICT, AggregatedSectorDictType, enforce_date_format

logger = getLogger(__name__)

IO_TABLE_NAME: Final[str] = "IOT"  # Todo: see if this is the standard sheet name
COEFFICIENT_TABLE_NAME: Final[str] = "A"

CPA_COLUMN_NAME: Final[str] = "CPA"
TOTAL_PRODUCTION_INDEX_NAME: Final[str] = "Intermediate Demand"
GROSS_VALUE_ADDED_INDEX_NAME: Final[str] = "Gross value added"
IMPORTS_COLUMN_NAME: Final[str] = "Imports"
FINAL_DEMAND_COLUMN_NAMES: Final[list[str]] = [
    "Household Purchase",
    "Government Purchase",
    "Non-profit Purchase",
]
UK_EXPORT_COLUMN_NAMES: Final[list[str]] = [
    "Exports to EU",
    "Exports outside EU",
    "Exports of services",
]
COVID_FLAGS_COLUMN: Final[str] = "COVID_FLAGS"

SECTOR_DESC_COLUMN_NAME: Final[str] = "Product"
NET_SUBSIDIES_COLUMN_NAME: Final[str] = "Net subsidies"
INTERMEDIATE_COLUMN_NAME: Final[str] = "Intermediate/final use w/purchaser's prices"

TOTAL_OUTPUT_COLUMN_NAME: Final[str] = "Total Purchase"
GROSS_CAPITAL_FORMATION_COLUMN_NAME: Final[str] = "Gross fixed capital formation"
INVENTORY_CHANGE_COLUMN_NAME: Final[str] = "changes in inventories"
ACQUISITION_NET_VALUABLES_DISPOAL_COLUMN_NAME: Final[str] = "Acquisitions less disposals of valuables"

UK_DOG_LEG_CODES: Final[dict[str, dict[str, str]]] = {
    "columns": {
        "Household Purchase": "P3 S14",
        "Government Purchase": "P3 S13",
        "Non-profit Purchase": "P3 S15",
        "Exports to EU": "P61EU",
        "Exports outside EU": "P61RW",
        "Exports of services": "P62",
        GROSS_CAPITAL_FORMATION_COLUMN_NAME: "P51G",
        INVENTORY_CHANGE_COLUMN_NAME:	"P52",
        ACQUISITION_NET_VALUABLES_DISPOAL_COLUMN_NAME:	"P53",
        TOTAL_OUTPUT_COLUMN_NAME: "TD",
    },
    "rows": {
        TOTAL_PRODUCTION_INDEX_NAME: "_T",
        "Imports": "Imports",
        NET_SUBSIDIES_COLUMN_NAME: NET_SUBSIDIES_COLUMN_NAME,
        "Intermediate Demand purchase price": INTERMEDIATE_COLUMN_NAME,
        "Employee Compensation": "D1",
        GROSS_VALUE_ADDED_INDEX_NAME: "GVA",
        "Total Sales": "P1",
    },
}

IO_TABLE_SCALING: Final[float] = 10000000.0
logger.warning(f"Currently set default IO_TABLE_SCALING to: {IO_TABLE_SCALING}")
CITY_SECTOR_ENGINE: Final[str] = "python"


def crop_io_table_to_sectors(
    full_io_table_df: DataFrame, sectors: Iterable[str], sector_prefix: str = ""
) -> DataFrame:
    """Drop extra rows and colums of full_io_table_df to just input-output of sectors."""
    if sector_prefix:
        sectors = [sector_prefix + sector for sector in sectors]
    return full_io_table_df.filter(items=sectors, axis="index").filter(
        items=sectors, axis="columns"
    )


def io_table_to_codes(
    full_io_table_df: DataFrame, sector_code_column: str = CPA_COLUMN_NAME
) -> DataFrame:
    """Convert an Input Output DataFrame table (default ONS) to use coded column names."""
    io_table: DataFrame = full_io_table_df.set_index(sector_code_column)
    io_table.columns = io_table.loc[sector_code_column]
    return io_table.drop(sector_code_column)


def load_region_employment_excel(
    sheet: str = UK_NATIONAL_EMPLOYMENT_SHEET,
    path: FilePathType = UK_JOBS_BY_SECTOR_XLS_FILE_NAME,
    date_column_name: str = DATE_COLUMN_NAME,
    covid_flags_column: str = COVID_FLAGS_COLUMN,
    **kwargs,
) -> DataFrame:
    """Load regional employment data from https://www.nomisweb.co.uk/ excel exports."""
    path = path_or_package_data(path, UK_JOBS_BY_SECTOR_XLS_FILE_NAME)
    region: DataFrame = read_excel(
        path,
        sheet_name=sheet,
        skiprows=5,
        skipfooter=4,
        usecols=lambda x: "Unnamed" not in x,
        dtype={date_column_name: str},
        **kwargs,
    )
    logger.warning(f"Applying NOMIS fixes loading sheet {sheet} from {path}")
    region[covid_flags_column] = region[date_column_name].apply(
        lambda cell: cell.strip().endswith(")")
    )
    region.index = region[date_column_name].apply(enforce_date_format)
    return region.drop([date_column_name], axis=1)


def arrange_cpa_io_table(
    io_table: DataFrame,
    cpa_column_name: Optional[str] = None,
    sector_desc_column_name: str = SECTOR_DESC_COLUMN_NAME,
    imports_column_name: str = IMPORTS_COLUMN_NAME,
    net_subsidies_column_name: str = NET_SUBSIDIES_COLUMN_NAME,
    intermediate_column_name: str = INTERMEDIATE_COLUMN_NAME,
) -> DataFrame:
    """Standardise CPA indexes and columns."""
    io_table.loc[sector_desc_column_name][0] = cpa_column_name
    io_table.loc[cpa_column_name][0] = cpa_column_name
    io_table.columns = io_table.loc[sector_desc_column_name]
    io_table.drop(sector_desc_column_name, inplace=True)

    io_table.loc["Use of imported products, cif"][cpa_column_name] = imports_column_name
    io_table.loc["Taxes less subsidies on products"][
        cpa_column_name
    ] = net_subsidies_column_name
    io_table.loc["Total intermediate/final use at purchaser's prices"][
        cpa_column_name
    ] = intermediate_column_name
    return io_table


def load_io_table_csv(
    path: FilePathType = io_table_1841.CSV_FILE_NAME,
    usecols: Optional[Union[str, list[str]]] = io_table_1841.COLUMNS,
    skiprows: Optional[list[int]] = io_table_1841.SKIPROWS,
    index_col: Optional[Union[int, str]] = io_table_1841.INDEX_COL,
    cpa_column_name: Optional[str] = None,
    sector_desc_column_name: str = SECTOR_DESC_COLUMN_NAME,
    imports_column_name: str = IMPORTS_COLUMN_NAME,
    net_subsidies_column_name: str = NET_SUBSIDIES_COLUMN_NAME,
    intermediate_column_name: str = INTERMEDIATE_COLUMN_NAME,
    **kwargs,
) -> DataFrame:
    """Import an Input-Ouput Table as a DataFrame from a csv file.

    Todo:
        * Raise warning if the file has the wrong extension.
        * Fix packaging of csv file
    """
    path = path_or_package_data(path, io_table_1841.CSV_FILE_NAME)
    io_table: DataFrame = read_csv(
        path,
        usecols=usecols,
        skiprows=skiprows,
        index_col=index_col,
        **kwargs,
    )
    if cpa_column_name:
        io_table = arrange_cpa_io_table(
            io_table,
            cpa_column_name,
            sector_desc_column_name,
            imports_column_name,
            net_subsidies_column_name,
            intermediate_column_name,
        )
    return io_table


def load_io_table_excel(
    path: FilePathType = ons_IO_2017.EXCEL_FILE_NAME,
    sheet_name: str = IO_TABLE_NAME,
    usecols: Optional[str] = ons_IO_2017.USECOLS,
    skiprows: Optional[list[int]] = ons_IO_2017.SKIPROWS,  # Default skips Rows 3 and 4
    index_col: Optional[int] = ons_IO_2017.INDEX_COL,
    header: Optional[Union[int, list[int]]] = ons_IO_2017.HEADER,
    cpa_column_name: str = CPA_COLUMN_NAME,
    sector_desc_column_name: str = SECTOR_DESC_COLUMN_NAME,
    imports_column_name: str = IMPORTS_COLUMN_NAME,
    net_subsidies_column_name: str = NET_SUBSIDIES_COLUMN_NAME,
    intermediate_column_name: str = INTERMEDIATE_COLUMN_NAME,
    **kwargs,
) -> DataFrame:
    """Import an Input-Ouput Table as a DataFrame from an ONS xlsx file."""
    path = path_or_package_data(path, ons_IO_2017.EXCEL_FILE_NAME)
    io_table: DataFrame = read_excel(
        path,
        sheet_name=sheet_name,
        usecols=usecols,
        skiprows=skiprows,
        index_col=index_col,
        header=header,
        **kwargs,
    )
    if cpa_column_name:
        io_table = arrange_cpa_io_table(
            io_table,
            cpa_column_name,
            sector_desc_column_name,
            imports_column_name,
            net_subsidies_column_name,
            intermediate_column_name,
        )
    return io_table


def load_employment_by_region_and_sector_csv(
    path: FilePathType = ons_IO_2017.CITY_SECTOR_EMPLOYMENT_CSV_FILE_NAME,
    skiprows: int = ons_IO_2017.CITY_SECTOR_SKIPROWS,
    skipfooter: int = ons_IO_2017.CITY_SECTOR_SKIPFOOTER,
    engine: str = CITY_SECTOR_ENGINE,
    usecols: Callable[[str], bool] = ons_IO_2017.CITY_SECTOR_USECOLS,
    index_col: int = ons_IO_2017.CITY_SECTOR_INDEX_COLUMN,
    **kwargs,
) -> DataFrame:
    """Import region level sector employment data as a DataFrame."""
    path = path_or_package_data(path, ons_IO_2017.CITY_SECTOR_EMPLOYMENT_CSV_FILE_NAME)
    return read_csv(
        path,
        skiprows=skiprows,
        skipfooter=skipfooter,
        engine=engine,
        usecols=usecols,
        index_col=index_col,
        **kwargs,
    )


def aggregate_io_table(
    agg_sector_dict: AggregatedSectorDictType,  # UK_SECTOR_10_CODE_DICT would suit
    code_io_table: DataFrame,
    dog_leg_columns: dict[str, str] = UK_DOG_LEG_CODES["columns"],
    dog_leg_rows: dict[str, str] = UK_DOG_LEG_CODES["rows"],
) -> DataFrame:
    """Return an aggregated Input Output table via an aggregated mapping of sectors."""
    # Todo: decide whether this dict copy (shallow) is worth keeping
    aggregated_sector_io_table = DataFrame(
        columns=list(agg_sector_dict.keys()) + list(dog_leg_columns.keys()),
        index=list(agg_sector_dict.keys()) + list(dog_leg_rows.keys()),
    )

    for sector_column in agg_sector_dict:
        for sector_row in agg_sector_dict:
            sector_column_names: Sequence[str] = agg_sector_dict[sector_column]
            sector_row_names: Sequence[str] = agg_sector_dict[sector_row]
            aggregated_sector_io_table.loc[
                sector_column, sector_row
            ] = (  # Check column row order
                code_io_table.loc[sector_column_names, sector_row_names].sum().sum()
            )
            for dog_leg_column, source_column_name in dog_leg_columns.items():
                aggregated_sector_io_table.loc[
                    sector_row, dog_leg_column
                ] = code_io_table.loc[sector_row_names, source_column_name].sum()
        for dog_leg_row, source_row_name in dog_leg_rows.items():
            aggregated_sector_io_table.loc[
                dog_leg_row, sector_column
            ] = code_io_table.loc[source_row_name, sector_column_names].sum()
    return aggregated_sector_io_table


@dataclass
class InputOutputTable:

    """Manage processing and aggregating Input Output Tables."""

    path: Optional[FilePathType] = io_table_1841.CSV_FILE_NAME
    full_io_table: Optional[DataFrame] = None
    base_io_table: Optional[DataFrame] = None
    io_scaling_factor: float = IO_TABLE_SCALING
    sector_names: Optional[Series] = field(
        default_factory=lambda: io_table_1841.HISTORIC_UK_SECTORS
    )
    sector_aggregation_dict: Optional[AggregatedSectorDictType] = None
    sector_prefix_str: str = ""
    io_table_kwargs: dict[str, Any] = field(default_factory=dict)
    meta_data: Optional[MetaData] = None
    national_gva: str | Series | None = GROSS_VALUE_ADDED_INDEX_NAME
    national_net_subsidies: str | Series | None = NET_SUBSIDIES_COLUMN_NAME

    dog_leg_columns: dict[str, str] = field(default_factory=dict)
    dog_leg_rows: dict[str, str] = field(default_factory=dict)
    _process_full_io_table: Callable[..., DataFrame] = crop_io_table_to_sectors
    _table_load_func: Callable[..., DataFrame] = load_io_table_csv

    class NullIOTableError(Exception):
        pass

    class NoSectorAggregationDictError(Exception):
        pass

    def _init_base_io_tables(self) -> None:
        if (
            self.full_io_table is None
            and self.base_io_table is None
            and self.path is None
        ):
            raise self.NullIOTableError(
                "One of full_io_table, base_io_table or path attributes must be set."
            )
        if self.full_io_table is None and self.path:
            self.full_io_table: DataFrame = self._table_load_func(
                self.path, **self.io_table_kwargs
            )
        if self.base_io_table is None:  # Assumes full_io_table is set
            self.base_io_table = self._process_full_io_table(
                self.full_io_table, self.sectors, self.sector_prefix_str
            )
        if not self.meta_data and self.path == io_table_1841.CSV_FILE_NAME:
            self.meta_data = MetaData(
                name=io_table_1841.NAME,
                year=io_table_1841.YEAR,
                region=io_table_1841.REGION,
                authors=io_table_1841.AUTHORS,
                doi=io_table_1841.DOI,
            )

    @property
    def sectors(self) -> Series:
        """If sector_names is None, populate with sector_aggregation_dict keys, else error.

        Todo:
            * This may need a further refactor
            * Assume simpler! and possibly remote sector_names now that the
              index is managed in CPA
        """
        if self.sector_names is not None:
            return self.sector_names
        elif (
            self.sector_aggregation_dict
        ):  # Default to sector_aggregation_dict keys if sector_names not set
            logger.debug("Returning {self} sector_aggregation_dict keys")
            return Series(self.sector_aggregation_dict.keys())
        raise ValueError("Neither {self} sector_names nor sector_aggregation_dict set.")

    def __post_init__(self) -> None:
        self._init_base_io_tables()

    @property
    def _aggregated_sectors_dict(self) -> AggregatedSectorDictType:
        """Call aggregate_sector_dict on the sectors property."""
        if self.sector_aggregation_dict:
            return aggregate_sector_dict(
                self.sectors, self.sector_aggregation_dict, self.sector_prefix_str
            )
        else:
            raise self.NoSectorAggregationDictError

    def get_aggregated_io_table(self) -> DataFrame:
        """Return aggregated io_table"""
        return aggregate_io_table(
            self._aggregated_sectors_dict,
            self.base_io_table,
            self.dog_leg_columns,
            self.dog_leg_rows,
        )


@dataclass
class InputOutputCPATable(InputOutputTable):

    """Manage processing and aggregating CPA format Input Output Tables.

    Note:
     * CPA stands for Classification of Products by Activity, see
       https://ec.europa.eu/eurostat/web/cpa/cpa-2008
     * Sector aggregation defaults to 10 see
       https://ec.europa.eu/eurostat/documents/1965803/1978839/NACEREV.2INTRODUCTORYGUIDELINESEN.pdf/f48c8a50-feb1-4227-8fe0-935b58a0a332
    """

    path: FilePathType = ons_IO_2017.EXCEL_FILE_NAME
    cpa_column_name: str = CPA_COLUMN_NAME
    sector_prefix_str: str = CPA_COLUMN_NAME
    sector_aggregation_dict: Optional[AggregatedSectorDictType] = field(
        default_factory=lambda: deepcopy(SECTOR_10_CODE_DICT)
    )
    io_table_kwargs: dict[str, Any] = field(
        default_factory=lambda: {"sheet_name": IO_TABLE_NAME}
    )
    dog_leg_columns: dict[str, str] = field(
        default_factory=lambda: UK_DOG_LEG_CODES["columns"]
    )
    dog_leg_rows: dict[str, str] = field(
        default_factory=lambda: UK_DOG_LEG_CODES["rows"]
    )
    _first_code_row: int = ons_IO_2017.FIRST_CODE_ROW
    _io_table_code_to_labels_func: Callable[
        [DataFrame, str], DataFrame
    ] = io_table_to_codes
    _table_load_func: Callable[..., DataFrame] = load_io_table_excel

    def __post_init__(self) -> None:
        """Call the core _init_base_io_tables method and then set code_io_table.

        Todo:
            * Decide whether to make sector_names frozen
        """
        self._init_base_io_tables()
        self.code_io_table: DataFrame = self._io_table_code_to_labels_func(
            self.full_io_table, self.cpa_column_name
        )
        self.sector_names = self._CPA_sectors_to_names
        # for attr in ['national_gva', 'national_net_subsidies']:
        #     if isinstance(attr, str):
        #         logger.info(f"Setting {attr} from `self.io_table[{attr}]`.")
        #         setattr(self, f"_{attr}_str", getattr(self, attr))
        #         setattr(self, attr, self.code_io_table[attr])

    @property
    def _CPA_sectors_to_names(self) -> Series:
        """Series for mapping CPA codes to standard names."""
        return self.row_codes[
            self.row_codes.index.str.startswith(self.sector_prefix_str)
        ]

    @property
    def _CPA_index(self) -> Index:
        return self._CPA_sectors_to_names.index

    @property
    def row_codes(self) -> Series:  # Default skip first row
        """Return the values in the index_column (intended to return sector codes)."""
        if self.full_io_table is None:
            raise self.NullIOTableError

        return (
            self.full_io_table.reset_index()
            .set_index(self.cpa_column_name)
            .iloc[:, 0][self._first_code_row :]
        )

    @property
    def _aggregated_sectors_dict(self) -> AggregatedSectorDictType:
        """Call aggregate_sector_dict on the _CPA_index property."""
        if self.sector_aggregation_dict:
            return aggregate_sector_dict(
                self._CPA_index, self.sector_aggregation_dict, self.sector_prefix_str
            )
        else:
            raise self.NoSectorAggregationDictError

    def get_aggregated_io_table(self) -> DataFrame:
        """Return aggregated io_table"""
        return aggregate_io_table(
            self._aggregated_sectors_dict,
            self.code_io_table,
            self.dog_leg_columns,
            self.dog_leg_rows,
        )


def aggregate_sector_dict(
    sectors: Iterable[str],
    sector_aggregation_dict: AggregatedSectorDictType = SECTOR_10_CODE_DICT,
    sector_code_prefix: str = CPA_COLUMN_NAME,
) -> AggregatedSectorDictType:
    """Generate a dictionary to aid aggregating sector data."""
    aggregated_sectors: AggregatedSectorDictType = {}
    for sector, code_letters in sector_aggregation_dict.items():
        sector_list: list[str] = []
        for letter in code_letters:
            sector_list += [
                sector_code
                for sector_code in sectors
                if sector_code.startswith(f"{sector_code_prefix}_{letter}")
            ]
        aggregated_sectors[sector] = sector_list
    return aggregated_sectors
