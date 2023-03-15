from dataclasses import dataclass, field
from logging import getLogger
from typing import Any, Callable, Final, Sequence

from pandas import DataFrame, Series

from ..input_output_tables import (
    ACQUISITION_NET_VALUABLES_DISPOAL_COLUMN_NAME,
    CPA_COLUMN_NAME,
    CPA_IMPORTS_COST_INSURANCE_FREIGHT_ROW_NAME,
    CPA_TAXES_NET_SUBSIDIES_ROW_NAME,
    CPA_TOTAL_INTERMEDIATE_AT_PURCHASERS_PRICE,
    GROSS_CAPITAL_FORMATION_COLUMN_NAME,
    INVENTORY_CHANGE_COLUMN_NAME,
    IO_TABLE_DEFAULT_COLUMNS_NAME,
    IO_TABLE_DEFAULT_INDEX_NAME,
    DogLegType,
    InputOutputTable,
    InputOutputTableOECD,
    aggregate_io_table,
    aggregate_sectors_by_dict_with_prefix,
)
from ..sources import MetaFileOrDataFrameType
from ..utils import (
    SECTOR_10_CODE_DICT,
    SECTOR_COLUMN_NAME,
    AggregatedSectorDictType,
    DateType,
    YearType,
)
from . import io_table_1841, ons_IO_2017

logger = getLogger(__name__)

# IO_TABLE_NAME: Final[str] = "IOT"  # Todo: see if this is the standard sheet name
# COEFFICIENT_TABLE_NAME: Final[str] = "A"

# CPA_COLUMN_NAME: Final[str] = "CPA"
#
# FINAL_DEMAND_COLUMN_NAMES: Final[list[str]] = [
#     "Household Purchase",
#     "Government Purchase",
#     "Non-profit Purchase",
# ]

UK_EXPORT_COLUMN_NAMES: Final[list[str]] = [
    "Exports to EU",
    "Exports outside EU",
    "Exports of services",
]

SECTOR_DESC_COLUMN_NAME: Final[str] = "Product"
TOTAL_OUTPUT_COLUMN_NAME: Final[str] = "Total Purchase"

INTERMEDIATE_DEMAND_BASE_PRICE_ROW_NAME: Final[str] = "Intermediate Demand base price"
INTERMEDIATE_DEMAND_BASE_PRICE_CODE: Final[str] = "_T"
INTERMEDIATE_DEMAND_PRICE_ROW_NAME: Final[str] = "Intermediate Demand purchase price"
TOTAL_SALES_ROW_NAME: Final[str] = "Total Sales"
IMPORTS_ROW_NAME: Final[str] = "Imports"
GROSS_VALUE_ADDED_ROW_NAME: Final[str] = "Gross Value Added"
NET_SUBSIDIES_ROW_NAME: Final[str] = "Net subsidies"

INTERMEDIATE_ROW_NAME: Final[str] = "Intermediate/final use w/purchaser's prices"

UK_FINAL_DEMAND_HOUSEHOLD_PURCHASE_CODE: Final[str] = "P3 S14"
UK_FINAL_DEMAND_GOVERNMENT_PURCHASE_CODE: Final[str] = "P3 S13"
UK_FINAL_DEMAND_NON_PROFIT_PURCHASE_CODE: Final[str] = "P3 S15"


UK_FINAL_DEMAND_HOUSEHOLD_PURCHASE_LABEL: Final[str] = "Household Purchase"
UK_FINAL_DEMAND_GOVERNMENT_PURCHASE_LABEL: Final[str] = "Government Purchase"
UK_FINAL_DEMAND_NON_PROFIT_PURCHASE_LABEL: Final[str] = "Non-profit Purchase"

UK_FINAL_DEMAND_COLUMN_KEYS: Final[dict[str, str]] = {
    UK_FINAL_DEMAND_HOUSEHOLD_PURCHASE_CODE: UK_FINAL_DEMAND_HOUSEHOLD_PURCHASE_LABEL,
    UK_FINAL_DEMAND_GOVERNMENT_PURCHASE_CODE: UK_FINAL_DEMAND_GOVERNMENT_PURCHASE_LABEL,
    UK_FINAL_DEMAND_NON_PROFIT_PURCHASE_CODE: UK_FINAL_DEMAND_NON_PROFIT_PURCHASE_LABEL,
}

UK_EXPORTS_TO_EU_COLUMN_CODE: Final[str] = "P61EU"
UK_EXPORTS_OUTSIDE_EU_CODE: Final[str] = "P61RW"
UK_EXPORTS_OF_SERVICES_CODE: Final[str] = "P62"

UK_DOG_LEG_CODES: Final[dict[str, dict[str, str]]] = {
    "columns": {
        "Household Purchase": UK_FINAL_DEMAND_HOUSEHOLD_PURCHASE_CODE,
        "Government Purchase": UK_FINAL_DEMAND_GOVERNMENT_PURCHASE_CODE,
        "Non-profit Purchase": UK_FINAL_DEMAND_NON_PROFIT_PURCHASE_CODE,
        "Exports to EU": UK_EXPORTS_TO_EU_COLUMN_CODE,
        "Exports outside EU": UK_EXPORTS_OUTSIDE_EU_CODE,
        "Exports of services": UK_EXPORTS_OF_SERVICES_CODE,
        GROSS_CAPITAL_FORMATION_COLUMN_NAME: "P51G",
        INVENTORY_CHANGE_COLUMN_NAME: "P52",
        ACQUISITION_NET_VALUABLES_DISPOAL_COLUMN_NAME: "P53",
        TOTAL_OUTPUT_COLUMN_NAME: "TD",
    },
    "rows": {
        INTERMEDIATE_DEMAND_BASE_PRICE_ROW_NAME: INTERMEDIATE_DEMAND_BASE_PRICE_CODE,
        IMPORTS_ROW_NAME: "Imports",
        NET_SUBSIDIES_ROW_NAME: NET_SUBSIDIES_ROW_NAME,
        INTERMEDIATE_DEMAND_PRICE_ROW_NAME: INTERMEDIATE_ROW_NAME,
        "Employee Compensation": "D1",
        GROSS_VALUE_ADDED_ROW_NAME: "GVA",
        TOTAL_SALES_ROW_NAME: "P1",
    },
}

# UK_DOG_LEG_CODES: Final[dict[str, dict[str, str]]] = {
#     "columns": {
#         "Household Purchase": "P3 S14",
#         "Government Purchase": "P3 S13",
#         "Non-profit Purchase": "P3 S15",
#         "Exports to EU": "P61EU",
#         "Exports outside EU": "P61RW",
#         "Exports of services": "P62",
#         GROSS_CAPITAL_FORMATION_COLUMN_NAME: "P51G",
#         INVENTORY_CHANGE_COLUMN_NAME: "P52",
#         ACQUISITION_NET_VALUABLES_DISPOAL_COLUMN_NAME: "P53",
#         TOTAL_OUTPUT_COLUMN_NAME: "TD",
#     },
#     "rows": {
#         TOTAL_PRODUCTION_ROW_NAME: "_T",
#         "Imports": "Imports",
#         NET_SUBSIDIES_COLUMN_NAME: NET_SUBSIDIES_COLUMN_NAME,
#         "Intermediate Demand purchase price": INTERMEDIATE_COLUMN_NAME,
#         "Employee Compensation": "D1",
#         GROSS_VALUE_ADDED_ROW_NAME: "GVA",
#         "Total Sales": "P1",
#     },
# }

logger.warning(
    f"Currently set default UK_2017_IO_TABLE_SCALING to: {ons_IO_2017.ONS_2017_IO_TABLE_SCALING}"
)


def fix_empty_col_and_na_values(
    df: DataFrame,
    sector_names: Sequence[str] = io_table_1841.HISTORIC_UK_SECTORS,
    drop_col: str = "Unnamed: 21",
    sector_col_name: str = SECTOR_COLUMN_NAME,
) -> DataFrame:
    """Drop column `Unnamed: 21`, replace `na` with 0.0 and `Sectors` -> `Sector`."""
    assert df[drop_col].isnull().all()
    df = df.drop(drop_col, axis=1)
    df[sector_names] = df[sector_names].fillna(0.0)
    df.loc[sector_names] = df.loc[sector_names].fillna(0.0)
    df.index = df.index.rename(sector_col_name)
    return df, df.index, df.columns


@dataclass(repr=False)
class InputOutputTableUK1841(InputOutputTable):

    """UK InputOutputTable 1841 estimate from Horrel et. al.

    Todo:
        * May need kw_only=True and repr=False
    """

    # meta_data: MetaData = io_table_1841.METADATA
    raw_io_table: MetaFileOrDataFrameType = field(
        default_factory=lambda: io_table_1841.METADATA
    )
    date: DateType | YearType = 1841
    all_regions: Sequence[str] | str = ("UK",)
    all_sectors: Sequence[str] = field(
        default_factory=lambda: io_table_1841.HISTORIC_UK_SECTORS
    )
    final_demand_column_names: list[str] = field(
        default_factory=lambda: ["Consumption", "Investment"]
    )
    full_io_table_func: Callable = fix_empty_col_and_na_values


@dataclass(kw_only=True, repr=False)
class InputOutputOECDTableUK2017(InputOutputTableOECD):
    date = 2017


def arrange_cpa_io_table(
    io_table: DataFrame,
    cpa_row_name: str | None = CPA_COLUMN_NAME,
    cpa_column_name: str | None = CPA_COLUMN_NAME,
    sector_desc_column_name: str = SECTOR_DESC_COLUMN_NAME,
    imports_row_name: str = IMPORTS_ROW_NAME,
    net_subsidies_row_name: str = NET_SUBSIDIES_ROW_NAME,
    intermediate_row_name: str = INTERMEDIATE_ROW_NAME,
    cpa_import_cpa_row_name: str = CPA_IMPORTS_COST_INSURANCE_FREIGHT_ROW_NAME,
    cpa_taxes_net_subsidies_row_name: str = CPA_TAXES_NET_SUBSIDIES_ROW_NAME,
    cpa_intermediate_at_purchase_price_row_name: str = CPA_TOTAL_INTERMEDIATE_AT_PURCHASERS_PRICE,
    input_index_label: str = IO_TABLE_DEFAULT_INDEX_NAME,
    output_column_label: str = IO_TABLE_DEFAULT_COLUMNS_NAME,
) -> tuple[DataFrame, Series, Series]:
    """Standardise CPA rows and columns.

    Todo:
        * Check if cpa parameters are too UK sepecific
        * If possible more to estios/uk/intput_output_table.py
        * See https://www.ons.gov.uk/economy/grossdomesticproductgdp/compendium/unitedkingdomnationalaccountsthebluebook/2022/pdf
    """
    io_table.loc[sector_desc_column_name][0] = cpa_row_name
    io_table.loc[cpa_row_name][0] = cpa_row_name
    io_table.columns = io_table.loc[sector_desc_column_name]
    io_table.drop(sector_desc_column_name, inplace=True)

    io_table.loc[cpa_import_cpa_row_name][cpa_column_name] = imports_row_name
    io_table.loc[cpa_taxes_net_subsidies_row_name][
        cpa_row_name
    ] = net_subsidies_row_name
    io_table.loc[cpa_intermediate_at_purchase_price_row_name][
        cpa_column_name
    ] = intermediate_row_name
    io_table.index.name = input_index_label
    io_table.columns.name = output_column_label
    input_row_labels: Series = io_table.index
    output_column_labels: Series = io_table.columns
    input_row_labels = input_row_labels.drop(cpa_row_name)
    output_column_labels = output_column_labels.drop(cpa_column_name)
    input_row_labels = input_row_labels.map(lambda x: x.strip())
    output_column_labels = output_column_labels.map(lambda x: x.strip())
    # io_table.set_index(cpa_column_name, inplace=True)# = io_table.loc[cpa_row_name, :]
    io_table.set_index(cpa_column_name, inplace=True)  # = io_table.loc[cpa_row_name, :]
    io_table.columns = io_table.loc[cpa_row_name, :]
    # io_table.index = io_table.loc[:, cpa_column_name]
    io_table.drop(cpa_row_name, inplace=True)
    # io_table.drop(cpa_column_name, axis="columns", inplace=True)
    io_table.index.name = input_index_label
    io_table.columns.name = output_column_label
    assert len(io_table.index) == len(input_row_labels)
    assert len(io_table.columns) == len(output_column_labels)
    return io_table, input_row_labels, output_column_labels


@dataclass(kw_only=True, repr=False)
class InputOutputTableUK2017(InputOutputTable):

    """UK InputOutputCPATable 2017 estimate from the ONS."""

    # meta_data: MetaData = ons_IO_2017.ONS_IO_TABLE_2017_METADATA
    raw_io_table: MetaFileOrDataFrameType = field(
        default_factory=lambda: ons_IO_2017.ONS_IO_TABLE_2017_METADATA
    )
    io_scaling_factor: float = ons_IO_2017.ONS_2017_IO_TABLE_SCALING
    # all_sectors: Sequence[str] | str = 'CPA'
    # all_sector_lables: Sequence[str] | str = 'Product'
    all_regions: Sequence[str] | str = ("UK",)

    # sector_names: Series | None = field(default_factory=lambda: Series(SECTOR_10_CODE_DICT))
    final_demand_column_names: list[str] = field(
        default_factory=lambda: list(UK_FINAL_DEMAND_COLUMN_KEYS.keys())
    )
    final_demand_column_labels: list[str] = field(
        default_factory=lambda: list(UK_FINAL_DEMAND_COLUMN_KEYS.values())
    )
    gross_value_added_row_name: str | None = GROSS_VALUE_ADDED_ROW_NAME
    intermediate_demand_base_price_row_name: str | None = (
        INTERMEDIATE_DEMAND_BASE_PRICE_CODE
    )
    intermediate_demand_row_name: str | None = INTERMEDIATE_ROW_NAME
    dog_leg_columns: DogLegType = field(
        default_factory=lambda: UK_DOG_LEG_CODES["columns"]
    )
    dog_leg_rows: DogLegType = field(default_factory=lambda: UK_DOG_LEG_CODES["rows"])
    sector_aggregation_dict: AggregatedSectorDictType = field(
        default_factory=lambda: SECTOR_10_CODE_DICT
    )
    full_io_table_func: Callable[..., DataFrame] | None = arrange_cpa_io_table
    sector_codes_skip: Sequence[str] = field(
        default_factory=lambda: [INTERMEDIATE_DEMAND_BASE_PRICE_CODE]
    )
    _aggregate_sectors_func: Callable[
        ..., DataFrame
    ] | None = aggregate_sectors_by_dict_with_prefix
    _aggregate_sectors_kwargs: dict[str, Any] = field(
        default_factory=lambda: {"sector_code_prefix": CPA_COLUMN_NAME}
    )
    _aggregate_io_table_func: Callable[..., DataFrame] | None = aggregate_io_table

    # full_io_table_kwargs: dict[str, Any] = field(default_factory=dict)
    # _first_code_row: int = ons_IO_2017.FIRST_CODE_ROW
    # _post_read_func: Callable | None = arrange_cpa_io_table


#     _post_read_kwargs: dict[str, Any] = field(
#             default_factory=lambda: dict(
#                 sector_desc_row_name=SECTOR_DESC_COLUMN_NAME,
#                 imports_row_name=IMPORTS_ROW_NAME,
#                 net_subsidies_row_name=NET_SUBSIDIES_ROW_NAME,
#                 intermediate_row_name=INTERMEDIATE_ROW_NAME,
#                 cpa_import_cpa_row_name=CPA_IMPORTS_COST_INSURANCE_FREIGHT_ROW_NAME,
#                 )
#             )
#
#     io_table: DataFrame,
#     cpa_row_name: str | None = None,
#     sector_desc_column_name: str = SECTOR_DESC_COLUMN_NAME,
#     imports_row_name: str = IMPORTS_ROW_NAME,
#     net_subsidies_row_name: str = NET_SUBSIDIES_ROW_NAME,
#     intermediate_row_name: str = INTERMEDIATE_ROW_NAME,
#     cpa_import_cpa_row_name: str = CPA_IMPORTS_COST_INSURANCE_FREIGHT_ROW_NAME,
#     cpa_taxes_net_subsidies_row_name: str = CPA_TAXES_NET_SUBSIDIES_ROW_NAME,
#     cpa_intermediate_at_purchase_price_row_name: str = CPA_TOTAL_INTERMEDIATE_AT_PURCHASERS_PRICE,
#
# io_table = arrange_cpa_io_table(
#     io_table,
#     cpa_column_name,
#     sector_desc_column_name,
#     imports_column_name,
#     net_subsidies_column_name,
#     intermediate_column_name,
# )
