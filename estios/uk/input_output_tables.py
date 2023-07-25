from dataclasses import dataclass, field
from logging import getLogger
from typing import Any, Callable, Final, Sequence

from pandas import DataFrame

from ..input_output_tables import (
    ACQUISITION_NET_VALUABLES_DISPOSAL_COLUMN_NAME,
    CPA_COLUMN_NAME,
    GROSS_CAPITAL_FORMATION_COLUMN_NAME,
    INVENTORY_CHANGE_COLUMN_NAME,
    DogLegType,
    InputOutputTable,
    InputOutputTableOECD,
    aggregate_io_table,
    aggregate_sectors_by_dict_with_prefix,
)
from ..sources import MetaFileOrDataFrameType
from ..utils import SECTOR_10_CODE_DICT, AggregatedSectorDictType, DateType, YearType
from . import io_table_1841, ons_IO_2017
from .utils import UK_NATIONAL_COLUMN_NAME

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

TOTAL_OUTPUT_COLUMN_NAME: Final[str] = "Total Purchase"

INTERMEDIATE_DEMAND_BASE_PRICE_ROW_NAME: Final[str] = "Intermediate Demand base price"
INTERMEDIATE_DEMAND_BASE_PRICE_CODE: Final[str] = "_T"
INTERMEDIATE_DEMAND_PRICE_ROW_NAME: Final[str] = "Intermediate Demand purchase price"
TOTAL_SALES_ROW_NAME: Final[str] = "Total Sales"
IMPORTS_ROW_NAME: Final[str] = "Imports"
GROSS_VALUE_ADDED_ROW_NAME: Final[str] = "Gross Value Added"
NET_SUBSIDIES_ROW_NAME: Final[str] = "Net subsidies"

# INTERMEDIATE_ROW_NAME: Final[str] = "Intermediate/final use w/purchaser's prices"

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
UK_TOTAL_USE_AT_BASIC_PRICE: Final[str] = "TU"

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
        ACQUISITION_NET_VALUABLES_DISPOSAL_COLUMN_NAME: "P53",
        TOTAL_OUTPUT_COLUMN_NAME: UK_TOTAL_USE_AT_BASIC_PRICE,
    },
    "rows": {
        INTERMEDIATE_DEMAND_BASE_PRICE_ROW_NAME: INTERMEDIATE_DEMAND_BASE_PRICE_CODE,
        IMPORTS_ROW_NAME: "Imports",
        NET_SUBSIDIES_ROW_NAME: NET_SUBSIDIES_ROW_NAME,
        INTERMEDIATE_DEMAND_PRICE_ROW_NAME: ons_IO_2017.INTERMEDIATE_ROW_NAME,
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
#         ACQUISITION_NET_VALUABLES_DISPOSAL_COLUMN_NAME: "P53",
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
    #     * May need kw_only=Ture and repr=False
    # """

    # meta_data: MetaData = io_table_1841.METADATA
    # raw_io_table: MetaFileOrDataFrameType = io_table_1841.METADATA
    date: DateType | YearType = 1841
    all_regions: Sequence[str] | str = (UK_NATIONAL_COLUMN_NAME,)
    all_sectors: Sequence[str] = field(
        default_factory=lambda: io_table_1841.HISTORIC_UK_SECTORS
    )
    final_demand_column_names: list[str] = field(
        default_factory=lambda: io_table_1841.FINAL_DEMAND_COLUMN_NAMES
    )


@dataclass(kw_only=True, repr=False)
class InputOutputOECDTableUK2017(InputOutputTableOECD):
    date = 2017


@dataclass(kw_only=True, repr=False)
class InputOutputTableUK2017(InputOutputTable):

    """UK InputOutputCPATable 2017 estimate from the ONS."""

    # meta_data: MetaData = ons_IO_2017.ONS_IO_TABLE_2017_METADATA
    # io_table_full: MetaFileOrDataFrameType = field(
    #     default_factory=lambda: ons_IO_2017.ONS_IO_TABLE_2017_METADATA
    # )
    raw_io_table: MetaFileOrDataFrameType = field(
        default_factory=lambda: ons_IO_2017.ONS_IO_TABLE_2017_METADATA
    )
    # raw_io_table: MetaFileOrDataFrameType = ons_IO_2017.ONS_IO_TABLE_2017_METADATA
    io_scaling_factor: float = ons_IO_2017.ONS_2017_IO_TABLE_SCALING
    # all_sectors: Sequence[str] | str = 'CPA'
    # all_sector_lables: Sequence[str] | str = 'Product'
    all_regions: Sequence[str] | str = (UK_NATIONAL_COLUMN_NAME,)

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
    intermediate_demand_row_name: str | None = ons_IO_2017.INTERMEDIATE_ROW_NAME
    dog_leg_columns: DogLegType = field(
        default_factory=lambda: UK_DOG_LEG_CODES["columns"]
    )
    dog_leg_rows: DogLegType = field(default_factory=lambda: UK_DOG_LEG_CODES["rows"])
    sector_aggregation_dict: AggregatedSectorDictType = field(
        default_factory=lambda: SECTOR_10_CODE_DICT
    )
    # full_io_table_func: Callable[..., DataFrame] | None = ons_IO_2017.arrange_cpa_io_table
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
