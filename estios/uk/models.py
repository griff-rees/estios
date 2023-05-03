from copy import deepcopy
from dataclasses import dataclass, field
from os import PathLike
from typing import Optional, Type

from pandas import DataFrame, Series

from ..input_output_tables import (  # TOTAL_PRODUCTION_ROW_NAME,; UK_EXPORT_COLUMN_NAMES,
    GROSS_VALUE_ADDED_ROW_NAME,
    IMPORTS_ROW_NAME,
    NET_SUBSIDIES_COLUMN_NAME,
    InputOutputTable,
)
from ..models import InterRegionInputOutput
from ..sources import MetaData, MetaFileOrDataFrameType
from ..utils import DateConfigType, DateType
from . import ons_IO_2017
from .input_output_tables import UK_EXPORT_COLUMN_NAMES, InputOutputTableUK2017
from .ons_employment_2017 import (
    CITY_SECTOR_REGION_PREFIX,
    EMPLOYMENT_QUARTER_DEC_2017,
    EMPLOYMENT_QUARTER_JUN_2017,
)
from .ons_uk_population_history import UK_NATIONAL_POPULATION_2017
from .populations import (
    NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA,
    NOMIS_REGIONAL_EMPLOYMENT_2017_METADATA,
    ONS_MID_YEAR_POPULATIONS_2017_METADATA,
)
from .regions import (
    CENTRE_FOR_CITIES_CSV_FILE_NAME,
    CITIES_TOWNS_GEOJSON_FILE_NAME,
    TEN_UK_CITY_REGIONS,
)
from .utils import UK_NAME, UK_NATIONAL_COLUMN_NAME


@dataclass(repr=False, kw_only=True)
class InterRegionInputOutputUK2017(InterRegionInputOutput):

    """InterRegionInputOutput customised for 2017 UK defaults.

    Todo:
        * Check if NOMIS needs scaling factors
    """

    nation_name: str = UK_NAME
    national_column_name: str = UK_NATIONAL_COLUMN_NAME
    # MetaData to be treated specially *beyond* the reader_func
    regions: dict[str, str] | list[str] = field(
        default_factory=lambda: deepcopy(TEN_UK_CITY_REGIONS)
    )
    raw_io_table: MetaData = field(
        default_factory=lambda: ons_IO_2017.ONS_IO_TABLE_2017_METADATA
    )
    io_table_scale: float = ons_IO_2017.ONS_2017_IO_TABLE_SCALING
    # io_table_file_path: PathLike = ons_IO_2017.EXCEL_FILE_NAME

    # Needs to be aggregated by sector letters
    national_employment: Series | MetaFileOrDataFrameType = field(
        default_factory=lambda: NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA,
    )
    # Following Bowan's approach
    national_distance: float = 703.5

    # Needs to be aggregated by sector letters
    regional_employment: DataFrame | MetaFileOrDataFrameType = field(
        default_factory=lambda: NOMIS_REGIONAL_EMPLOYMENT_2017_METADATA,
    )
    # Check if NOMIS API requires scaling factors
    national_employment_scale: float = 1.0
    regional_employment_scale: float = 1.0

    regional_populations: Series | MetaFileOrDataFrameType = field(
        default_factory=lambda: ONS_MID_YEAR_POPULATIONS_2017_METADATA,
    )

    # Remove the NONE option
    regional_working_populations: Series | MetaFileOrDataFrameType | None = None
    region_attributes_path: PathLike = CENTRE_FOR_CITIES_CSV_FILE_NAME
    region_spatial_path: PathLike = CITIES_TOWNS_GEOJSON_FILE_NAME
    region_type_prefix: str = CITY_SECTOR_REGION_PREFIX
    date = EMPLOYMENT_QUARTER_JUN_2017
    employment_date: Optional[DateType] = EMPLOYMENT_QUARTER_JUN_2017
    national_population: float = UK_NATIONAL_POPULATION_2017
    national_gva_row_name: str = GROSS_VALUE_ADDED_ROW_NAME
    national_net_subsidies_row_name: str = NET_SUBSIDIES_COLUMN_NAME
    national_gov_investment_column_names: tuple[
        str, ...
    ] = ons_IO_2017.UK_GOV_INVESTMENT_COLUMN_NAMES

    export_column_names: list[str] = field(
        default_factory=lambda: UK_EXPORT_COLUMN_NAMES
    )
    import_row_names: list[str] = field(default_factory=lambda: [IMPORTS_ROW_NAME])
    _io_table_cls: Type[InputOutputTable] = InputOutputTableUK2017

    def __post_init__(self):
        super().__post_init__()


ONS_2017_DEFAULT_CONFIG: DateConfigType = {
    EMPLOYMENT_QUARTER_DEC_2017: {
        "raw_io_table": ons_IO_2017.ONS_IO_TABLE_2017_METADATA,
        # "io_table_file_path": ons_IO_2017.ONS_IO_TABLE_2017_METADATA,
    },
}
