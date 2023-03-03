from dataclasses import dataclass, field
from os import PathLike
from typing import Any, Optional, Type

from ..input_output_tables import (  # TOTAL_PRODUCTION_ROW_NAME,; UK_EXPORT_COLUMN_NAMES,
    InputOutputTable,
)
from ..models import InterRegionInputOutput
from ..sources import MetaData, MetaFileOrDataFrameType
from ..utils import DateType
from . import ons_IO_2017
from .input_output_tables import InputOutputTableUK2017
from .ons_employment_2017 import (
    CITY_SECTOR_REGION_PREFIX,
    EMPLOYMENT_QUARTER_DEC_2017,
    EMPLOYMENT_QUARTER_JUN_2017,
    NOMIS_2017_SECTOR_EMPLOYMENT_METADATA,
    UK_JOBS_BY_SECTOR_SCALING,
    UK_JOBS_BY_SECTOR_XLS_FILE_NAME,
)
from .regions import CENTRE_FOR_CITIES_CSV_FILE_NAME, CITIES_TOWNS_GEOJSON_FILE_NAME
from .utils import UK_NATIONAL_COLUMN_NAME

# InterRegionInputOutputUK2017 = InterRegionInputOutput.load_from_file(
#     io_table_meta_data = ons_IO_2017.ONS_IO_TABLE_2017_METADATA,
#     date=EMPLOYMENT_QUARTER_JUN_2017,
# )


@dataclass(repr=False, kw_only=True)
class InterRegionInputOutputUK2017(InterRegionInputOutput):

    """InterRegionInputOutput customised for 2017 UK defaults."""

    raw_io_table: MetaData = ons_IO_2017.ONS_IO_TABLE_2017_METADATA
    # io_table_file_path: PathLike = ons_IO_2017.EXCEL_FILE_NAME
    io_table_scale: float = ons_IO_2017.ONS_2017_IO_TABLE_SCALING
    national_employment_scale: float = UK_JOBS_BY_SECTOR_SCALING
    national_employment_path: Optional[PathLike] = UK_JOBS_BY_SECTOR_XLS_FILE_NAME
    employment_date: Optional[DateType] = EMPLOYMENT_QUARTER_DEC_2017
    io_table_kwargs: dict[str, Any] = field(default_factory=dict)
    region_attributes_path: PathLike = CENTRE_FOR_CITIES_CSV_FILE_NAME
    region_spatial_path: PathLike = CITIES_TOWNS_GEOJSON_FILE_NAME
    region_type_prefix: str = CITY_SECTOR_REGION_PREFIX
    date = EMPLOYMENT_QUARTER_JUN_2017
    region_sector_employment: MetaFileOrDataFrameType = (
        NOMIS_2017_SECTOR_EMPLOYMENT_METADATA
    )
    employment_by_sector_and_region: MetaFileOrDataFrameType = (
        NOMIS_2017_SECTOR_EMPLOYMENT_METADATA
    )
    national_column_name: str = UK_NATIONAL_COLUMN_NAME
    _io_table_cls: Type[InputOutputTable] = InputOutputTableUK2017


#     national_employment_path: Optional[PathLike] = UK_JOBS_BY_SECTOR_XLS_FILE_NAME
#     io_table_kwargs: dict[str, Any] = field(default_factory=dict)
#     region_attributes_path: PathLike = CENTRE_FOR_CITIES_CSV_FILE_NAME
#     region_spatial_path: PathLike = CITIES_TOWNS_GEOJSON_FILE_NAME
#     region_type_prefix: str = CITY_SECTOR_REGION_PREFIX
#     total_sales_row_name: str = TOTAL_SALES_ROW_NAME
#     imports_column_name: str = IMPORTS_ROW_NAME
#     _io_table_cls: Type[InputOutputTable] = InputOutputCPATable
#     _employment_by_sector_and_region: Optional[DataFrame] = None
#     _raw_region_data: Optional[DataFrame] = None
#     _region_load_func: Callable[
#         ..., GeoDataFrame
#     ] = load_and_join_centre_for_cities_data
