#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import OrderedDict
from collections.abc import MutableSequence
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date
from functools import cached_property
from logging import getLogger
from os import PathLike
from typing import (
    Any,
    Callable,
    Collection,
    Iterable,
    Iterator,
    Optional,
    Type,
    Union,
    overload,
)
from warnings import filterwarnings

from geopandas import GeoDataFrame
from pandas import DataFrame, MultiIndex, Series
from shapely.errors import ShapelyDeprecationWarning

from .calc import (
    DEFAULT_IMPORT_EXPORT_ITERATIONS,
    DISTANCE_UNIT_DIVIDE,
    INITIAL_P,
    E_i_m_scaled,
    F_i_m_scaled,
    I_m,
    M_i_m_scaled,
    S_m,
    X_i_m_scaled,
    X_m,
    calc_region_distances,
    generate_e_m_dataframe,
    gross_value_added,
    import_export_convergence,
    region_and_sector_convergence,
    regional_io_projection,
    technical_coefficients,
    x_i_mn_summed,
)
from .input_output_tables import (  # GROSS_VALUE_ADDED_INDEX_NAME,; UK_EXPORT_COLUMN_NAMES,; TOTAL_PRODUCTION_ROW_NAME,; aggregate_io_table,
    FINAL_DEMAND_COLUMN_NAMES,
    GROSS_VALUE_ADDED_ROW_NAME,
    IMPORTS_ROW_NAME,
    NET_SUBSIDIES_COLUMN_NAME,
    TOTAL_PRODUCTION_ROW_NAME,
    TOTAL_SALES_ROW_NAME,
    InputOutputCPATable,
    InputOutputTable,
)
from .sources import (
    MetaFileOrDataFrameType,
    ModelDataSourcesHandler,
    pandas_from_path_or_package,
)
from .spatial import AttractionConstrained, SpatialInteractionBaseClass
from .uk import ons_IO_2017
from .uk.ons_employment_2017 import (  # CITY_SECTOR_REGION_PREFIX,; EMPLOYMENT_QUARTER_DEC_2017,; UK_JOBS_BY_SECTOR_SCALING,; UK_JOBS_BY_SECTOR_XLS_FILE_NAME,; NOMIS_2017_SECTOR_EMPLOYMENT_METADATA,; load_employment_by_region_and_sector_csv,
    load_region_employment_excel,
)
from .uk.regions import UK_CITY_REGIONS, load_and_join_centre_for_cities_data
from .utils import (
    SECTOR_10_CODE_DICT,
    AggregatedSectorDictType,
    DateType,
    RegionConfigType,
    RegionNamesListType,
    SectorConfigType,
    SectorNamesListType,
    aggregate_rows,
    collect_dupes,
    column_to_series,
    filter_attrs_by_substring,
    filter_by_region_name_and_type,
    generate_ij_index,
    generate_ij_m_index,
    iter_attr_by_key,
    str_keys_of_dict,
    tuples_to_ordered_dict,
)

logger = getLogger(__name__)

filterwarnings("ignore", category=ShapelyDeprecationWarning)

# DEFAULT_TIME_SERIES_CONFIG: DateConfigType = {
#     EMPLOYMENT_QUARTER_DEC_2017: {
#         "io_table_file_path": ons_IO_2017.EXCEL_FILE_NAME,
#     }
# }

# <<<<<<< Updated upstream
NamesListType = Union[list[str], Collection[str]]
# DateType: TypeAlias = date


# @dataclass(kw_only=True)
# class InterRegionInputOutputBaseClass:
# =======


@dataclass(kw_only=True)
class InterRegionInputOutputBaseClass(ModelDataSourcesHandler):
    # >>>>>>> Stashed changes

    """Bass attributes for InputOutput Model and TimeSeries.

    Todo:
        * Refactor raw_regions vs regions
        * Refactor raw_sectors vs sector_aggregation
        * Move column_name attributes to InputOutputTable class
        * Consider moving sector names etc to InputOutputTable class
        * Check if io_table_scale is duplicated with input_output_table config
    """

    raw_io_table: MetaFileOrDataFrameType | InputOutputTable
    max_import_export_model_iterations: int = DEFAULT_IMPORT_EXPORT_ITERATIONS
    employment_by_sector_and_region: MetaFileOrDataFrameType | None = None
    # io_table_meta_data: MetaData | None = None
    raw_regions: dict[str, str] = field(default_factory=dict)
    regions: dict[str, str] | list[str] = field(
        default_factory=lambda: deepcopy(UK_CITY_REGIONS)
    )

    raw_sectors: dict[str, str] = field(default_factory=dict)
    sector_aggregation: Optional[AggregatedSectorDictType] = field(
        default_factory=lambda: deepcopy(SECTOR_10_CODE_DICT)
    )
    P_initial_export_proportion: float = INITIAL_P

    date: Optional[DateType] = None
    distance_unit_factor: float = DISTANCE_UNIT_DIVIDE
    final_demand_column_names: list[str] = field(
        default_factory=lambda: FINAL_DEMAND_COLUMN_NAMES
    )
    export_column_names: list[str] = field(
        # default_factory=lambda: UK_EXPORT_COLUMN_NAMES
        default_factory=list
    )
    # imports_column_name: str = IMPORTS_COLUMN_NAME
    total_production_index_name: str = TOTAL_PRODUCTION_ROW_NAME
    imports_column_name: str = IMPORTS_ROW_NAME
    # total_production_column_name: str = TOTAL_PRODUCTION_ROW_NAME
    raw_national_employment: Optional[DataFrame | Series] = None
    national_employment: Optional[Series] = None
    national_employment_scale: float = 1.0
    io_table_scale: float = 1.0
    national_population: Optional[float] = None
    national_working_population: Optional[float] = None
    national_gva_row_name: str = GROSS_VALUE_ADDED_ROW_NAME
    national_net_subsidies_row_name: str = NET_SUBSIDIES_COLUMN_NAME
    national_gov_investment_column_names: tuple[
        str, ...
    ] = ons_IO_2017.UK_GOV_INVESTMENT_COLUMN_NAMES
    regional_populations: Optional[Series] = None
    regional_working_populations: Optional[Series] = None
    regional_employment: Optional[DataFrame] = None
    regional_employment_scale: float = 1.0
    _io_table_cls: Type[InputOutputTable] = InputOutputTable
    _spatial_model_cls: Type[SpatialInteractionBaseClass] = AttractionConstrained
    _exogenous_i_m_func: Callable[..., Series] = region_and_sector_convergence
    _import_export_convergence: Callable[..., DataFrame] = import_export_convergence

    def _process_raw_io_table(self) -> InputOutputTable:
        if not isinstance(self.raw_io_table, self._io_table_cls):
            logger.info(
                f"{self} `raw_io_table` attribute needs conversion from type "
                f"{type(self.raw_io_table)}. Will try instantiating a {self._io_table_cls} ."
            )
            for attr_name, _ in filter_attrs_by_substring(self, "_raw_io_table"):
                logger.debug(f"Removing {attr_name} from {self}.")
                delattr(self, attr_name)
            processed_io_table = self._io_table_cls(
                raw_io_table=self.raw_io_table,
                sector_names=self.sector_names,
                date=self.date,
                io_scaling_factor=self.io_table_scale,
                sector_aggregation_dict=self.sector_aggregation,
            )
            assert type(processed_io_table) == self._io_table_cls
            for attr_name, attr_value in filter_attrs_by_substring(
                self, "_raw_io_table"
            ):
                setattr(self, attr_name, attr_value)
            self.raw_io_table = processed_io_table
        return self.raw_io_table

    def __post_init__(self):
        self._set_all_meta_file_or_data_fields()
        # _ = self._process_raw_io_table()

        #     self.raw_io_table =
        #     self._get_meta_file_or_data_field('raw_io_table', parser=InputOutputTable)
        #     assert False
        # if isinstance(self.raw_io_table, InputOutputTable):
        #     if self.sector_aggregation:
        #         return self.raw_io_table.get_aggregated_io_table() * self.io_table_scale
        #     elif isinstance(self.raw_io_table, InputOutputCPATable):
        #         return self.raw_io_table.code_io_table * self.io_table_scale
        #     else:
        #         return self.raw_io_table.base_io_table * self.io_table_scale
        # elif isinstance(self.raw_io_table, DataFrame):
        #     if self.sector_aggregation:
        #         return aggregate_io_table(self.sector_aggregation, self.raw_io_table, [], [])
        #     else:
        #         return self.raw_io_table * self.io_table_scale
        # else:
        #     raise TypeError(f"{self} `raw_io_table` attribute needs conversion from type {type(self.raw_io_table)}. Try running `self._set_all_meta_file_or_data_fields()`.")

    @property
    def region_names(self) -> list[str]:
        """Return the region names."""
        if isinstance(self.regions, dict):
            return list(self.regions.keys())
        else:
            return self.regions

    @property
    def sectors(self) -> list[str]:
        """A list of sectors used in the model

        Todo:
            * Manage disambiguation between sectors and sector_names.
        """
        if self.sector_aggregation:
            return [*self.sector_aggregation]
        elif self.raw_sectors:
            return [*self.raw_sectors]
        else:
            logger.warning(f"No sectors specified")
            return []

    @property
    def sector_names(self) -> list[str]:
        """Return the region names."""
        if isinstance(self.sectors, dict):
            return list(self.sectors.keys())
        else:
            return self.sectors

    @cached_property
    def technical_coefficients(self) -> DataFrame:
        """Return the technical coefficients derived from `self.io_table`.

        Todo:
            * Refactor to avoid `self.raw_io_table` vs `self._raw_io_table` ambiguity.
        """
        # if not self.raw_io_table:
        #     if hasattr(self, '_raw_io_table'):
        #         return self._raw_io_table.technical_coefficients
        # elif self.raw_io_table:
        # return self.raw_io_table.technical_coefficients
        return technical_coefficients(
            self.io_table, self.final_demand_column_names, self.sectors
        )


class MissingIOTable(Exception):
    pass


# @dataclass(kw_only=True)
@dataclass(kw_only=True, repr=False)
class InterRegionInputOutput(InterRegionInputOutputBaseClass):

    """Manage Inter Region input output model runs.

    Note:
        * `kw_only` ensures the order of parameters is not pbroblematic with inheritance
        * `repr` determines if a new `repr` method is auto-generated (or not)

    Todo:
        * Abstract path for employment data to ease setting directly.
        * Manage extracting time point result rather than filtering each time (eg: need for employment date)
        * Remove regional attributes and regional spatial path to ease copying
    """

    # io_table_file_path: PathLike = ons_IO_2017.EXCEL_FILE_NAME
    # region_sector_employment: MetaFileOrDataFrameType | None  = None
    national_employment_path: Optional[
        PathLike
    ] = None  # UK_JOBS_BY_SECTOR_XLS_FILE_NAME
    employment_date: Optional[DateType] = None  # EMPLOYMENT_QUARTER_DEC_2017
    io_table_kwargs: dict[str, Any] = field(default_factory=dict)
    region_attributes_path: PathLike | None = None  # CENTRE_FOR_CITIES_CSV_FILE_NAME
    region_spatial_path: PathLike | None = None  # CITIES_TOWNS_GEOJSON_FILE_NAME
    region_type_prefix: str | None = None  # CITY_SECTOR_REGION_PREFIX
    total_sales_row_name: str = TOTAL_SALES_ROW_NAME
    imports_column_name: str = IMPORTS_ROW_NAME
    national_column_name: str = "UK"  # To be replaced in future
    _io_table_cls: Type[InputOutputTable] = InputOutputCPATable
    # _employment_by_sector_and_region: Optional[DataFrame] = None
    _raw_region_data: Optional[DataFrame] = None
    _region_load_func: Callable[
        ..., GeoDataFrame
    ] = load_and_join_centre_for_cities_data
    _load_path_or_package_func: Callable[..., DataFrame] = pandas_from_path_or_package

    def __post_init__(self) -> None:
        """Initialise model based on path attributes in preparation for run.

        Todo:
            * Refactor _raw_io_table and raw_io_table components.
        """
        # if self.raw_io_table is not None:
        #     self._raw_io_table: InputOutputTable = self.raw_io_table
        # elif self.io_table_file_path:
        #     if not self.date and not self.employment_date:
        #         raise NotImplemented(f"Date required for input-output-table.")
        #     else:
        #         assert self.date or self.employment_date
        #         self._raw_io_table = self._io_table_cls.load_from_file(
        #             path=self.io_table_file_path,
        #             meta_data=self.io_table_meta_data,
        #             year=self.date.year if self.date else self.employment_date.year,
        #             **self.io_table_kwargs
        #         )
        # else:
        #     raise MissingIOTable(f"Input-Ouput Table needed to run the model.")
        if not self._raw_region_data and self.region_attributes_path:
            self._raw_region_data: GeoDataFrame = self._region_load_func(
                region_path=self.region_attributes_path,
                spatial_path=self.region_spatial_path,
            )
        self._set_national_employment()
        # if not self.national_employment and self.national_employment_path:
        #     self.national_empoyment = self._set_national_employment()
        # if (
        #    not self._employment_by_sector_and_region
        #    and self.region_sector_employment_path
        # ):
        #    self._employment_by_sector_and_region: DataFrame = (
        #        # load_employment_by_region_and_sector_csv(
        #        #     path=self.region_sector_employment_path,
        #        # )
        #        pandas_from_path_or_package(
        #            path=self.region_sector_employment_path,
        #            default_file=ons_IO_2017.CITY_SECTOR_EMPLOYMENT_CSV_FILE_NAME,
        #        )
        #    )
        if not self.date:
            self.date = self.employment_date
            logger.warning(
                f"Set {self} date to employment_date {self.employment_date}."
            )
        super().__post_init__()

    def __repr__(self) -> str:
        return (
            f"Input output model of {self.year}: "
            f"{len(self.sectors)} sectors, {len(self.regions)} regions"
        )

    @cached_property
    def region_data(self) -> GeoDataFrame:
        """Return an indexable collection of attribute data for each region.

        Todo:
            * Consider refactor and/or custom exceptions.
        """
        if self._raw_region_data is not None and isinstance(
            self._raw_region_data, DataFrame
        ):
            return self._raw_region_data.loc[self.region_names]
        elif not self._raw_region_data:
            # raise NullRawRegionError("'_raw_region_data' attribute required for 'region_data' property")
            raise TypeError(
                "'_raw_region_data' attribute cannot be null for 'region_data' property"
            )
        else:
            # raise RawRegionTypeError(f"Raw region type {type(self._raw_region_data)} not implemented, use a GeoDataFrame.")
            raise NotImplementedError(
                f"Raw region type {type(self._raw_region_data)} not implemented, use a GeoDataFrame."
            )

    @property
    def _ij_index(self) -> MultiIndex:
        """Return self.region x self.region MultiIndex."""
        return generate_ij_index(self.regions, self.regions)

    @property
    def _ij_m_index(self) -> MultiIndex:
        """Return self.region x self.region MultiIndex."""
        return generate_ij_m_index(self.regions, self.sectors)

    @property
    def year(self) -> int:
        """Return the year of the date attribute."""
        if self.date:
            if isinstance(self.date, DateType):
                return self.date.year
            elif isinstance(self.date, int):
                return self.date
        else:
            logger.warning(
                f"Date not set, falling back to employment_date: {self.employment_date}."
            )
            if self.employment_date:
                return self.employment_date.year
            else:
                raise ValueError(
                    f"At least `self.date` or `self.employment_date` required"
                )

    def _set_national_employment(self) -> None:
        """Set national_employment Series, aggregated if sector_aggregation set.

        Todo:
            * Refactor around raw_national_employment vs national_employment
            * Potential risk the scaling is applied more times that it should be
        """
        if self.raw_national_employment and self.national_employment:
            logger.warning(
                f"Both raw_national_employment and national_employment set for {self}. national_employment used."
            )
        if self.national_employment is None:
            if self.raw_national_employment is None and self.national_employment_path:
                logger.warning(
                    f"Loading {self.national_employment_path} with scale {self.national_employment_scale}"
                )
                self.raw_national_employment = load_region_employment_excel(
                    path=self.national_employment_path
                )
            if self.raw_national_employment is not None:
                if isinstance(self.raw_national_employment, Series):
                    logger.warning(
                        f"Setting National Employment from raw_national_employment length {len(self.raw_national_employment)}"
                    )
                    self.national_employment = self.raw_national_employment
                elif isinstance(self.raw_national_employment, DataFrame):
                    logger.warning(
                        f"Extracting National Employment from {self} raw_national employment "
                        f"DataFrame of length {len(self.raw_national_employment)} "
                        f"with scaling {self.national_employment_scale}"
                    )
                    self.national_employment = (
                        self.raw_national_employment.loc[str(self.employment_date)]
                        # * self.national_employment_scale  # This was originally applied twice, hopefully fixed now
                    )
        logger.warning(
            f"Setting {self} National Employment with scaling {self.national_employment_scale}"
        )
        assert type(self.national_employment) in (
            Series,
            DataFrame,
        )  # Should be defined or error raised
        # self.national_employment = self.national_employment*self.national_employment_scale
        if self.sector_aggregation:
            # if self.national_employment.columns.to_list() == list(self.sector_aggregation.keys()):
            #     logger.warning(f"sector_aggregation method called for pre-aggregated national employment on {self}")

            # if
            # self._aggregated_national_employment: DataFrame = aggregate_rows(
            #     self.national_employment,
            #     sector_dict=self.sector_aggregation,
            # )
            # self.national_employment = (
            #     self._aggregated_national_employment.loc[str(self.employment_date)]
            #     * self.national_employment_scale
            # )
            logger.warning(
                f"Aggregating national employment by {len(self.sector_aggregation)} groups"
            )
            self.national_employment = aggregate_rows(
                self.national_employment, sector_dict=self.sector_aggregation
            )
        if self.national_employment_scale:
            logger.warning(
                f"Scaling national_employment of {self} by {self.national_employment_scale}"
            )
            self.national_employment = (
                self.national_employment * self.national_employment_scale
            )
        # elif self._national_employment is None:
        #     raise TypeError("'_national_employment' attribute cannot be None.")

    @cached_property
    def io_table(self) -> DataFrame:
        """Return national Input-Ouput table, aggregated by self.sector_aggregation.

        Todo:
            * Check ways of skipping aggregation
            * Fix use of _raw_io_table
            * Check scaling approach
        """
        if not isinstance(self.raw_io_table, InputOutputTable):
            logger.warning(
                f"{self} `raw_io_table` attribute needs conversion from type {type(self.raw_io_table)}. Will try running `self._get_meta_file_or_data_fields()`."
            )
            self._process_raw_io_table()
            # self._get_meta_file_or_data_field('raw_io_table', parser=InputOutputTable)
        assert isinstance(self.raw_io_table, InputOutputTable)
        if self.sector_aggregation:
            return self.raw_io_table.get_aggregated_io_table()  # * self.io_table_scale
        # elif isinstance(self.raw_io_table, InputOutputCPATable):
        #     return self.raw_io_table.code_io_table # * self.io_table_scale
        else:
            return self.raw_io_table.base_io_table  # * self.io_table_scale
        # if isinstance(self.raw_io_table, InputOutputTable):
        #     assert False
        #     if self.sector_aggregation:
        #         return self.raw_io_table.get_aggregated_io_table() * self.io_table_scale
        #     elif isinstance(self.raw_io_table, InputOutputCPATable):
        #         return self.raw_io_table.code_io_table * self.io_table_scale
        #     else:
        #         return self.raw_io_table.base_io_table * self.io_table_scale
        # elif isinstance(self.raw_io_table, DataFrame):
        #     if self.sector_aggregation:
        #         return aggregate_io_table(self.sector_aggregation, self.raw_io_table, [], [])
        #     else:
        #         return self.raw_io_table * self.io_table_scale
        # else:
        #     raise TypeError(f"{self} `raw_io_table` attribute needs conversion from type {type(self.raw_io_table)}. Try running `self._set_all_meta_file_or_data_fields()`.")

        # elif self.raw_io_table.base_io_table is not None:
        #     return self.raw_io_table.base_io_table * self.io_table_scale
        # else:
        #     raise ValueError("No valid io table set for {self}")
        # raise NotImplementedError(
        #     "Currently io_table requires an aggregation dictionary."
        #

    @cached_property
    def base_io_table(self) -> DataFrame:
        return self.io_table.loc[self.sectors, self.sectors]

    @cached_property
    def employment_table(self) -> DataFrame:
        """Return employment table, aggregated if self.sector_aggregation set.

        Todo:
            * Refactor into a separate function
            * Ease passing table without calling filter
        """
        if self.regional_employment is not None:
            return self.regional_employment
        elif self.sector_aggregation:
            self.employment_by_sector_and_region_aggregated = aggregate_rows(
                self.employment_by_sector_and_region, True
            )
            return filter_by_region_name_and_type(
                self.employment_by_sector_and_region_aggregated,
                self.region_names,
                region_type_prefix=self.region_type_prefix,
            )
        else:
            return filter_by_region_name_and_type(
                self.employment_by_sector_and_region,
                self.region_names,
                region_type_prefix=self.region_type_prefix,
            )

    @cached_property
    def X_i_m(self) -> DataFrame:
        """Return the total production of sector $m$ in region $i$ and cache results.

        $X_i^m = X_*^m * Q_i^m/Q_*^m$
        """
        return X_i_m_scaled(
            total_production=self.io_table[self.sectors].loc[self.total_sales_row_name],
            employment=self.employment_table,
            national_employment=self.national_employment,
        ).astype("float64")

    @cached_property
    def M_i_m(self) -> DataFrame:
        """Return the imports of sector $m$ in region $i$ and cache results.

        $M_i^m = M_*^m * Q_i^m/Q_*^m$
        """
        return M_i_m_scaled(
            imports=self.io_table[self.sectors].loc[self.imports_column_name],
            employment=self.employment_table,
            national_employment=self.national_employment,
        ).astype("float64")

    @cached_property
    def F_i_m(self) -> DataFrame:
        """Return the final demand of sector $m$ in region $i$ and cache results.

        $F_i^m = F_*^m * Q_i^m/Q_*^m$
        """
        return F_i_m_scaled(
            final_demand=self.io_table.loc[
                self.sectors, self.final_demand_column_names
            ].sum(axis=1),
            employment=self.employment_table,
            national_employment=self.national_employment,
        ).astype("float64")

    @cached_property
    def E_i_m(self) -> DataFrame:
        """Return the exports of sector $m$ in region $i$ and cache results.

        $E_i^m = E_*^m * Q_i^m/Q_*^m$
        """
        return E_i_m_scaled(
            exports=self.io_table.loc[self.sectors, self.export_column_names].sum(
                axis=1
            ),
            employment=self.employment_table,
            national_employment=self.national_employment,
        ).astype("float64")

    @cached_property
    def distances(self) -> GeoDataFrame:
        """Return a GeoDataFrame of all distances between regions.

        Todo:
            * Replace this with calc_transport_table
        """
        return calc_region_distances(
            self.region_data,
            self.regions,
            self.regions,
            national_column_name=self.national_column_name,
            unit_divide_conversion=self.distance_unit_factor,
        )

    @property
    def S_m_national(self) -> Series:
        return S_m(
            full_io_table=self.io_table,
            subsidy_row_names=self.national_net_subsidies_row_name,
            sector_column_names=self.sector_names,
        )

    @property
    def GVA_m_national(self) -> Series:
        return gross_value_added(
            full_io_table=self.io_table,
            gva_row_names=self.national_gva_row_name,
            sector_column_names=self.sector_names,
        )

    @property
    def I_m_national(self) -> Series:
        return I_m(
            full_io_table=self.io_table,
            investment_column_names=self.national_gov_investment_column_names,
            sector_row_names=self.sector_names,
        )

    @property
    def X_m_national(self) -> Series:
        """Return national $X_m$: aggregate input of $m$ + $G_i$ + $S_i$."""
        return X_m(
            full_io_table=self.io_table,
            gva=self.GVA_m_national,
            net_subsidies=self.S_m_national,
        )

    @cached_property
    def X_i_m(self) -> DataFrame:
        """Return the total production of sector $m$ in region $i$ and cache results.

        $X_i^m = X_*^m * Q_i^m/Q_*^m$

        Todo:
            * At least check the "Total Sale" column specified.
        """
        return X_i_m_scaled(
            total_production=self.io_table[self.sectors].loc["Total Sales"],
            employment=self.employment_table,
            national_employment=self.national_employment,
        ).astype("float64")

    @cached_property
    def x_i_mn_summed(self) -> DataFrame:
        """Return sum of all total demands for good $m$ in region $i$.

        Equation 1:
        $x_i^{mn} = a_i^{mn}X_i^n$
        """
        return x_i_mn_summed(
            X_i_m=self.X_i_m, technical_coefficients=self.technical_coefficients
        )

    @cached_property
    def _y_ij_m(self) -> DataFrame:
        return self.spatial_interaction.y_ij_m

    @cached_property
    def spatial_interaction(self) -> SpatialInteractionBaseClass:
        return self._spatial_model_cls(
            self.distances, self.employment_table, self.national_column_name
        )

    @cached_property
    def _initial_e_m(self) -> DataFrame:
        """Return the initial e_m DataFrame for import_export_convergence."""
        return generate_e_m_dataframe(
            E_i_m=self.E_i_m,
            initial_p=self.P_initial_export_proportion,
            region_names=self.region_names,
            sector_names=self.sector_names,
        )

    @property
    def exogenous_i_m(self) -> Series:
        (
            self._exogenous_i_m,
            self._difference_i_m,
            self._net_constraint,
        ) = self._exogenous_i_m_func(
            self.F_i_m,
            self.E_i_m,
            self.x_i_mn_summed,
            self.X_i_m,
            self.M_i_m,
            self.employment_table,
        )
        return self._net_constraint

    def import_export_convergence(self) -> tuple[DataFrame, DataFrame]:
        """Return the final results of model convergence in two dataframes.

        Todo:
            * Refactor to minimise call configuration.
            * Rename e_i_m_summed to x_i_mn_summed
            * Consider refactoring convergence as class
        """
        self.e_m_model, self.y_ij_m_model = self._import_export_convergence(
            e_m_regions=self._initial_e_m,
            y_ij_m=self._y_ij_m,
            exogenous_i_m=self.exogenous_i_m,
            iterations=self.max_import_export_model_iterations,
        )
        return self.e_m_model, self.y_ij_m_model

    def _load_convergence_results(
        self, e_m_model: DataFrame, y_ij_m_model: DataFrame
    ) -> None:
        """Load pre-existing model results."""
        self.e_m_model = e_m_model
        self.y_ij_m_model = y_ij_m_model
        logger.warning(f"{self} loaded pre-existing e_m_model and y_ij_m_model results")

    @property
    def y_ij_m(self) -> Series:
        try:
            return column_to_series(self.y_ij_m_model, -1)
        except:
            AttributeError(
                f"`y_ij_m_model` not set on {self}, try running ",
                "the `.import_export_convergence` method.",
            )

    @property
    def is_calculated(self) -> bool:
        return hasattr(self, "e_m_model") and hasattr(self, "y_ij_m_model")

    @cached_property
    def regional_io_projections(self) -> dict[str, DataFrame]:
        """Projeting input-output table for specific regions.

        Todo:
            * This function may not be fully tested yet.
        """
        return {
            region: regional_io_projection(
                self.technical_coefficients, self.X_i_m.loc[region]
            )
            for region in self.regions
        }


@dataclass
class InterRegionInputOutputTimeSeries(MutableSequence):

    """Input-Output models over time."""

    io_models: list[InterRegionInputOutput] = field(default_factory=list)
    annual: bool = False
    _io_model_config_index: Optional[
        int
    ] = None  # This assumes they are in chronological order and the last (*most recent one*) is the default for projections into the future
    _input_output_model_cls: Type[InterRegionInputOutput] = InterRegionInputOutput

    def __post_init__(self) -> None:
        if self.dupe_date_counts:
            dupe_dict_formatted = str_keys_of_dict(self.dupe_date_counts)
            logger.warning(f"Duplicate(s) of {dupe_dict_formatted}")

    @property
    def dupe_date_counts(self) -> dict[DateType, int]:
        """Return any duplicate date entries.

        Todo:
            * Consider making hashable to avoid this.
        """
        return collect_dupes(self.dates)

    def __repr__(self) -> str:
        """Summary of model state in a str."""
        summary: str = "Spatial Input-Output model"
        models_count: int = len(self)
        if models_count:
            if models_count > 1:
                summary += "s"
            if self.annual:
                summary = f"{models_count} Annual {summary} from {self.years[0]} to {self.years[-1]}"
            else:
                summary = (
                    f"{models_count} {summary} from {self.dates[0]} to {self.dates[-1]}"
                )
            return f"{summary}: {str(self._core_model).split(': ')[-1]}"
        else:
            return f"Empty {summary} time series"
        # prefix: str = (
        #     f"Input output models from {self[0].date} to {self[-1].date}: "
        #     if len(self)
        #     else f"Empty Input-Output time series for "
        # )
        # return (
        #     prefix
        #     + f"{len(list(self.sectors))} sectors, {len(list(self.regions))} regions"
        # )

    @property
    def _core_model(self) -> Optional[InterRegionInputOutput]:
        """Return the specified or last as summary InterRegionInputOutput model.

        Todo:
            * Determine a better way of summarising
            * Potential use of a config *class* for managing/scenario runs etc.
        """
        if self._io_model_config_index:
            return self[self._io_model_config_index]
        elif len(self) > 0:
            return self[-1]
        else:
            return None

    @property
    def sectors(self) -> SectorConfigType:
        """Return sectors from _core_model."""
        return self._core_model.sectors if self._core_model else []

    @property
    def regions(self) -> RegionConfigType:
        """Return sectors from _core_model."""
        return self._core_model.regions if self._core_model else []

    @property
    def sector_names(self) -> SectorNamesListType:
        if isinstance(self.sectors, dict):
            return list(self.sectors.keys())
        else:
            return self.sectors

    @property
    def is_calculated(self) -> bool:
        return all(model.is_calculated for model in self)

    # @property
    # def sectors(self) -> list[str]:
    #     if not len(self):
    #         logger.warning(f"No InterRegionInputOutput time points.")
    #         return []
    #     elif self._io_model_config_index:
    #         return self[self._io_model_config_index].sectors
    #     else:
    #         logger.warning(
    #             f"_io_model_config_index of {self} not set, may need refactor."
    #         )
    #         return []

    @property
    def region_names(self) -> RegionNamesListType:
        """Return the region names."""
        if isinstance(self.regions, dict):
            return list(self.regions.keys())
        else:
            return self.regions

    @overload
    def __getitem__(self, index: int) -> InterRegionInputOutput:
        ...

    @overload
    def __getitem__(self, index: slice) -> list[InterRegionInputOutput]:
        ...

    def __getitem__(self, index):
        return self.io_models[index] if self.io_models else None

    @overload
    def __setitem__(self, i: int, item: InterRegionInputOutput) -> None:
        ...

    @overload
    def __setitem__(self, s: slice, items: Iterable[InterRegionInputOutput]) -> None:
        ...

    def __setitem__(self, index, item):
        self.io_models[index] = item

    @overload
    def __delitem__(self, i: int) -> None:
        ...

    @overload
    def __delitem__(self, s: slice) -> None:
        ...

    def __delitem__(self, index):
        del self.io_models[index]

    def __len__(self) -> int:
        return len(self.io_models)

    def __iter__(self) -> Iterator[InterRegionInputOutput]:
        return iter(self.io_models)

    def insert(self, i: int, item: InterRegionInputOutput) -> None:
        self.io_models.insert(i, item)

    @property
    def years(self) -> list[int]:
        logger.warning(
            "Potential inefficient indexing years method, consider generator"
        )
        # return [io_model.year for io_model in self]
        return [date.year for date in self.dates]

    @property
    def dates(self) -> list[DateType]:
        return [model.date for model in self if model.date]

    def calc_models(self) -> None:
        for model in self:
            model.import_export_convergence()

    def _return_iter_attr(
        self,
        attr_name: str,
    ) -> OrderedDict[DateType, DataFrame]:
        """Wrappy to manage retuing Generator dict attributes over time series."""
        return tuples_to_ordered_dict(iter_attr_by_key(self, attr_name))

    @property
    def national_employment_ts(self) -> OrderedDict[DateType, DataFrame]:
        return self._return_iter_attr("national_employment")

    @property
    def regional_employment_ts(self) -> OrderedDict[DateType, DataFrame]:
        return self._return_iter_attr("regional_employment")
