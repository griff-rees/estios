#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import OrderedDict
from collections.abc import MutableSequence, Sequence
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
    E_i_m_scaled_by_regions,
    F_i_m_scaled_by_regions,
    I_m,
    M_i_m_scaled_by_regions,
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
from .input_output_tables import (
    FINAL_DEMAND_COLUMN_NAMES,
    IMPORTS_ROW_NAME,
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
from .uk.regions import load_and_join_centre_for_cities_data
from .utils import (
    SECTOR_10_CODE_DICT,
    AggregatedSectorDictType,
    DateType,
    RegionConfigType,
    RegionNamesListType,
    RegionsIterableType,
    SectorConfigType,
    SectorNamesListType,
    aggregate_rows,
    collect_dupes,
    column_to_series,
    conditional_type_wrapper,
    df_set_columns,
    filter_attrs_by_substring,
    filter_by_region_name_and_type,
    generate_i_m_index,
    generate_ij_index,
    generate_ij_m_index,
    get_df_first_row,
    iter_attr_by_key,
    len_less_or_eq,
    regions_type_to_list,
    str_keys_of_dict,
    sum_if_multi_column_df,
    tuples_to_ordered_dict,
)

logger = getLogger(__name__)

filterwarnings("ignore", category=ShapelyDeprecationWarning)

ColumnOrRowNames = str | Sequence[str]

NamesListType = list[str] | Collection[str]


@dataclass(kw_only=True)
class InterRegionInputOutputBaseClass(ModelDataSourcesHandler):

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
    raw_regions: dict[str, str] = field(default_factory=dict)
    regions: RegionsIterableType = field(default_factory=Series)

    # Column, index lables etc. for formatting
    nation_name: str | None = None
    national_column_name: str = ""  # To be replaced in future
    total_sales_row_name: str = TOTAL_SALES_ROW_NAME
    final_demand_column_names: list[str] = field(
        default_factory=lambda: FINAL_DEMAND_COLUMN_NAMES
    )
    import_row_names: list[str] = field(default_factory=list)
    total_production_index_name: str = TOTAL_PRODUCTION_ROW_NAME

    # Sector management attributes (needs refactoring)
    raw_sectors: dict[str, str] = field(default_factory=dict)
    sector_aggregation: Optional[AggregatedSectorDictType] = field(
        default_factory=lambda: deepcopy(SECTOR_10_CODE_DICT)
    )
    P_initial_export_proportion: float = INITIAL_P

    date: Optional[DateType] = None
    distance_unit_factor: float = DISTANCE_UNIT_DIVIDE
    export_column_names: list[str] = field(
        # default_factory=lambda: UK_EXPORT_COLUMN_NAMES
        default_factory=list
    )
    imports_column_name: str = IMPORTS_ROW_NAME
    national_distance: float | None = None
    national_employment: Series | None = None
    national_employment_scale: float = 1.0
    io_table_scale: float = 1.0
    national_population: float | None = None
    national_working_population: float | None = None
    national_gva_row_name: ColumnOrRowNames | None = None
    national_net_subsidies_row_name: ColumnOrRowNames | None = None
    national_gov_investment_column_names: ColumnOrRowNames | None = None
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
                f"{type(self.raw_io_table)}. Will try instantiating a {self._io_table_cls}."
            )
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

    def __repr__(self) -> str:
        """Return a str indicated class type and number of sectors.

        Todo:
            * Apply __repr__ format coherence across classes
        """
        repr: str = f"{self.__class__.__name__}("
        repr += f"nation='{self.national_column_name}', "
        repr += f"date='{self.date}', "
        repr += f"sectors={self.sectors_count}, "
        repr += f"regions={self.regions_count})"
        return repr

    def __str__(self) -> str:
        return (
            f"{self.national_column_name} {self.date} Input-Output model: "
            f"{len(self.sectors)} sectors, {len(self.regions)} regions"
        )

    @property
    def sectors_count(self) -> int:
        """Return the number of sectors."""
        return len(self.sectors)

    @property
    def regions_count(self) -> int:
        """Return the number of sectors."""
        return len(self.regions)

    def __post_init__(self):
        self._set_all_meta_file_or_data_fields()

    @property
    def region_names(self) -> list[str]:
        """Return the region names."""
        return regions_type_to_list(self.regions)

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


class MissingIOTable(Exception):
    pass


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

    national_employment_path: Optional[
        PathLike
    ] = None  # UK_JOBS_BY_SECTOR_XLS_FILE_NAME
    employment_date: Optional[DateType] = None  # EMPLOYMENT_QUARTER_DEC_2017
    io_table_kwargs: dict[str, Any] = field(default_factory=dict)
    region_attributes_path: PathLike | None = None  # CENTRE_FOR_CITIES_CSV_FILE_NAME
    region_spatial_path: PathLike | None = None  # CITIES_TOWNS_GEOJSON_FILE_NAME
    region_type_prefix: str | None = None  # CITY_SECTOR_REGION_PREFIX
    national_column_name: str = ""  # To be replaced in future

    _io_table_cls: Type[InputOutputTable] = InputOutputCPATable
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
        if not self._raw_region_data and self.region_attributes_path:
            self._raw_region_data: GeoDataFrame = self._region_load_func(
                region_path=self.region_attributes_path,
                spatial_path=self.region_spatial_path,
            )
        if not self.date:
            self.date = self.employment_date
            logger.warning(
                f"Set {self} date to employment_date {self.employment_date}."
            )
        super().__post_init__()

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
        return generate_ij_m_index(
            self.regions, self.sectors, self.national_column_name
        )

    @property
    def _i_m_index(self) -> MultiIndex:
        """Return self.region_names x self.sector_names MultiIndex."""
        return generate_i_m_index(
            self.region_names, self.sector_names, self.national_column_name
        )

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

    @property
    def technical_coefficients(self) -> DataFrame:
        """Return the technical coefficients derived from `self.io_table`.

        Todo:
            * Refactor to avoid `self.raw_io_table` vs `self._raw_io_table` ambiguity.
        """
        return technical_coefficients(
            self.io_table, self.final_demand_column_names, self.sectors
        )

    @property
    def io_table(self) -> DataFrame:
        """Return national Input-Ouput table, aggregated by self.sector_aggregation.

        Todo:
            * Check ways of skipping aggregation
            * Fix use of _raw_io_table
            * Check scaling approach
        """
        if not isinstance(self.raw_io_table, InputOutputTable):
            logger.warning(
                f"{self} `raw_io_table` attribute needs conversion from type "
                f"{type(self.raw_io_table)} to {type(self._io_table_cls)}. "
                f"Will try running `self._get_meta_file_or_data_fields()`."
            )
            self._process_raw_io_table()
        assert isinstance(self.raw_io_table, InputOutputTable)
        if self.sector_aggregation:
            return self.raw_io_table.get_aggregated_io_table()  # * self.io_table_scale
        else:
            return self.raw_io_table.base_io_table  # * self.io_table_scale

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
            assert self.region_type_prefix
            return filter_by_region_name_and_type(
                self.employment_by_sector_and_region_aggregated,
                self.region_names,
                region_type_prefix=self.region_type_prefix,
            )
        else:
            assert self.region_type_prefix
            return filter_by_region_name_and_type(
                self.employment_by_sector_and_region,
                self.region_names,
                region_type_prefix=self.region_type_prefix,
            )

    @property
    @conditional_type_wrapper(len_less_or_eq, get_df_first_row)
    def national_imports(self) -> DataFrame | Series:
        """Return national final demand columns.

        Todo:
            * Add a decorator to apply Series to other cases
        """
        return self.io_table.loc[self.import_row_names, self.sector_names]

    @property
    def M_i_m_national(self) -> DataFrame | Series:
        """Return national final demand columns.

        Todo:
            * Add a decorator to apply Series to other cases
        """
        return sum_if_multi_column_df(self.national_imports)

    @property
    def M_i_m(self) -> DataFrame:
        """Return the imports of sector $m$ in region $i$ and cache results.

        $M_i^{(m)} = M_*^{(m)} * P_i/P_*$
        """
        return M_i_m_scaled_by_regions(
            imports=self.national_imports,
            regional_populations=self.regional_populations,
            national_population=self.national_population,
            sector_row_names=self.sector_names,
        )

    @property
    def national_final_demand(self) -> DataFrame:
        """Return national final demand columns."""
        return self.io_table.loc[self.sector_names, self.final_demand_column_names]

    @property
    def F_i_m_national(self) -> Series:
        """Aggregate self.national_final_demand."""
        return sum_if_multi_column_df(self.national_final_demand)

    @property
    def F_i_m_full(self) -> DataFrame:
        """Return the final demand of sector $m$ in region $i$ in all categories

        $F_i^{(m)} = F_*^{(m)} * P_i/P_*$
        """
        return F_i_m_scaled_by_regions(
            final_demand=self.national_final_demand,
            regional_populations=self.regional_populations,
            national_population=self.national_population,
            sector_row_names=self.sector_names,
        )

    @property
    def national_exports(self) -> DataFrame:
        """Return national final demand columns."""
        return self.io_table.loc[self.sector_names, self.export_column_names]

    @property
    def F_i_m(self) -> DataFrame:
        """Return the final demand of sector $m$ in region $i$, summing all columns if needed.

        $F_i^{(m)} = F_*^{(m)} * P_i/P_*$
        """
        return df_set_columns(
            sum_if_multi_column_df(self.F_i_m_full).unstack(), self.sector_names
        )

    @property
    def E_i_m_full(self) -> DataFrame:
        """Return all export types of sector $m$ in region $i$ and cache results.

        $E_i^{(m)} = E_*^{(m)} * Q_i^{(m)}/Q_*^{(m)}$
        """
        return E_i_m_scaled_by_regions(
            exports=self.national_exports,
            regional_employment=self.regional_employment,
            national_employment=self.national_employment,
            sector_row_names=self.sector_names,
        )

    @property
    def E_i_m(self) -> DataFrame:
        """Return the exports of sector $m$ in region $i$ and cache results.

        $E_i^{(m)} = E_*^{(m)} * Q_i^{(m)}/Q_*^{(m)}$
        """
        return df_set_columns(
            sum_if_multi_column_df(self.E_i_m_full).unstack(), self.sector_names
        )

    @property
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

    @property
    def X_i_m(self) -> DataFrame:
        """Return the total production of sector $m$ in region $i$ and cache results.

        $X_i^{(m)} = X_*^{(m)} * Q_i^{(m)}/Q_*^{(m)}$

        Todo:
            * At least check the "Total Sale" column specified.

        """
        return X_i_m_scaled(
            total_production=self.X_m_national
            + self.GVA_m_national
            + self.S_m_national,
            employment=self.employment_table,
            national_employment=self.national_employment,
        )

    @property
    def x_i_mn_summed(self) -> DataFrame:
        """Return sum of all total demands for good $m$ in region $i$.

        Equation 1:
        $x_i^{mn} = a_i^{mn}X_i^n$
        """
        return x_i_mn_summed(
            X_i_m=self.X_i_m, technical_coefficients=self.technical_coefficients
        )

    @property
    def _y_ij_m(self) -> DataFrame:
        return self.spatial_interaction.y_ij_m

    @property
    def spatial_interaction(self) -> SpatialInteractionBaseClass:
        return self._spatial_model_cls(
            self.distances, self.employment_table, self.national_column_name
        )

    @property
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

    @property
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

    @property
    def regional_total_population(self) -> float:
        """Return the sum of all included regional populations.

        Note:
            `self.regional_total_population <= self.national_population`
        """
        assert self.regional_populations is not None
        return self.regional_populations.sum()

    @property
    def residual_population(self) -> float:
        """Difference between `national_population` and `regional_total_population`.

        Note:
            This *should* be <= 0
        """
        assert self.national_population and self.regional_total_population
        return self.national_population - self.regional_total_population

    @property
    def residual_X_m(self) -> Series:
        """Return the diference between `X_m_national` and sum of `X_i_m`."""
        return self.X_m_national - self.X_i_m.sum(axis="rows")

    # @property
    # def national_sales(self) -> Series:
    #     return


# @dataclass(repr=False, kw_only=True)
# class InterRegionInputOutputNationalResidual(InterRegionInputOutput):
#
#     """Extend the InterRegionInputOutput model with national residuals."""


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
        """Return a str indicated class type and number of sectors.

        Todo:
            * Apply __repr__ format coherence across classes
        """
        repr: str = f"{self.__class__.__name__}("
        repr += f"dates={len(self)}, "
        if self.annual:
            repr += f"start={self.years[0]}, end={self.years[-1]}, "
        else:
            repr += f"start='{self.dates[0]}', end='{self.dates[-1]}', "
            # repr += f"start={self.dates[0]}, end={self.dates[-1]}, "
        repr += f"sectors={self.sectors_count}, "
        repr += f"regions={self.regions_count})"
        return repr

    def __str__(self) -> str:
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
            return (
                f"{summary}: {self.sectors_count} sectors, {self.regions_count} regions"
            )
        else:
            return f"Empty {summary} time series"

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
    def sectors_count(self) -> int:
        return len(self.sectors)

    @property
    def regions_count(self) -> int:
        return len(self.regions)

    @property
    def is_calculated(self) -> bool:
        return all(model.is_calculated for model in self)

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
