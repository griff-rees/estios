#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
    Iterable,
    Iterator,
    Optional,
    Type,
    Union,
    get_type_hints,
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
    E_i_m,
    F_i_m,
    M_i_m,
    X_i_m,
    calc_region_distances,
    generate_e_m_dataframe,
    import_export_convergence,
    region_and_sector_convergence,
    regional_io_projection,
    technical_coefficients,
    x_i_mn_summed,
)
from .input_output_tables import (
    FINAL_DEMAND_COLUMN_NAMES,
    IMPORTS_COLUMN_NAME,
    IO_TABLE_SCALING,
    TOTAL_PRODUCTION_COLUMN_NAME,
    UK_EXPORT_COLUMN_NAMES,
    InputOutputCPATable,
    InputOutputTable,
    load_employment_by_region_and_sector_csv,
    load_region_employment_excel,
)
from .spatial_interaction import AttractionConstrained, SpatialInteractionBaseClass
from .uk import ons_IO_2017
from .uk.employment import (
    EMPLOYMENT_QUARTER_DEC_2017,
    UK_JOBS_BY_SECTOR_SCALING,
    UK_JOBS_BY_SECTOR_XLS_FILE_NAME,
)
from .uk.ons_population_projections import ONS_PROJECTION_YEARS
from .uk.regions import (
    CENTRE_FOR_CITIES_CSV_FILE_NAME,
    CITIES_TOWNS_GEOJSON_FILE_NAME,
    UK_CITY_REGIONS,
    load_and_join_centre_for_cities_data,
)
from .utils import (
    DEFAULT_ANNUAL_MONTH_DAY,
    SECTOR_10_CODE_DICT,
    AggregatedSectorDictType,
    AnnualConfigType,
    DateConfigType,
    MonthDay,
    aggregate_rows,
    column_to_series,
    filter_by_region_name_and_type,
    generate_ij_index,
    generate_ij_m_index,
)

logger = getLogger(__name__)

filterwarnings("ignore", category=ShapelyDeprecationWarning)

DEFAULT_TIME_SERIES_CONFIG: DateConfigType = {
    EMPLOYMENT_QUARTER_DEC_2017: {
        "io_table_file_path": ons_IO_2017.EXCEL_FILE_NAME,
    }
}


@dataclass
class InterRegionInputOutputBaseClass:

    """Bass attributes for InputOutput Model and TimeSeries."""

    max_import_export_model_iterations: int = DEFAULT_IMPORT_EXPORT_ITERATIONS
    regions: dict[str, str] = field(default_factory=lambda: deepcopy(UK_CITY_REGIONS))

    sector_aggregation: AggregatedSectorDictType = field(
        default_factory=lambda: deepcopy(SECTOR_10_CODE_DICT)
    )
    P_initial_export_proportion: float = INITIAL_P

    date: Optional[date] = None
    distance_unit_factor: float = DISTANCE_UNIT_DIVIDE
    final_demand_column_names: list[str] = field(
        default_factory=lambda: FINAL_DEMAND_COLUMN_NAMES
    )
    export_column_names: list[str] = field(
        default_factory=lambda: UK_EXPORT_COLUMN_NAMES
    )
    imports_column_name: str = IMPORTS_COLUMN_NAME
    total_production_column_name: str = TOTAL_PRODUCTION_COLUMN_NAME
    national_employment_scale: float = UK_JOBS_BY_SECTOR_SCALING
    io_table_scale: float = IO_TABLE_SCALING
    _spatial_model_cls: Type[SpatialInteractionBaseClass] = AttractionConstrained
    _exogenous_i_m_func: Callable[..., Series] = region_and_sector_convergence
    _import_export_convergence: Callable[..., DataFrame] = import_export_convergence
    _region_load_func: Callable[
        ..., GeoDataFrame
    ] = load_and_join_centre_for_cities_data

    @property
    def region_names(self) -> list[str]:
        """Return the region names."""
        return list(self.regions.keys())

    @property
    def sectors(self) -> list[str]:
        return [*self.sector_aggregation]


@dataclass
class InterRegionInputOutput(InterRegionInputOutputBaseClass):

    """Manage Inter Region input output model runs.

    Todo:
        * Abstract path for employment data to ease setting directly.
    """

    io_table_file_path: PathLike = ons_IO_2017.EXCEL_FILE_NAME
    region_sector_employment_path: Optional[
        PathLike
    ] = ons_IO_2017.CITY_SECTOR_EMPLOYMENT_CSV_FILE_NAME
    national_employment_path: Optional[PathLike] = UK_JOBS_BY_SECTOR_XLS_FILE_NAME
    employment_date: date = EMPLOYMENT_QUARTER_DEC_2017
    io_table_kwargs: dict[str, Any] = field(default_factory=dict)
    region_attributes_path: PathLike = CENTRE_FOR_CITIES_CSV_FILE_NAME
    region_spatial_path: PathLike = CITIES_TOWNS_GEOJSON_FILE_NAME
    _io_table_cls: Type[InputOutputTable] = InputOutputCPATable
    _national_employment: Optional[DataFrame] = None
    _employment_by_sector_and_region: Optional[DataFrame] = None
    _raw_region_data: Optional[DataFrame] = None

    def __post_init__(self) -> None:
        """Initialise model based on path attributes in preparation for run."""
        self._raw_io_table: InputOutputTable = self._io_table_cls(
            path=self.io_table_file_path, **self.io_table_kwargs
        )
        if not self._raw_region_data and self.region_attributes_path:
            self._raw_region_data: GeoDataFrame = self._region_load_func(
                region_path=self.region_attributes_path,
                spatial_path=self.region_spatial_path,
            )
        if not self._national_employment and self.national_employment_path:
            self._national_employment: DataFrame = load_region_employment_excel(
                path=self.national_employment_path
            )
        if (
            not self._employment_by_sector_and_region
            and self.region_sector_employment_path
        ):
            self._employment_by_sector_and_region: DataFrame = (
                load_employment_by_region_and_sector_csv(
                    path=self.region_sector_employment_path
                )
            )
        if not self.date:
            self.date = self.employment_date
            logger.warning(
                f"Set {self} date to employment_date {self.employment_date}."
            )

    def __repr__(self) -> str:
        return (
            f"Input output model of {self.year}: "
            f"{len(self.sectors)} sectors, {len(self.regions)} cities"
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
            return self.date.year
        else:
            logger.warning(
                f"Date not set, falling back to employment_date: {self.employment_date}."
            )
            return self.employment_date.year

    @cached_property
    def national_employment(self) -> DataFrame:
        """Return DataFrame, aggregated if sector_aggregation set."""
        if self.sector_aggregation:
            self._aggregated_national_employment: DataFrame = aggregate_rows(
                self._national_employment
            )
            return (
                self._aggregated_national_employment.loc[str(self.employment_date)]
                * self.national_employment_scale
            )
        elif self._national_employment is None:
            raise TypeError("'_national_employment' attribute cannot be None.")
        elif isinstance(self._national_employment, DataFrame):
            return (
                self._national_employment.loc[str(self.employment_date)]
                * self.national_employment_scale
            )

    @cached_property
    def io_table(self) -> DataFrame:
        """Return national Input-Ouput table, aggregated by self.sector_aggregation.

        Todo:
            * Check ways of skipping aggregation
        """
        if self.sector_aggregation:
            return self._raw_io_table.get_aggregated_io_table() * self.io_table_scale
        elif isinstance(self._raw_io_table, InputOutputCPATable):
            return self._raw_io_table.code_io_table * self.io_table_scale
        elif self._raw_io_table.base_io_table is not None:
            return self._raw_io_table.base_io_table * self.io_table_scale
        else:
            raise ValueError("No valid io table set for {self}")
            # raise NotImplementedError(
            #     "Currently io_table requires an aggregation dictionary."
            # )

    @cached_property
    def base_io_table(self) -> DataFrame:
        return self.io_table[self.sectors].loc[self.sectors]

    @cached_property
    def technical_coefficients(self) -> DataFrame:
        """Return the technical coefficients derived from self.io_table."""
        return technical_coefficients(self.io_table, self.sectors)

    @cached_property
    def employment_table(self) -> DataFrame:
        """Return employment table, aggregated if self.sector_aggregation set."""
        if self.sector_aggregation:
            self._employment_by_sector_and_region_aggregated = aggregate_rows(
                self._employment_by_sector_and_region, True
            )
            return filter_by_region_name_and_type(
                self._employment_by_sector_and_region_aggregated, self.region_names
            )
        else:
            return filter_by_region_name_and_type(
                self._employment_by_sector_and_region, self.region_names
            )

    @cached_property
    def X_i_m(self) -> DataFrame:
        """Return the total production of sector 𝑚 in region 𝑖and cache results.

        $X_i^m = X_*^m * Q_i^m/Q_*^m$
        """
        return X_i_m(
            total_sales=self.io_table[self.sectors].loc["Total Sales"],
            employment=self.employment_table,
            national_employment=self.national_employment,
        )

    @cached_property
    def M_i_m(self) -> DataFrame:
        """Return the imports of sector 𝑚 in region 𝑖and cache results.

        $M_i^m = M_*^m * Q_i^m/Q_*^m$
        """
        return M_i_m(
            imports=self.io_table[self.sectors].loc["Imports"],
            employment=self.employment_table,
            national_employment=self.national_employment,
        )

    @cached_property
    def F_i_m(self) -> DataFrame:
        """Return the final demand of sector 𝑚 in region 𝑖and cache results.

        $F_i^m = F_*^m * Q_i^m/Q_*^m$
        """
        return F_i_m(
            final_demand=self.io_table.loc[
                self.sectors, self.final_demand_column_names
            ].sum(axis=1),
            employment=self.employment_table,
            national_employment=self.national_employment,
        )

    @cached_property
    def E_i_m(self) -> DataFrame:
        """Return the exports of sector 𝑚 in region 𝑖and cache results.

        $E_i^m = E_*^m * Q_i^m/Q_*^m$
        """
        return E_i_m(
            exports=self.io_table.loc[self.sectors, self.export_column_names].sum(
                axis=1
            ),
            employment=self.employment_table,
            national_employment=self.national_employment,
        )

    @cached_property
    def distances(self) -> GeoDataFrame:
        """Return a GeoDataFrame of all distances between cities."""
        return calc_region_distances(
            self.region_data,
            self.regions,
            self.regions,
            unit_divide_conversion=self.distance_unit_factor,
        )

    @cached_property
    def x_i_mn_summed(self) -> DataFrame:
        """Return sum of all total demands for good 𝑚 in region 𝑖.

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
        return self._spatial_model_cls(self.distances, self.employment_table)

    @cached_property
    def _initial_e_m(self) -> DataFrame:
        """Return the initial e_m DataFrame for import_export_convergence."""
        return generate_e_m_dataframe(
            E_i_m=self.E_i_m,
            initial_p=self.P_initial_export_proportion,
            region_names=self.region_names,
            sector_names=self.sectors,
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
            e_m_cities=self._initial_e_m,
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

    @cached_property
    def regional_io_projections(self) -> dict[str, DataFrame]:
        return {
            region: regional_io_projection(
                self.technical_coefficients, self.X_i_m.loc[region]
            )
            for region in self.regions
        }

    @property
    def y_ij_m(self) -> Series:
        try:
            return column_to_series(self.y_ij_m_model, -1)
        except:
            AttributeError(
                f"`y_ij_m_model` not set on {self}, try running ",
                "the `.import_export_convergence` method.",
            )


@dataclass
class InterRegionInputOutputTimeSeries(
    MutableSequence, InterRegionInputOutputBaseClass
):

    """Input-Output models over time."""

    io_models: list[InterRegionInputOutput] = field(default_factory=list)
    _common_io_model_kwargs: dict = field(default_factory=dict)
    _input_output_model_cls: Type[InterRegionInputOutput] = InterRegionInputOutput
    # _enforce_same_input_output_model: bool = True
    # _populate_common_config_from_nth_model: Optional[int] = -1
    # _copy_config_from_nth_io_model: int = 0
    # _global_config_key_name: str = "__all_io_models__"
    # _temporal_metrics: dict[str: Callable[[InterRegionInputOutput], DataFrame]] = field(default_factory=dict)
    _io_model_config_index: Optional[int] = -1

    def __post_init__(self) -> None:
        """Apply configuration options, first default_io_model then _common_io_model_kwargs."""
        if self._io_model_config_index != None:
            self._apply_io_model_config()
        self._apply_common_io_model_kwargs()

    @property
    def _default_io_model_config(self) -> Optional[InterRegionInputOutput]:
        return (
            self[self._io_model_config_index] if self._io_model_config_index else None
        )

    def _apply_io_model_config(self) -> None:
        if self._default_io_model_config:
            logger.info(
                f"Setting attrs for {self} from {self._default_io_model_config} "
                f"(index = {self._io_model_config_index})"
            )
            self._input_output_model_cls = type(self._default_io_model_config)
            for var in get_type_hints(self.__class__):
                # Note, get_type_hints(self) will only include local instance hints, not inherited
                if var in get_type_hints(self._input_output_model_cls):
                    value: Any = getattr(self._default_io_model_config, var)
                    logger.debug(
                        f"Setting {var} of {self} from {getattr(self, var)} to {value}"
                    )
                    setattr(self, var, value)
        else:
            logger.info(f"No self._default_io_model_config to apply.")

    def _apply_common_io_model_kwargs(self):
        """Apply all self._io_model_kwargs to self.io_models."""
        logger.info(f"Applying ._common_io_model_kwargs to all {len(self)} io_models.")
        for key, value in self._common_io_model_kwargs:
            for io_model in self:
                if hasattr(io_model, key):
                    logger.debug(f"Setting {io_model} {key} to {value}.")
                    setattr(io_model, key, value)

    @classmethod
    def from_years(
        cls,
        years: AnnualConfigType = ONS_PROJECTION_YEARS,
        default_month_day: MonthDay = DEFAULT_ANNUAL_MONTH_DAY,
        **kwargs,
    ) -> "InterRegionInputOutputTimeSeries":
        """Generate an InterRegionInputOutputTimeSeries from a list of dates."""
        logger.info(
            f"Generating an InputOutputTimeSeries using {default_month_day} for each year."
        )
        date_config: DateConfigType
        if isinstance(years, dict):
            date_config = {
                default_month_day.from_year(year): config_dict
                for year, config_dict in years.items()
            }
        else:
            date_config = [default_month_day.from_year(year) for year in years]
        return cls.from_dates(date_config, **kwargs)

    @classmethod
    def from_dates(
        cls,
        dates: DateConfigType = DEFAULT_TIME_SERIES_CONFIG,
        input_output_model_cls: Type[InterRegionInputOutput] = InterRegionInputOutput,
        **kwargs,
    ) -> "InterRegionInputOutputTimeSeries":
        """Generate an InterRegionInputOutputTimeSeries from a list of dates."""
        logger.info(
            "Generating an InputOutputTimeSeries with dates and passed general config."
        )
        io_models: list[InterRegionInputOutput] = []
        if type(dates) is dict:
            logger.debug(f"Iterating over {len(dates)} with dict configs")
            for date, config_dict in dates.items():
                io_model: InterRegionInputOutput = input_output_model_cls(
                    date=date, **(config_dict | kwargs)
                )
                io_models.append(io_model)
                logger.debug(f"Added {io_model} to list for generating time series.")
            return cls(
                io_models=io_models, _input_output_model_cls=input_output_model_cls
            )
        else:
            io_models = [input_output_model_cls(date=date, **kwargs) for date in dates]
            return cls(
                io_models=io_models,
                _input_output_model_cls=input_output_model_cls,
                **kwargs,
            )

    def __repr__(self) -> str:
        prefix: str = (
            f"Input output models from {self[0].date} to {self[-1].date}: "
            if len(self)
            else f"Empty Input-Output time series for "
        )
        return (
            prefix
            + f"{len(list(self.sectors))} sectors, {len(list(self.regions))} cities"
        )

    @overload
    def __getitem__(self, i: int) -> InterRegionInputOutput:
        ...

    @overload
    def __getitem__(self, s: slice) -> list[InterRegionInputOutput]:
        ...

    def __getitem__(
        self, index
    ) -> Union[InterRegionInputOutput, list[InterRegionInputOutput]]:
        return self.io_models[index]

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
    def years(self) -> Iterable[int]:
        logger.warning(
            "Potential inefficient indexing years method, consider generator"
        )
        # return [io_model.year for io_model in self]
        return [date.year for date in self.dates]

    @property
    def sectors(self) -> list[str]:
        if self._io_model_config_index:
            return self[self._io_model_config_index].sectors
        else:
            logger.warning(
                f"_io_model_config_index of {self} not set, may need refactor."
            )
            return []

    @property
    def dates(self) -> list[date]:
        return [model.date for model in self if model.date]

    def calc_models(self) -> None:
        for model in self:
            model.import_export_convergence()

    @property
    def national_employment_ts(self) -> DataFrame:
        return DataFrame({model.date: model.national_employment for model in self})
