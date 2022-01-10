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

from geopandas import GeoDataFrame
from numpy import exp
from pandas import DataFrame, MultiIndex, Series

from .input_output_func import (
    CITY_POPULATION_COLUMN_NAME,
    DEFAULT_IMPORT_EXPORT_ITERATIONS,
    DISTANCE_COLUMN,
    DISTANCE_UNIT_DIVIDE,
    INITIAL_P,
    E_i_m,
    F_i_m,
    M_i_m,
    X_i_m,
    andrews_suggestion,
    calc_city_distance,
    generate_e_m_dataframe,
    import_export_convergence,
    technical_coefficients,
    x_i_mn_summed,
)
from .uk_data.utils import (
    CENTRE_FOR_CITIES_PATH,
    CITIES_TOWNS_SHAPE_PATH,
    CITY_REGIONS,
    CITY_SECTOR_EMPLOYMENT_PATH,
    CITY_SECTOR_YEARS,
    EMPLOYMENT_QUARTER_DEC_2017,
    IO_TABLE_2017_EXCEL_PATH,
    IO_TABLE_EXPORT_COLUMN_NAMES,
    IO_TABLE_FINAL_DEMAND_COLUMN_NAMES,
    IO_TABLE_IMPORTS_COLUMN_NAME,
    IO_TABLE_SCALING,
    IO_TABLE_TOTAL_PRODUCTION_COLUMN_NAME,
    JOBS_BY_SECTOR_PATH,
    JOBS_BY_SECTOR_SCALING,
    NATIONAL_COLUMN_NAME,
    SECTOR_10_CODE_DICT,
    TOTAL_OUTPUT_COLUMN,
    AggregatedSectorDictType,
    ONSInputOutputTable,
    aggregate_rows,
    filter_by_region_name_and_type,
    load_and_join_centre_for_cities_data,
    load_employment_by_city_and_sector,
    load_region_employment,
)
from .utils import generate_ij_index, generate_ij_m_index

logger = getLogger(__name__)

DEFAULT_TIME_SERIES_CONFIG = {
    EMPLOYMENT_QUARTER_DEC_2017: {
        "io_table_file_path": IO_TABLE_2017_EXCEL_PATH,
    }
}


@dataclass
class SpatialInteractionBaseClass:

    # beta: float
    distances: GeoDataFrame
    employment: DataFrame
    employment_column_name: str = CITY_POPULATION_COLUMN_NAME
    distance_column_name: str = DISTANCE_COLUMN
    national_term: bool = True
    national_column_name: str = NATIONAL_COLUMN_NAME

    _gen_ij_m_func: Callable[..., MultiIndex] = generate_ij_m_index

    @property
    def y_ij_m(self) -> DataFrame:
        """Placeholder for initial conditions for model y_ij_m DataFrame."""
        raise NotImplementedError("This is not implemented in the BaseClass")

    @property
    def ij_m_index(self) -> MultiIndex:
        """Return city x other city x sector MultiIndex."""
        return self._gen_ij_m_func(self.employment.index, self.employment.columns)

    def _func_by_index(self, func):
        return [
            func(city, other_city, sector)
            for city, other_city, sector in self.ij_m_index
        ]

    def _Q_i_m_func(self, city, other_city, sector) -> float:
        return self.employment.loc[city][sector]

    def _distance_func(self, city, other_city, sector) -> float:
        return self.distances[self.distance_column_name][city][other_city]

    @property
    def Q_i_m_list(self) -> list[float]:
        return self._func_by_index(self._Q_i_m_func)

    @property
    def distance_list(self) -> list[float]:
        return self._func_by_index(self._distance_func)

    def distance_and_Q(self) -> DataFrame:
        """Return basic DataFrame with Distance and Q_i^m columns."""
        return DataFrame(
            {
                self.employment_column_name: self.Q_i_m_list,
                self.distance_column_name: self.distance_list,
            },
            index=self.ij_m_index,
        )


@dataclass
class AttractionConstrained(SpatialInteractionBaseClass):

    beta: float = 0.0002
    constrained_column_name: str = "B_j^m * Q_i^m * exp(-β c_{ij})"

    def __repr__(self) -> str:
        """Return base config of model."""
        return f"Singly constrained attraction β = {self.beta}"

    def __post_init__(self) -> None:
        """Calculate core singly constrained spatial components."""
        self.B_j_m = self.distance_and_Q()
        self.B_j_m["-β c_{ij}"] = -1 * self.B_j_m[self.distance_column_name] * self.beta
        self.B_j_m["exp(-β c_{ij})"] = self.B_j_m["-β c_{ij}"].apply(lambda x: exp(x))
        self.B_j_m["Q_i^m * exp(-β c_{ij})"] = (
            self.B_j_m[self.employment_column_name] * self.B_j_m["exp(-β c_{ij})"]
        )
        self.B_j_m["sum Q_i^m * exp(-β c_{ij})"] = self.B_j_m.groupby(
            ["Other_City", "Sector"]
        )["Q_i^m * exp(-β c_{ij})"].transform("sum")

        # Equation 16
        self.B_j_m["B_j^m"] = 1 / self.B_j_m["sum Q_i^m * exp(-β c_{ij})"]

    @property
    def y_ij_m(self) -> DataFrame:
        """A dataframe initial conditions for model y_ij_m DataFrame."""
        return DataFrame(
            data={
                self.employment_column_name: self.B_j_m[self.employment_column_name],
                "B_j^m": self.B_j_m["B_j^m"],
                "exp(-β c_{ij})": self.B_j_m["exp(-β c_{ij})"],
                self.constrained_column_name: (
                    self.B_j_m["B_j^m"] * self.B_j_m["Q_i^m * exp(-β c_{ij})"]
                ),
            }
        )


@dataclass
class DoublyConstrained(SpatialInteractionBaseClass):

    beta: float = 0.0002
    constrained_column_name: str = "B_j^m * Q_i^m * exp(-β c_{ij})"

    def __repr__(self) -> str:
        """Return base config of model."""
        return f"Singly constrained attraction β = {self.beta}"

    def __post_init__(self) -> None:
        """Calculate core singly constrained spatial components."""
        self.b_ij_m = self.distance_and_Q()
        self.b_ij_m["-β c_{ij}"] = (
            -1 * self.b_ij_m[self.distance_column_name] * self.beta
        )
        self.b_ij_m["exp(-β c_{ij})"] = self.b_ij_m["-β c_{ij}"].apply(lambda x: exp(x))

    def doubly_constrained(self) -> DataFrame:
        pass


@dataclass
class InterRegionInputOutputBaseClass:

    """Bass attributes for InputOutput Model and TimeSeries."""

    max_import_export_model_iterations: int = DEFAULT_IMPORT_EXPORT_ITERATIONS
    regions: dict[str, str] = field(default_factory=lambda: deepcopy(CITY_REGIONS))

    sector_aggregation: AggregatedSectorDictType = field(
        default_factory=lambda: deepcopy(SECTOR_10_CODE_DICT)
    )
    P_initial_export_proportion: float = INITIAL_P

    centre_for_cities_path: PathLike = CENTRE_FOR_CITIES_PATH
    centre_for_cities_spatial_path: PathLike = CITIES_TOWNS_SHAPE_PATH
    distance_unit_factor: float = DISTANCE_UNIT_DIVIDE
    final_demand_column_names: list[str] = field(
        default_factory=lambda: IO_TABLE_FINAL_DEMAND_COLUMN_NAMES
    )
    export_column_names: list[str] = field(
        default_factory=lambda: IO_TABLE_EXPORT_COLUMN_NAMES
    )
    imports_column_name: str = IO_TABLE_IMPORTS_COLUMN_NAME
    total_production_column_name: str = IO_TABLE_TOTAL_PRODUCTION_COLUMN_NAME
    national_employment_scale: float = JOBS_BY_SECTOR_SCALING
    io_table_scale: float = IO_TABLE_SCALING
    _spatial_model_cls: Type[SpatialInteractionBaseClass] = AttractionConstrained
    _exogenous_i_m_func: Callable[..., Series] = andrews_suggestion
    _import_export_convergence: Callable[..., DataFrame] = import_export_convergence

    @property
    def region_names(self) -> list[str]:
        """Return the region names."""
        return list(self.regions.keys())

    @property
    def sectors(self) -> list[str]:
        return [*self.sector_aggregation]


@dataclass
class InterRegionInputOutput(InterRegionInputOutputBaseClass):

    """Manage Inter Region input output model runs."""

    io_table_file_path: PathLike = IO_TABLE_2017_EXCEL_PATH
    city_sector_employment_path: PathLike = CITY_SECTOR_EMPLOYMENT_PATH
    national_employment_path: PathLike = JOBS_BY_SECTOR_PATH
    employment_date: date = EMPLOYMENT_QUARTER_DEC_2017
    date: Optional[date] = None

    def __post_init__(self) -> None:
        """Initialise model based on path attributes in preparation for run."""
        self._raw_region_data: GeoDataFrame = load_and_join_centre_for_cities_data(
            city_path=self.centre_for_cities_path,
            spatial_path=self.centre_for_cities_spatial_path,
        )
        self._raw_io_table: ONSInputOutputTable = ONSInputOutputTable(
            path=self.io_table_file_path
        )
        self._national_employment: DataFrame = load_region_employment(
            path=self.national_employment_path
        )
        self._employment_by_sector_and_city: DataFrame = (
            load_employment_by_city_and_sector(path=self.city_sector_employment_path)
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

    @property
    def _ij_index(self) -> MultiIndex:
        """Return self.city x self.city MultiIndex."""
        return generate_ij_index(self.regions, self.regions)

    @property
    def _ij_m_index(self) -> MultiIndex:
        """Return self.city x self.city MultiIndex."""
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
        else:
            return (
                self._national_employment.loc[str(self.employment_date)]
                * self.national_employment_scale
            )

    @cached_property
    def region_data(self) -> GeoDataFrame:
        return self._raw_region_data.loc[self.regions]

    @cached_property
    def io_table(self) -> DataFrame:
        """Return national Input-Ouput table, aggregated by self.sector_aggregation.

        Todo:
            * Check ways of skipping aggregation
        """
        if self.sector_aggregation:
            return (
                self._raw_io_table.get_aggregated_io_table(
                    sector_aggregation_dict=self.sector_aggregation
                )
                * self.io_table_scale
            )
        else:
            raise NotImplementedError(
                "Currently io_table requires an aggregation dictionary."
            )

    @cached_property
    def technical_coefficients(self) -> DataFrame:
        """Return the technical coefficients derived from self.io_table."""
        return technical_coefficients(self.io_table, self.sectors)

    @cached_property
    def employment_table(self) -> DataFrame:
        """Return employment table, aggregated if self.sector_aggregation set."""
        if self.sector_aggregation:
            self._employment_by_sector_and_city_aggregated = aggregate_rows(
                self._employment_by_sector_and_city, True
            )
            return filter_by_region_name_and_type(
                self._employment_by_sector_and_city_aggregated, self.region_names
            )
        else:
            return filter_by_region_name_and_type(
                self._employment_by_sector_and_city, self.region_names
            )

    @cached_property
    def X_i_m(self) -> DataFrame:
        """Return the total production of sector 𝑚 in city 𝑖and cache results.

        X_i^m = X_*^m * Q_i^m/Q_*^m
        """
        return X_i_m(
            total_sales=self.io_table[self.sectors].loc["Total Sales"],
            employment=self.employment_table,
            national_employment=self.national_employment,
        )

    @cached_property
    def M_i_m(self) -> DataFrame:
        """Return the imports of sector 𝑚 in city 𝑖and cache results.

        M_i^m = M_*^m * Q_i^m/Q_*^m
        """
        return M_i_m(
            imports=self.io_table[self.sectors].loc["Imports"],
            employment=self.employment_table,
            national_employment=self.national_employment,
        )

    @cached_property
    def F_i_m(self) -> DataFrame:
        """Return the final demand of sector 𝑚 in city 𝑖and cache results.

        F_i^m = F_*^m * Q_i^m/Q_*^m
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
        """Return the exports of sector 𝑚 in city 𝑖and cache results.

        E_i^m = E_*^m * Q_i^m/Q_*^m
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
        return calc_city_distance(
            self.region_data,
            self.regions,
            self.regions,
            unit_divide_conversion=self.distance_unit_factor,
        )

    @cached_property
    def x_i_mn_summed(self) -> DataFrame:
        """Return sum of all total demands for good 𝑚 in city 𝑖.

        Equation 1:
        x_i^{mn} = a_i^{mn}X_i^n

        Equation 2:

        .. math::
            X_i^m + m_i^m + M_i^m = F_i^m + e_i^m + E_i^m + \\sum_n{a_i^{mn}X_i^n}

        Note: the \\s is to avoid a docstring warning, and should have a single \
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
            city_names=self.region_names,
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


class ModelsOrDatesAmbiguityError(Exception):

    pass


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
    def from_dates(
        cls,
        dates: Union[Iterable[date], dict[date, dict]],
        input_output_model_cls: Type[InterRegionInputOutput] = InterRegionInputOutput,
        **kwargs,
    ) -> "InterRegionInputOutputTimeSeries":
        """Generate an InterRegionInputOutputTimeSeries from a list of dates."""
        logger.info(
            "Generating an InputOutputTimeSeries with dates and passed general config."
        )
        io_models = []
        if type(dates) is dict:
            logger.debug(f"Iterating over {len(dates)} with dict configs")
            for date, config_dict in dates.items():
                io_model = input_output_model_cls(date=date, **(config_dict | kwargs))
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
