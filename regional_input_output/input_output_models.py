#!/usr/bin/env python
# -*- coding: utf-8 -*-

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date
from functools import cached_property
from os import PathLike
from typing import Callable, Final, Type

from geopandas import GeoDataFrame
from numpy import exp
from pandas import DataFrame, MultiIndex

from .input_output_func import (
    DEFAULT_IMPORT_EXPORT_ITERATIONS,
    DISTANCE_UNIT_DIVIDE,
    INITIAL_P,
    E_i_m,
    F_i_m,
    M_i_m,
    X_i_m,
    calc_city_distance,
    generate_e_m_dataframe,
    import_export_force_convergence,
    technical_coefficients,
    x_i_mn_summed,
)
from .uk_data.utils import (
    CENTRE_FOR_CITIES_PATH,
    CITIES_TOWNS_SHAPE_PATH,
    CITY_REGIONS,
    CITY_SECTOR_EMPLOYMENT_PATH,
    EMPLOYMENT_QUARTER_DEC_2017,
    IO_TABLE_2017_EXCEL_PATH,
    IO_TABLE_EXPORT_COLUMN_NAMES,
    IO_TABLE_FINAL_DEMAND_COLUMN_NAMES,
    IO_TABLE_IMPORTS_COLUMN_NAME,
    IO_TABLE_TOTAL_PRODUCTION_COLUMN_NAME,
    JOBS_BY_SECTOR_PATH,
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

CITY_POPULATION_COLUMN_NAME: Final[str] = "Q_i^m"


@dataclass
class SpatialConstrainedBaseClass:

    distances: GeoDataFrame
    employment: DataFrame
    employment_column_name: str = CITY_POPULATION_COLUMN_NAME

    @property
    def y_ij_m(self) -> DataFrame:
        """Placeholder for initial conditions for model y_ij_m DataFrame."""
        raise NotImplementedError("This is not implemented in the BaseClass")


@dataclass
class AttractionConstrained(SpatialConstrainedBaseClass):

    beta: float = 0.0002
    constrained_column_name: str = "B_j^m * Q_i^m * exp(-β c_{ij})"

    def __repr__(self) -> str:
        """Return base config of model."""
        return f"Singly constrained attraction β = {self.beta}"

    @property
    def _ij_m_index(self) -> MultiIndex:
        """Return sector x city x other city MultiIndex."""
        return generate_ij_m_index(self.employment.index, self.employment.columns)

    def __post_init__(self) -> None:
        """Calculate core singly constrained spatial components."""
        self.B_j_m: DataFrame = DataFrame({"Q_i^m": None}, index=self._ij_m_index)

        # Inefficient, should just be reindexing distances
        self.B_j_m["Distance"] = self.B_j_m.apply(
            lambda row: self.distances["Distance"][row.name[0]][row.name[1]], axis=1
        )
        self.B_j_m[self.employment_column_name] = self.B_j_m.apply(
            lambda row: self.employment.loc[row.name[0]][row.name[2]], axis=1
        )
        self.B_j_m["-β c_{ij}"] = -1 * self.B_j_m["Distance"] * self.beta
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
class InterRegionInputOutput:

    """Manage Inter Region input output model runs."""

    # Add these to support multiple runs
    # _start_run_time: Optional[datetime] = field(default_factory=datetime.now)
    # _end_run_time: Optional[datetime] = None

    import_export_iterations: int = DEFAULT_IMPORT_EXPORT_ITERATIONS
    regions: dict[str, str] = field(default_factory=lambda: deepcopy(CITY_REGIONS))
    employment_date: date = EMPLOYMENT_QUARTER_DEC_2017

    sector_aggregation: AggregatedSectorDictType = field(
        default_factory=lambda: deepcopy(SECTOR_10_CODE_DICT)
    )
    P_initial_export_proportion: float = INITIAL_P

    centre_for_cities_path: PathLike = CENTRE_FOR_CITIES_PATH
    centre_for_cities_spatial_path: PathLike = CITIES_TOWNS_SHAPE_PATH
    io_table_file_path: PathLike = IO_TABLE_2017_EXCEL_PATH
    city_sector_employment_path: PathLike = CITY_SECTOR_EMPLOYMENT_PATH
    national_employment_path: PathLike = JOBS_BY_SECTOR_PATH
    distance_unit_factor: float = DISTANCE_UNIT_DIVIDE
    final_demand_column_names: list[str] = field(
        default_factory=lambda: IO_TABLE_FINAL_DEMAND_COLUMN_NAMES
    )
    export_column_names: list[str] = field(
        default_factory=lambda: IO_TABLE_EXPORT_COLUMN_NAMES
    )
    imports_column_name: str = IO_TABLE_IMPORTS_COLUMN_NAME
    total_production_column_name: str = IO_TABLE_TOTAL_PRODUCTION_COLUMN_NAME
    _spatial_model_cls: Type[SpatialConstrainedBaseClass] = AttractionConstrained
    _import_export_convergence: Callable[
        ..., DataFrame
    ] = import_export_force_convergence

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
    def region_names(self) -> list[str]:
        """Return the region names."""
        return list(self.regions.keys())

    @property
    def year(self) -> int:
        """Return the year of the employment_date record."""
        return self.employment_date.year

    @property
    def sectors(self) -> list[str]:
        return [*self.sector_aggregation]

    @cached_property
    def national_employment(self) -> DataFrame:
        """Return DataFrame, aggregated if sector_aggregation set."""
        if self.sector_aggregation:
            self._aggregated_national_employment: DataFrame = aggregate_rows(
                self._national_employment
            )
            return self._aggregated_national_employment.loc[str(self.employment_date)]
        else:
            return self._national_employment.loc[str(self.employment_date)]

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
            return self._raw_io_table.get_aggregated_io_table(
                sector_aggregation_dict=self.sector_aggregation
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
    def spatial_interaction(self) -> SpatialConstrainedBaseClass:
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

    def import_export_convergence(self) -> tuple[DataFrame, DataFrame]:
        """Return the final results of model convergence in two dataframes.

        Todo:
            * Refactor to minimise call configuration.
            * Rename e_i_m_summed to x_i_mn_summed
            * Consider refactoring convergence as class
        """
        self.e_m_model, self.y_ij_m_model = import_export_force_convergence(
            e_m_cities=self._initial_e_m,
            y_ij_m=self._y_ij_m,
            F_i_m=self.F_i_m,
            E_i_m=self.E_i_m,
            x_i_m_summed=self.x_i_mn_summed,
            X_i_m=self.X_i_m,
            M_i_m=self.M_i_m,
            employment=self.employment_table,
            iterations=self.import_export_iterations,
        )
        return self.e_m_model, self.y_ij_m_model


@dataclass
class InterRegionInputOutputTimeSeries(InterRegionInputOutput):

    """An Input-Output models over time."""

    years: list[int] = field(default_factory=list)
    _input_output_model_cls: Type[InterRegionInputOutput] = InterRegionInputOutput
    _spatial_model_cls: Type[SpatialConstrainedBaseClass] = AttractionConstrained
