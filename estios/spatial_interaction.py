#!/usr/bin/env python
# -*- coding: utf-8 -*-

from dataclasses import dataclass
from typing import Callable

from geopandas import GeoDataFrame
from numpy import exp
from pandas import DataFrame, MultiIndex

from .calc import CITY_POPULATION_COLUMN_NAME, DISTANCE_COLUMN
from .utils import UK_NATIONAL_COLUMN_NAME, generate_ij_m_index


@dataclass
class SpatialInteractionBaseClass:

    # beta: float
    distances: GeoDataFrame
    employment: DataFrame
    employment_column_name: str = CITY_POPULATION_COLUMN_NAME
    distance_column_name: str = DISTANCE_COLUMN
    national_term: bool = True
    national_column_name: str = UK_NATIONAL_COLUMN_NAME

    _gen_ij_m_func: Callable[..., MultiIndex] = generate_ij_m_index

    @property
    def y_ij_m(self) -> DataFrame:
        """Placeholder for initial conditions for model y_ij_m DataFrame."""
        raise NotImplementedError("This is not implemented in the BaseClass")

    @property
    def ij_m_index(self) -> MultiIndex:
        """Return region x other region x sector MultiIndex."""
        return self._gen_ij_m_func(self.employment.index, self.employment.columns)

    def _func_by_index(self, func):
        return [
            func(region, other_region, sector)
            for region, other_region, sector in self.ij_m_index
        ]

    def _Q_i_m_func(self, region, other_region, sector) -> float:
        return self.employment.loc[region][sector]

    def _distance_func(self, region, other_region, sector) -> float:
        return self.distances[self.distance_column_name][region][other_region]

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


# def NullRawRegionError(BaseException):
#     pass


# def RawRegionTypeError(NotImplementedError):
#     pass
