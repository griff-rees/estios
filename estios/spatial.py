#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import UserDict
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable, Final, Generator, Sequence

from geopandas import GeoDataFrame
from numpy import exp
from pandas import DataFrame, MultiIndex, Series

from .calc import CITY_POPULATION_COLUMN_NAME, DISTANCE_COLUMN
from .sources import MetaData
from .utils import UK_NATIONAL_COLUMN_NAME, generate_ij_m_index

LA_CODES_COLUMN: Final[str] = "la_codes"


@dataclass
class Region:

    name: str
    code: str | None
    geography_type: str | None
    alternate_names: dict[str, str] = field(default_factory=dict)
    date: date | int | None = None
    flags: dict[str, bool | str | int] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.geography_type} {self.name}"


RegionsManagerType = UserDict[str, Region]


class NullCodeException(Exception):
    pass


class RegionsManager(RegionsManagerType):

    """Class for managing and indexing Regions."""

    meta_data: MetaData | None

    def __init__(self, meta_data: MetaData | None = None) -> None:
        super().__init__()
        self.meta_data = meta_data

    def __str__(self) -> str:
        return f"{len(self)} UK region data from {self.meta_data}"

    @property
    def names(self) -> Generator[str, None, None]:
        for region_name in self:
            yield region_name

    @property
    def codes(self) -> Generator[str, None, None]:
        for region_details in self.values():
            if isinstance(region_details.code, str):
                yield region_details.code
            else:
                raise NullCodeException(f"{self} has no code set.")


GenericRegionsManager = RegionsManagerType | UserDict[str, Any]


class MissingAttributeColumnException(Exception):
    pass


def sum_for_regions_by_attr(
    df: DataFrame,
    region_names: Sequence[str],
    column_names: Sequence[str | int],
    regions: GenericRegionsManager,
    attr: str = LA_CODES_COLUMN,
) -> dict[str, float | Series]:
    """Sum columns for passed pua_names from df.

    Todo:
        * Basic unit tests
        * Potentially generalise for different number of sum calls.
    """
    try:
        assert hasattr(regions[region_names[0]], attr)
    except AssertionError:
        raise MissingAttributeColumnException(
            f"{region_names[0]} is not available for {attr} in passed regions"
        )
    return {
        region: df.loc[getattr(regions[region], attr), column_names]
        .sum()
        .sum()  # .sum()
        for region in region_names
    }


def sum_for_regions_by_la_code(
    df: DataFrame,
    region_names: Sequence[str],
    column_names: Sequence[str | int],
    regions: GenericRegionsManager,
) -> dict[str, float | Series]:
    return sum_for_regions_by_attr(
        df=df,
        region_names=region_names,
        column_names=column_names,
        regions=regions,
        attr=LA_CODES_COLUMN,
    )


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
