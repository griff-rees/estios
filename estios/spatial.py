#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import UserDict
from collections.abc import Sized
from dataclasses import dataclass, field
from logging import getLogger
from typing import Any, Callable, Generator, Sequence

from geopandas import GeoDataFrame
from numpy import exp
from pandas import DataFrame, MultiIndex, Series

from .calc import CITY_POPULATION_COLUMN_NAME, DISTANCE_COLUMN
from .sources import MetaData
from .utils import DateType, generate_ij_m_index

# from .uk.utils import UK_NATIONAL_COLUMN_NAME

# from .uk.utils import UK_NATIONAL_COLUMN_NAME

logger = getLogger(__name__)


@dataclass(repr=False)
class Region:
    name: str
    code: str | None
    geography_type: str | None
    alternate_names: dict[str, str] = field(default_factory=dict)
    date: DateType | int | None = None
    flags: dict[str, bool | str | int] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.geography_type} {self.name}"

    def __repr__(self) -> str:
        """Return a str indicated class type and number of sectors.

        Todo:
            * Apply __repr__ format coherence across classes
        """
        repr: str = f"{self.__class__.__name__}("
        repr += f"name={self.name}, "
        repr += f"date={self.date}, "
        repr += f"code={self.code}, "

        repr += f"geography_type={self.geography_type})"
        return repr

    @property
    def alternate_name_types(self) -> tuple[str, ...]:
        return tuple(self.alternate_names.keys())

    @property
    def alternate_names_count(self) -> int:
        return len(self.alternate_name_types)


RegionsManagerType = UserDict[str, Region]


class NullCodeException(Exception):
    pass


class RegionsManagerMixin:

    """Base mixin methods for RegionsManager inheritance."""

    meta_data: MetaData | None
    region_name: str | None

    def __init__(
        self, meta_data: MetaData | None = None, region_name: str | None = None
    ) -> None:
        super().__init__()
        self.meta_data = meta_data
        self.region_name = region_name

    def __str__(self) -> str:
        prefix: str = f"{len(self)} " if isinstance(self, Sized) else ""
        return f"{prefix} {self.region_name} region data from {self.meta_data}"

    def __repr__(self) -> str:
        """Return a str indicated class type and number of sectors.

        Todo:
            * Apply __repr__ format coherence across classes
        """
        repr: str = f"{self.__class__.__name__}("
        repr += f"count={self.names_count}, "
        repr += f"codes_count={self.codes_count})"
        return repr

    def names_generator(self) -> Generator[str, None, None]:
        assert isinstance(self, UserDict)
        for region_name in self:
            if not region_name:
                raise ValueError(f"Invalid `region_name`: {region_name} for {self}")
            yield region_name

    def codes_generator(self) -> Generator[str, None, None]:
        assert isinstance(self, UserDict)
        for region_details in self.values():
            if isinstance(region_details.code, str):
                yield region_details.code
            else:
                raise NullCodeException(f"{self} has no code set.")

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self.names_generator())

    @property
    def names_count(self) -> int:
        return len(self.names)

    @property
    def codes(self) -> tuple[str, ...]:
        return tuple(self.codes_generator())

    @property
    def codes_count(self) -> int:
        return len(self.codes)


class RegionsManager(RegionsManagerMixin, RegionsManagerType):

    """Class for managing and indexing Regions."""

    pass

    # def codes_generator(self) -> Generator[str, None, None]:
    #     for region_name in self:
    #         if not region_name:
    #             raise ValueError(f"Invalid `code` self[region_namefor {self[]}")
    #         yield region_name

    # @property
    # def valid_names_iter(self) -> Generator[str, None, None]:
    #     """Return all names that are not empty `str`s."""
    #     for name in self.names:
    #         if name:  #
    #             yield name

    # @property
    # def valid_names(self) -> tuple[str, ...]:
    #     return tuple(self.valid_names_iter)

    # @property
    # def codes(self) -> Generator[str, None, None]:
    #     for region_details in self.values():
    #         if isinstance(region_details.code, str):
    #             yield region_details.code
    #         else:
    #             raise NullCodeException(f"{self} has no code set.")


GenericRegionsManager = RegionsManagerType | UserDict[str, Any]


class MissingAttributeColumnException(Exception):
    pass


def sum_for_regions_by_attr(
    df: DataFrame,
    region_names: Sequence[str],
    column_names: Sequence[str | int],
    regions: GenericRegionsManager,
    attr: str,
    set_index_to_column: str | None = None,
    ignore_key_errors: bool = False,
    strict_set_index_to_column: bool = False,
) -> Generator[tuple[str, float | Series], None, None]:
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
    if set_index_to_column:
        if df.index.name == set_index_to_column:
            if set_index_to_column in df.columns:
                assert df[set_index_to_column] == df.index
            else:
                if strict_set_index_to_column:
                    raise ValueError(
                        f"`set_index_to_column`: '{set_index_to_column}' "
                        f"only matches `df.index.name` and "
                        f"`strict_set_index_to_column` is '{strict_set_index_to_column}'."
                    )
                else:
                    logger.warning(
                        f"`set_index_to_column`: '{set_index_to_column}' "
                        "only matches `df.index.name`, assuming correct."
                    )
        else:
            df.set_index(set_index_to_column, inplace=True)
    for region in region_names:
        try:
            indexes: list[str] = list(getattr(regions[region], attr))
            yield region, df.loc[indexes, column_names].sum().sum()  # .sum()
        except KeyError as err:
            if ignore_key_errors:
                logger.error(f"Raised by {region}: {err}")
            else:
                raise err
        # Below is added to manage slightly different structures from OECD data
        # else:
        #     indexes: list[str] = list(getattr(regions[region], attr))
        #     try:
        #         yield region, df.loc[indexes][column_names].sum().sum()  # .sum()
        #     except KeyError as err:
        #         if ignore_key_errors:
        #             logger.error(f"Raised by {region}: {err}")
        #         else:
        #             raise err

    # return {
    #     region: df.loc[getattr(regions[region], attr), column_names]
    #     .sum()
    #     .sum()  # .sum()
    #     for region in region_names
    # }


@dataclass
class SpatialInteractionBaseClass:
    # beta: float
    distances: GeoDataFrame
    employment: DataFrame
    national_column_name: str
    employment_column_name: str = CITY_POPULATION_COLUMN_NAME
    distance_column_name: str = DISTANCE_COLUMN
    national_term: bool = True

    _gen_ij_m_func: Callable[..., MultiIndex] = generate_ij_m_index

    @property
    def y_ij_m(self) -> DataFrame:
        """Placeholder for initial conditions for model y_ij_m DataFrame."""
        raise NotImplementedError("This is not implemented in the BaseClass")

    @property
    def ij_m_index(self) -> MultiIndex:
        """Return region x other region x sector MultiIndex."""
        return self._gen_ij_m_func(
            self.employment.index,
            self.employment.columns,
            national_column_name=self.national_column_name,
        )

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
