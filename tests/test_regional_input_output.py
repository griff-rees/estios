from pandas import MultiIndex, Series
from pandas.testing import assert_series_equal
import pytest

from regional_input_output import __version__
from regional_input_output.input_output import (
    CITY_REGIONS,
    SECTOR_10_CODE_DICT,
    InterRegionInputOutput,
    generate_im_index,
    generate_ij_index,
    generate_ijm_index,
)
from regional_input_output.utils import get_all_centre_for_cities_dict


def test_version() -> None:
    assert __version__ == "0.1.0"


class TestMultiIndexeGenerators:
    def test_im_index(self) -> None:
        """Test correct hierarchical dimensions for an im index."""
        default_im_index: MultiIndex = generate_im_index()
        assert len(default_im_index) == len(CITY_REGIONS) * len(SECTOR_10_CODE_DICT)
        assert set(default_im_index.get_level_values(0)) == set(CITY_REGIONS)
        assert set(default_im_index.get_level_values(1)) == set(SECTOR_10_CODE_DICT)

    def test_ij_index(self) -> None:
        """Test correct hierarchical dimensions for an ij index."""
        default_im_index: MultiIndex = generate_ij_index()
        assert len(default_im_index) == len(CITY_REGIONS) * len(CITY_REGIONS)
        assert set(default_im_index.get_level_values(0)) == set(CITY_REGIONS)
        assert set(default_im_index.get_level_values(1)) == set(CITY_REGIONS)

    def test_ijm_index(self) -> None:
        """Test correct hierarchical dimensions for an ijm index."""
        default_im_index: MultiIndex = generate_ijm_index()
        assert len(default_im_index) == (
            # Note i=j is excluded
            len(CITY_REGIONS)
            * (len(CITY_REGIONS) - 1)
            * len(SECTOR_10_CODE_DICT)
        )
        assert set(default_im_index.get_level_values(0)) == set(CITY_REGIONS)
        assert set(default_im_index.get_level_values(1)) == set(CITY_REGIONS)
        assert set(default_im_index.get_level_values(2)) == set(SECTOR_10_CODE_DICT)


THREE_CITIES: tuple[str, str, str] = ("Manchester", "Leeds", "Liverpool")


@pytest.fixture
def three_cities() -> dict[str, str]:
    return {
        city: region for city, region in CITY_REGIONS.items() if city in THREE_CITIES
    }


@pytest.fixture
def three_cities_io(three_cities: dict[str, str]) -> InterRegionInputOutput:
    return InterRegionInputOutput(regions=three_cities)


@pytest.fixture
def all_cities() -> dict[str, str]:
    return get_all_centre_for_cities_dict()


@pytest.fixture
def all_cities_io(all_cities: dict[str, str]) -> InterRegionInputOutput:
    return InterRegionInputOutput(regions=all_cities)


class TestInputOutputModel:

    """Test constructing and running an InterRegionInputOutput model."""

    def test_default_construction(self) -> None:
        io_model = InterRegionInputOutput()
        assert (
            str(io_model)
            == "Input output model of 10 sectors between 10 cities in 2017"
        )

    def test_3_city_construction(self, three_cities) -> None:
        io_model = InterRegionInputOutput(regions=three_cities)
        assert (
            str(io_model) == "Input output model of 10 sectors between 3 cities in 2017"
        )

    def test_3_city_distances(self, three_cities_io) -> None:
        CORRECT_DISTANCES = Series(
            [
                104.05308373,
                58.24977679,
                104.05308373,
                49.31390539,
                58.24977679,
                49.31390539,
            ],
            index=three_cities_io.distances.index,
            name="Distance",
        )
        assert_series_equal(three_cities_io.distances["Distance"], CORRECT_DISTANCES)


class TestInputOutputModelAllCities:
    def test_all_city_construction(self, all_cities_io) -> None:
        assert (
            str(all_cities_io)
            == "Input output model of 10 sectors between 48 cities in 2017"
        )

    def test_all_city_distances(self, all_cities_io) -> None:
        CORRECT_HEAD_DISTANCES = Series(
            [
                256.638308,
                103.219690,
                122.786283,
                107.799854,
                316.213934,
            ],
            index=all_cities_io.distances.head().index,
            name="Distance",
        )
        CORRECT_TAIL_DISTANCES = Series(
            [
                169.575114,
                41.272173,
                118.191104,
                111.975798,
                353.505924,
            ],
            index=all_cities_io.distances.tail().index,
            name="Distance",
        )
        assert_series_equal(
            all_cities_io.distances["Distance"].head(), CORRECT_HEAD_DISTANCES
        )
        assert_series_equal(
            all_cities_io.distances["Distance"].tail(), CORRECT_TAIL_DISTANCES
        )
