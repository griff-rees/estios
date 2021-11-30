from pandas import MultiIndex
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
