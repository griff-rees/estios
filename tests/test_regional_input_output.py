from pandas import MultiIndex

from regional_input_output import __version__
from regional_input_output.input_output import (
    CITY_REGIONS,
    SECTOR_10_CODE_DICT,
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
