"""
Core tests of running regional input output modelling.

Todo:
    * test raising NullRawRegionError and RawRegionTypeError,
"""
import pytest
from pandas import DataFrame, Series
from pandas.testing import assert_frame_equal, assert_series_equal

from estios import __version__
from estios.uk.models import InterRegionInputOutputUK2017
from estios.uk.regions import CENTRE_FOR_CITIES_NAME_FIX_DICT


def test_version() -> None:
    """Keep track of library version."""
    assert __version__ == "0.1.0"


class TestInputOutputModel:

    """Test constructing and running a 3 city InterRegionInputOutput model."""

    @pytest.mark.skip(f"Cache currently interferes with 3 city results")
    def test_default_construction(
        self,
        tmp_path,
        monkeypatch,
        nomis_2017_10_cities_employment,
        nomis_2017_national_employment,
    ) -> None:  # -> Generator[TestMetaExample, None, None]:
        monkeypatch.chdir(tmp_path)  # Enforce location to fit tmp_path
        io_model = InterRegionInputOutputUK2017()
        assert (
            str(io_model) == "UK 2017-06-01 Input-Output model: 10 sectors, 10 regions"
        )
        assert (
            repr(io_model)
            == "InterRegionInputOutputUK2017(nation='UK', date='2017-06-01', sectors=10, regions=10)"
        )
        assert_frame_equal(
            io_model.regional_employment, nomis_2017_10_cities_employment
        )
        # assert_series_equal(io_model.national_employment, nomis_2017_national_employment)

    def test_3_city_construction(
        self,
        # tmp_path,
        # monkeypatch,
        three_cities_io,
        # three_cities,
        nomis_2017_3_cities_employment,
        nomis_2017_national_employment,
    ) -> None:
        # monkeypatch.chdir(tmp_path)  # Enforce location to fit tmp_path
        # io_model = InterRegionInputOutputUK2017(regions=three_cities)
        assert (
            str(three_cities_io)
            == "UK 2017-06-01 Input-Output model: 10 sectors, 3 regions"
        )
        assert (
            repr(three_cities_io)
            == "InterRegionInputOutputUK2017(nation='UK', date='2017-06-01', sectors=10, regions=3)"
        )
        assert_frame_equal(
            three_cities_io.regional_employment, nomis_2017_3_cities_employment
        )
        # assert_series_equal(three_cities_io.national_employment, nomis_2017_national_employment)

    def test_3_city_national_employment(
        self, three_cities_io, nomis_2017_national_employment
    ) -> None:
        assert_series_equal(
            three_cities_io.national_employment, nomis_2017_national_employment
        )

    def test_default_construction(self) -> None:
        io_model = InterRegionInputOutputUK2017()
        assert (
            str(io_model) == "UK 2017-06-01 Input-Output model: 10 sectors, 10 regions"
        )
        assert (
            repr(io_model)
            == "InterRegionInputOutputUK2017(nation='UK', date='2017-06-01', sectors=10, regions=10)"
        )

    def test_3_city_construction(self, three_cities) -> None:
        io_model = InterRegionInputOutputUK2017(regions=three_cities)
        assert (
            str(io_model) == "UK 2017-06-01 Input-Output model: 10 sectors, 3 regions"
        )
        assert (
            repr(io_model)
            == "InterRegionInputOutputUK2017(nation='UK', date='2017-06-01', sectors=10, regions=3)"
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

    def test_national_final_demand(
        self, three_cities_io, correct_agg_uk_nation_final_demand
    ) -> None:
        assert (
            (
                three_cities_io.national_final_demand
                == correct_agg_uk_nation_final_demand
            )
            .all()
            .all()
        )

    @pytest.mark.xfail(reason="These need to be rechecked with new city units")
    def test_3_city_singly_constrained_national_table(
        self,
        three_cities_results,
        correct_uk_national_employment_2017,
        correct_uk_ons_X_m_national,
        correct_uk_ons_I_m_national,
        correct_uk_ons_S_m_national,
        correct_uk_gva_2017,
        correct_leeds_2017_final_demand,
        correct_leeds_2017_exports,
        correct_leeds_2017_imports,
    ) -> None:
        # CORRECT_X_m_national.index.name = "Area"
        # assert_series_equal(
        #     correct_uk_national_employment_2017,
        #     three_cities_results.national_employment,
        # )
        assert_series_equal(
            correct_uk_ons_X_m_national, three_cities_results.X_m_national
        )
        assert_series_equal(
            correct_uk_ons_I_m_national, three_cities_results.I_m_national
        )
        assert_series_equal(
            correct_uk_ons_S_m_national, three_cities_results.S_m_national
        )
        assert_series_equal(correct_uk_gva_2017, three_cities_results.GVA_m_national)
        assert_series_equal(
            correct_leeds_2017_final_demand.sum(axis="columns"),
            three_cities_results.F_i_m.loc["Leeds"],
        )
        assert_series_equal(
            correct_leeds_2017_exports.sum(axis="columns"),
            three_cities_results.E_i_m.loc["Leeds"],
        )
        assert_series_equal(
            correct_leeds_2017_imports, three_cities_results.M_i_m["Leeds"]
        )

    @pytest.mark.xfail(reason="These need to be rechecked with new city units")
    def test_3_city_singly_constrained(self, three_cities_results) -> None:
        CORRECT_Q_i_m = [
            40,
            22035,
            12000,
            71000,
            15000,
            21000,
            4500,
            76000,
            88000,
            14000,
            40,
            22035,
            12000,
            71000,
            15000,
            21000,
            4500,
            76000,
            88000,
            14000,
            10,
            18960,
            8000,
            75000,
            7000,
            10000,
            5000,
            40000,
            94000,
            15500,
            10,
            18960,
            8000,
            75000,
            7000,
            10000,
            5000,
            40000,
            94000,
            15500,
            75,
            13880,
            9000,
            88000,
            14000,
            20000,
            10000,
            96000,
            100000,
            18000,
            75,
            13880,
            9000,
            88000,
            14000,
            20000,
            10000,
            96000,
            100000,
            18000,
        ]
        B_j_m_im_column: str = "B_j^m * Q_i^m * exp(-β c_{ij})"
        CORRECT_B_j_m_Q_im_distance = [
            0.345347,
            0.610933,
            0.568745,
            0.443837,
            0.514507,
            0.509459,
            0.308007,
            0.439162,
            0.465360,
            0.434808,
            0.799714,
            0.537060,
            0.599571,
            0.485855,
            0.681430,
            0.677029,
            0.473239,
            0.654769,
            0.483070,
            0.474131,
            0.116699,
            0.575108,
            0.468307,
            0.457848,
            0.331301,
            0.331301,
            0.331301,
            0.292219,
            0.482248,
            0.460410,
            0.200286,
            0.462940,
            0.400429,
            0.514145,
            0.318570,
            0.322971,
            0.526761,
            0.345231,
            0.516930,
            0.525869,
            0.883301,
            0.424892,
            0.531693,
            0.542152,
            0.668699,
            0.668699,
            0.668699,
            0.707781,
            0.517752,
            0.539590,
            0.654653,
            0.389067,
            0.431255,
            0.556163,
            0.485493,
            0.490541,
            0.691993,
            0.560838,
            0.534640,
            0.565192,
        ]
        CORRECT_y_ij_m_df = DataFrame(
            {
                "Q_i^m": CORRECT_Q_i_m,
                "B_j^m": None,
                "exp(-β c_{ij})": None,
                B_j_m_im_column: CORRECT_B_j_m_Q_im_distance,
            },
            index=three_cities_results._ij_m_index,
        )
        CORRECT_national_employment = Series(
            {
                "Agriculture": 422000,
                "Production": 3129000,
                "Construction": 2330000,
                "Distribution, transport, hotels and restaurants": 9036000,
                "Information and communication": 1459000,
                "Financial and insurance": 1114000,
                "Real estate": 589000,
                "Professional and support activities": 6039000,
                "Government, health & education": 8756000,
                "Other services": 1989000,
            },
            dtype="int64",
        )
        CORRECT_X_i_m = DataFrame(
            index=["Leeds", "Liverpool", "Manchester"],
            data={
                "Agriculture": (26718483.412322, 6679620.853081, 50097156.398104),
                "Production": (
                    47659402205.177376,
                    41008498561.840843,
                    30020989453.499519,
                ),
                "Construction": (
                    15610763948.497858,
                    10407175965.665239,
                    11708072961.373394,
                ),
                "Distribution, transport, hotels and restaurants": (
                    50560077467.906143,
                    53408532536.520576,
                    62666011509.517479,
                ),
                "Information and communication": (
                    20341501028.10144,
                    9492700479.780672,
                    18985400959.561344,
                ),
                "Financial and insurance": (
                    48722073608.617584,
                    23200987432.675041,
                    46401974865.350082,
                ),
                "Real estate": (
                    26441434634.974537,
                    29379371816.638374,
                    58758743633.276749,
                ),
                "Professional and support activities": (
                    56027189932.107979,
                    29487994701.109459,
                    70771187282.662704,
                ),
                "Government, health & education": (
                    52047738693.467346,
                    55596448149.840118,
                    59145157606.212898,
                ),
                "Other services": (
                    6780532931.121166,
                    7507018602.31272,
                    8717828054.298643,
                ),
            },
        )
        # CORRECT_X_i_m.index.name = "Area"
        # assert_series_equal(
        #     CORRECT_national_employment, three_cities_results.national_employment
        # )
        assert_series_equal(
            CORRECT_y_ij_m_df["Q_i^m"].astype(float),
            three_cities_results._y_ij_m["Q_i^m"],
        )
        assert_series_equal(
            CORRECT_y_ij_m_df[B_j_m_im_column],
            three_cities_results._y_ij_m[B_j_m_im_column],
        )
        assert_frame_equal(
            CORRECT_X_i_m,
            three_cities_results.X_i_m.astype("float64"),
        )

    # def test_null_raw_io(self, three_cities_results) -> None:
    #     """Test raising null error for raw InputOutput attribute."""
    #     three_cities_results._raw_region_data = None
    #     with pytest.raises(NullRawRegionError):
    #         test_region_data: DataFrame = three_cities_results.region_data

    def test_not_implemented_raw_io(self) -> None:
        """Test raising null error for raw InputOutput attribute."""
        test_io = InterRegionInputOutputUK2017(_raw_region_data=["a", "list"])
        with pytest.raises(NotImplementedError):
            test_region_data: DataFrame = test_io.region_data

    def test_base_io_table(self, three_cities_io) -> None:
        """Test simble structure of the base_io_table attribute"""
        assert (
            three_cities_io.base_io_table.columns.to_list() == three_cities_io.sectors
        )
        assert three_cities_io.base_io_table.index.to_list() == three_cities_io.sectors

    @pytest.mark.xfail(reason="Possible issues with scaling")
    def test_region_io_table(self, three_cities_io) -> None:
        manchester_io = three_cities_io.regional_io_projections["Manchester"]
        AGRICULTURE = Series(
            [
                64259157.962,
                101794281.505,
                5033080.35,
                58680556.187,
                5617135.251,
                21967701.891,
                2029.493,
                26931406.063,
                291842.082,
                425625.329,
            ],
            index=three_cities_io.sectors,
            name="Agriculture",
            dtype=object,
        )
        assert_series_equal(manchester_io["Agriculture"], AGRICULTURE)

    def test_load_cached_results(
        self, three_cities_io, three_cities_results, caplog
    ) -> None:
        three_cities_io._load_convergence_results(
            three_cities_results.e_m_model, three_cities_results.y_ij_m_model
        )
        assert (
            f"{three_cities_io} loaded pre-existing e_m_model and y_ij_m_model results"
            in caplog.text
        )


class TestInputOutputModelAllCities:

    """Test results for 48 cities in England over 10 aggregated sectors."""

    def test_all_city_construction(self, all_cities_io) -> None:
        assert (
            str(all_cities_io)
            == "UK 2017-06-01 Input-Output model: 10 sectors, 50 regions"
        )
        assert (
            repr(all_cities_io)
            == "InterRegionInputOutputUK2017(nation='UK', date='2017-06-01', sectors=10, regions=50)"
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
        for value in CENTRE_FOR_CITIES_NAME_FIX_DICT.values():
            value in all_cities_io.distances.index
