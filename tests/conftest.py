#!/usr/bin/env python
# -*- coding: utf-8 -*-

from copy import deepcopy
from logging import getLogger
from string import ascii_uppercase
from typing import Generator, Sequence

import pytest
from geopandas import GeoDataFrame
from pandas import DataFrame, Series

# from estios.input_output_tables import InputOutputCPATable
from estios.models import InterRegionInputOutput, InterRegionInputOutputTimeSeries
from estios.sources import MetaData, MonthDay
from estios.uk.input_output_tables import InputOutputTableUK2017
from estios.uk.models import InterRegionInputOutputUK2017
from estios.uk.nomis_contemporary_employment import (
    NOMIS_API_KEY,
    NOMIS_LETTER_SECTOR_QUERY_PARAM_DICT,
    NOMIS_SECTOR_EMPLOYMENT_TABLE_CODE,
    APIKeyNommisError,
    clean_nomis_employment_query,
    national_employment_query,
    nomis_query,
)
from estios.uk.ons_employment_2017 import generate_employment_quarterly_dates
from estios.uk.ons_population_projections import (
    ONS_ENGLAND_POPULATION_META_DATA,
    ONS_PROJECTION_YEARS,
    ONSPopulationProjection,
)
from estios.uk.ons_uk_population_history import ONS_UK_POPULATION_HISTORY_META_DATA
from estios.uk.populations import (
    NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA,
    NOMIS_REGIONAL_EMPLOYMENT_2017_METADATA,
)
from estios.uk.regions import (
    TEN_UK_CITY_REGIONS,
    get_all_centre_for_cities_dict,
    load_and_join_centre_for_cities_data,
)
from estios.uk.scenarios import annual_io_time_series_ons_2017
from estios.uk.utils import (
    THREE_UK_CITY_REGIONS,
    PUASManager,
    generate_uk_puas,
    get_working_cities_puas_manager,
    load_contemporary_ons_population,
)
from estios.utils import SECTOR_10_CODE_DICT

logger = getLogger(__name__)


@pytest.fixture
def uk_sector_letter_codes() -> tuple[str, ...]:
    return tuple(ascii_uppercase[:21])


@pytest.fixture(scope="session")
def three_cities() -> dict[str, str]:
    return THREE_UK_CITY_REGIONS


@pytest.fixture(scope="session")
def three_city_names(three_cities) -> tuple[str, ...]:
    return tuple(three_cities.keys())


@pytest.fixture
def ten_sector_aggregation_dict() -> dict[str, Sequence[str]]:
    return SECTOR_10_CODE_DICT


@pytest.fixture
def ten_sector_aggregation_names() -> tuple[str, ...]:
    return tuple(SECTOR_10_CODE_DICT.keys())


@pytest.fixture
def region_geo_data() -> GeoDataFrame:
    return load_and_join_centre_for_cities_data()


@pytest.fixture(scope="session")
def three_cities_io(three_cities: dict[str, str]) -> InterRegionInputOutputUK2017:
    return InterRegionInputOutputUK2017(regions=three_cities)


# @pytest.fixture
# def ten_sector_aggregation_names() -> tuple[str, ...]:
#     return tuple(SECTOR_10_CODE_DICT.keys())


# @pytest.fixture
# def region_geo_data() -> GeoDataFrame:
#     return load_and_join_centre_for_cities_data()


# @pytest.fixture(scope="session")
# def three_cities_io(three_cities: dict[str, str]) -> InterRegionInputOutputUK2017:
#     return InterRegionInputOutputUK2017(regions=three_cities)


@pytest.mark.remote_data
@pytest.mark.nomis
@pytest.fixture(scope="session")
def three_cities_results(
    three_cities_io: InterRegionInputOutputUK2017,
) -> InterRegionInputOutput:
    three_cities_io.import_export_convergence()
    return three_cities_io


@pytest.fixture
def quarterly_2017_employment_dates():
    return generate_employment_quarterly_dates(
        [
            2017,
        ]
    )


@pytest.fixture
def all_cities() -> dict[str, str]:
    return get_all_centre_for_cities_dict()


@pytest.fixture
def all_cities_io(all_cities: dict[str, str]) -> InterRegionInputOutput:
    return InterRegionInputOutputUK2017(regions=all_cities)


@pytest.fixture
def three_cities_2018_2043(three_cities) -> InterRegionInputOutputTimeSeries:
    return annual_io_time_series_ons_2017(
        annual_config=ONS_PROJECTION_YEARS, regions=three_cities
    )


@pytest.fixture
def three_cities_2018_2020(three_cities) -> InterRegionInputOutputTimeSeries:
    return annual_io_time_series_ons_2017(
        annual_config=range(2018, 2021), regions=three_cities
    )


@pytest.fixture
def ons_cpa_io_table() -> InputOutputTableUK2017:
    # =======
    # def ons_cpa_io_table() -> InputOutputCPATable | InputOutputTableUK2017:
    # >>>>>>> origin/uk-model-refactor
    return InputOutputTableUK2017()


@pytest.fixture
def month_day() -> MonthDay:
    return MonthDay()


# @pytest.fixture(scope="session") doesn't seem to speed up...
@pytest.fixture(scope="session")
def pop_projection(tmp_path_factory) -> Generator[MetaData, None, None]:
    """Extract ONS population projection for testing and remove when concluded."""
    pop_projection: MetaData = ONS_ENGLAND_POPULATION_META_DATA
    # pop_projection.auto_download = True
    pop_projection._package_data = False
    pop_projection.set_folder(tmp_path_factory.mktemp("test-session"))
    pop_projection.save_local()
    yield pop_projection
    pop_projection.delete_local()


@pytest.fixture(scope="session")
def english_pop_projections(pop_projection) -> Generator[MetaData, None, None]:
    """Extract ONS population projection as DataFrame."""
    yield pop_projection.read()


@pytest.fixture(scope="session")
def uk_pua_manager() -> PUASManager:
    return generate_uk_puas()


@pytest.fixture(scope="session")
def working_puas_manager() -> PUASManager:
    return get_working_cities_puas_manager()


@pytest.fixture
def ons_2018_projection(pop_projection, three_cities) -> ONSPopulationProjection:
    return ONSPopulationProjection(regions=three_cities, meta_data=pop_projection)


@pytest.fixture
def york_leeds_bristol() -> list[str]:
    return ["York", "Leeds", "Bristol"]


@pytest.fixture
def ons_york_leeds_bristol_projection(
    pop_projection, york_leeds_bristol
) -> ONSPopulationProjection:
    return ONSPopulationProjection(regions=york_leeds_bristol, meta_data=pop_projection)


@pytest.fixture(scope="session")
def pop_history(tmp_path_factory) -> Generator[DataFrame, None, None]:
    """Extract ONS population history to test and remove when concluded."""
    pop_history: MetaData = ONS_UK_POPULATION_HISTORY_META_DATA
    pop_history.set_folder(tmp_path_factory.mktemp("test-session"))
    pop_history.save_local()
    yield pop_history.read()
    pop_history.delete_local()


@pytest.fixture
def pop_recent() -> DataFrame:
    return load_contemporary_ons_population()


@pytest.fixture
def correct_three_cities_pop_2017() -> Series:
    """Three cities population aggregated from PUAs.

    Todo:
        * Worth double checking this aggregation.
    """
    return Series(
        {
            "Leeds": 784846,
            "Liverpool": 640109,
            "Manchester": 2474149,
        }
    )


@pytest.mark.remote_data
@pytest.fixture(scope="session")
def nomis_2017_regional_employment_raw(tmp_path_factory) -> DataFrame:
    return nomis_query(
        2017,
        nomis_table_code=NOMIS_SECTOR_EMPLOYMENT_TABLE_CODE,
        query_params=NOMIS_LETTER_SECTOR_QUERY_PARAM_DICT,
        download_path=tmp_path_factory.mktemp("test-nomis"),
    )


@pytest.mark.remote_data
@pytest.mark.nomis
@pytest.fixture(scope="session")
def nomis_2017_regional_employment_filtered(tmp_path_factory) -> DataFrame:
    try:
        api_key = NOMIS_API_KEY
        assert api_key
    except KeyError:
        raise APIKeyNommisError(
            f"To run these tests a `NOMIS_API_KEY` is required in `.env`"
        )
    return clean_nomis_employment_query(
        2017, download_path=tmp_path_factory.mktemp("test-nomis"), api_key=api_key
    )


@pytest.fixture(scope="session")
def ten_city_names(ten_regions: dict = TEN_UK_CITY_REGIONS) -> tuple[str, ...]:
    return tuple(ten_regions.keys())


@pytest.mark.remote_data
@pytest.mark.nomis
@pytest.fixture(scope="session")
def nomis_2017_10_cities_employment(tmp_path_factory, ten_city_names) -> DataFrame:
    regional_employment_nomis_2017 = deepcopy(NOMIS_REGIONAL_EMPLOYMENT_2017_METADATA)
    regional_employment_nomis_2017.path = "10-cities-test.csv"
    regional_employment_nomis_2017.set_folder(tmp_path_factory.mktemp("test-nomis"))
    regional_employment_nomis_2017._reader_kwargs["region_names"] = ten_city_names
    return regional_employment_nomis_2017.read()


@pytest.mark.remote_data
@pytest.mark.nomis
@pytest.fixture(scope="session")
def nomis_2017_3_cities_employment(tmp_path_factory, three_city_names) -> DataFrame:
    regional_employment_nomis_2017 = deepcopy(NOMIS_REGIONAL_EMPLOYMENT_2017_METADATA)
    regional_employment_nomis_2017.path = "3-cities-test.csv"
    regional_employment_nomis_2017.set_folder(tmp_path_factory.mktemp("test-nomis"))
    regional_employment_nomis_2017._reader_kwargs["region_names"] = three_city_names
    return regional_employment_nomis_2017.read()


@pytest.mark.remote_data
@pytest.mark.nomis
@pytest.fixture(scope="session")
def nomis_2017_national_employment(tmp_path_factory) -> DataFrame:
    national_nomis_2017 = deepcopy(NOMIS_NATIONAL_EMPLOYMENT_2017_METADATA)
    national_nomis_2017.set_folder(tmp_path_factory.mktemp("test-nomis"))
    return national_nomis_2017.read()


@pytest.mark.remote_data
@pytest.mark.nomis
@pytest.fixture(scope="session")
def nomis_2017_nation_employment_table(tmp_path_factory) -> DataFrame:
    try:
        api_key = NOMIS_API_KEY
        assert api_key
    except KeyError:
        raise APIKeyNommisError(
            f"To run these tests a `NOMIS_API_KEY` is required in `.env`"
        )
    return national_employment_query(
        2017, download_path=tmp_path_factory.mktemp("test-nomis"), api_key=api_key
    )


@pytest.fixture
def correct_uk_ons_X_m_national(three_cities_io) -> Series:
    """Example X_m_national talies for testing.

    Todo:
        * Check results currently commented out.
    """
    # return Series(
    #     [
    #         272997328683.096,
    #         5564510362211.610,
    #         2953222552470.090,
    #         6052276697689.360,
    #         1797685549558.830,
    #         2350643301666.180,
    #         3410315660836.110,
    #         4206339454407.310,
    #         4977015624637.480,
    #         933480385688.2780,
    #     ],
    #     index=three_cities_io.sectors,
    # )
    return Series(
        [
            289442470348.79114,
            5818450168143.932,
            3153488889821.8154,
            6323216068010.093,
            1899522593822.1008,
            2913907428507.6763,
            3528332764358.8877,
            4411504653869.348,
            5181479466207.092,
            954735496910.265,
        ],
        index=three_cities_io.sectors,
    )


@pytest.fixture
def correct_uk_ons_I_m_national(three_cities_io) -> Series:
    """Example I_m_national talies for testing.

    Todo:
        * Check results currently commented out.
    """
    # return Series(
    #     [
    #         11829310350.839594,
    #         234754157531.46796,
    #         1623614765544.0493,
    #         125750040000.00002,
    #         394630091779.0043,
    #         0.0,
    #         86780010000.0,
    #         415841120000.0001,
    #         11130020000.0,
    #         -4360010000.0,
    #     ],
    #     index=three_cities_io.sectors,
    # )
    return Series(
        [
            1850000000.0,
            198110000000.0,
            1728440000000.0,
            163120000000.0,
            438970000000.0,
            0.0,
            80870000000.0,
            406090000000.0,
            11640000000.0,
            2320000000.0,
        ],
        index=three_cities_io.sectors,
    )


@pytest.fixture
def correct_uk_ons_S_m_national(three_cities_io) -> Series:
    """Example S_m_national talies for testing.

    Todo:
        * Check results currently commented out.
    """
    # return Series(
    #     [
    #         5219879538.987354,
    #         48726630179.360085,
    #         65923156176.74704,
    #         109917023800.91006,
    #         5657846676.360364,
    #         126155859356.99503,
    #         15693516329.211025,
    #         21591371070.04223,
    #         173752907187.63373,
    #         21733979683.75351,
    #     ],
    #     index=three_cities_io.sectors,
    # )
    return Series(
        [
            5196999131.544732,
            53157445243.01777,
            66320083087.75002,
            119344853583.39317,
            6459042512.359258,
            125403792201.85165,
            13459242236.748169,
            21639857794.041252,
            180650767090.27383,
            22847917119.020103,
        ],
        index=three_cities_io.sectors,
    )


@pytest.fixture
def correct_uk_ons_E_m_national(three_cities_io) -> DataFrame:
    """Example S_m_national talies for testing.

    Todo:
        * Check results currently commented out.
    """
    # return Series(
    #     [
    #         5219879538.987354,
    #         48726630179.360085,
    #         65923156176.74704,
    #         109917023800.91006,
    #         5657846676.360364,
    #         126155859356.99503,
    #         15693516329.211025,
    #         21591371070.04223,
    #         173752907187.63373,
    #         21733979683.75351,
    #     ],
    #     index=three_cities_io.sectors,
    # )
    return DataFrame(
        {
            "Exports to EU": [
                16290000000.0,
                1009680000000.0,
                0.0,
                261910000000.0,
                15480000000.0,
                0.0,
                0.0,
                260000000.0,
                0.0,
                160000000.0,
            ],
            "Exports outside EU": [
                6690000000.0,
                936150000000.0,
                0.0,
                237590000000.0,
                13800000000.0,
                0.0,
                0.0,
                70000000.0,
                0.0,
                48090000000.0,
            ],
            "Exports of services": [
                1740000000.0,
                111400000000.0,
                27190000000.0,
                431360000000.0,
                376810000000.0,
                873470000000.0,
                20010000000.0,
                1113280000000.0,
                114630000000.0,
                23610000000.0,
            ],
        },
        index=three_cities_io.sector_names,
    )


@pytest.fixture
def correct_uk_gva_2017(three_cities_io) -> Series:
    """Example G_m_national talies for testing.

    Todo:
        * Check results currently commented out.
        * Checking column aggregation decimal points
    """
    # return Series(
    #     [
    #         107415864611.57884,
    #         2440789852881.2056,
    #         1202628202044.299,
    #         3390433769651.8784,
    #         1131618902061.9211,
    #         1162302626594.8616,
    #         2597532933265.886,
    #         2551229010509.971,
    #         3229760696230.9946,
    #         626388142147.4033,
    #     ],
    #     index=three_cities_io.sectors,
    # )
    return Series(
        [
            114478795080.39958,
            2523446181797.0215,
            1239259094875.971,
            3489714816208.268,
            1150298982361.4224,
            1536869889008.9902,
            2733935955247.9546,
            2623311335721.1494,
            3373526621690.34,
            641348328008.4843,
        ],
        index=three_cities_io.sectors,
    )


@pytest.fixture
def correct_uk_national_employment_2017(three_cities_io) -> Series:
    """Example national employment talies for testing.

    Todo:
        * Check results currently commented out.
    """
    return Series(
        [
            422000,
            3129000,
            2330000,
            9036000,
            1459000,
            1114000,
            589000,
            6039000,
            8756000,
            1989000,
        ],
        dtype="int64",
        index=three_cities_io.sectors,
    )


@pytest.fixture
def correct_leeds_2017_final_demand(three_cities_io) -> DataFrame:
    """Example Leeds talies for testing.

    Todo:
        * Check results currently commented out.
    """
    # return DataFrame(
    #     {
    #         "Household Purchase": [
    #             863824080.1922507,
    #             12910688415.722689,
    #             195078434.4977476,
    #             32846833836.39243,
    #             3897179528.8352375,
    #             7845761753.233474,
    #             35315069651.356255,
    #             3942577124.0064607,
    #             6979808493.090483,
    #             7964555182.937662,
    #         ],
    #         "Government Purchase": [
    #             243.9047716360901,
    #             1259025829.250631,
    #             244.6169027669075,
    #             377444862.3125664,
    #             405085838.6851054,
    #             0.0,
    #             244.616904630556,
    #             244.616904630556,
    #             44086204312.41963,
    #             487766352.4502333,
    #         ],
    #         "Non-profit Purchase": [
    #             121.95238581804504,
    #             122.308452315278,
    #             122.30845138345376,
    #             489.233809261112,
    #             122.308452315278,
    #             0.0,
    #             157411100.4382151,
    #             308217544.45140517,
    #             4444806206.326068,
    #             892607207.3053511,
    #         ],
    #     },
    #     index=three_cities_io.sector_names,
    # )
    return DataFrame(
        {
            "Household Purchase": [
                1004886244.222324,
                14097639139.215887,
                187988091.20858228,
                34602039627.610664,
                3858342436.7377596,
                9566110980.934837,
                36370986773.44653,
                4263550339.258276,
                7201643980.775884,
                7973288006.432973,
            ],
            "Government Purchase": [
                0.0,
                1989836210.7172577,
                0.0,
                342708283.387409,
                401783265.8556882,
                0.0,
                0.0,
                0.0,
                45584971720.16569,
                487154565.57175225,
            ],
            "Non-profit Purchase": [
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                137719317.30700302,
                342096741.12583256,
                5103075555.950344,
                883067025.7163072,
            ],
        },
        index=three_cities_io.sector_names,
    )


@pytest.fixture
def correct_leeds_2017_exports(three_cities_io) -> DataFrame:
    """Example Leeds talies for testing.

    Todo:
        * Check results currently commented out.
    """
    # return DataFrame(
    #     {
    #         "Exports to EU": [
    #             54415828.56237425,
    #             9809992044.03674,
    #             90.43701681873807,
    #             3470227876.1839767,
    #             176664183.2225929,
    #             0.0,
    #             126.2102663036619,
    #             4525463.007579512,
    #             246.01207030888295,
    #             1290488.185247481,
    #         ],
    #         "Exports outside EU": [
    #             21126522.775823142,
    #             9326960784.934875,
    #             90.43701681873807,
    #             3276861858.4626465,
    #             164621905.56472328,
    #             0.0,
    #             126.2102663036619,
    #             3439395.3284675935,
    #             246.01207030888295,
    #             398456621.4053595,
    #         ],
    #         "Exports of services": [
    #             5817935.754843496,
    #             1206046904.8668356,
    #             221477270.769351,
    #             5254272633.270587,
    #             4607163894.68771,
    #             19031456130.250244,
    #             215314966.7345798,
    #             17394106071.604374,
    #             1335232848.7161303,
    #             202634018.24060616,
    #         ],
    #     },
    #     index=three_cities_io.sector_names,
    # )
    return DataFrame(
        {
            "Exports to EU": [
                50253457.5859584,
                10919631206.432209,
                0.0,
                3155420168.3111267,
                204851052.90529874,
                0.0,
                0.0,
                4706293.276151647,
                0.0,
                1469699.6913630648,
            ],
            "Exports outside EU": [
                20638160.297732454,
                10124408479.816885,
                0.0,
                2862419448.623728,
                182619155.69077018,
                0.0,
                0.0,
                1267078.9589639048,
                0.0,
                441736613.48531115,
            ],
            "Exports of services": [
                5367772.633490953,
                1204784601.454469,
                245898250.60355943,
                5196907501.823861,
                4986429279.408631,
                21051133396.70893,
                252546742.87362745,
                20151623763.361942,
                1410018180.9753628,
                216872560.70676225,
            ],
        },
        index=three_cities_io.sector_names,
    )


@pytest.fixture
def correct_leeds_2017_imports(three_cities_io) -> Series:
    """Example Leeds talies for testing.

    Todo:
        * Check results currently commented out.
    """
    # return Series(
    #     [
    #         108642578.11956923,
    #         14716026243.967617,
    #         952384699.312282,
    #         4676870989.681791,
    #         2212125101.231273,
    #         2861365859.7478576,
    #         618566915.0002943,
    #         3004024570.375315,
    #         2467381922.9378333,
    #         364963704.4151428,
    #     ],
    #     index=three_cities_io.sector_names,
    #     name="Imports",
    # )
    return Series(
        [
            112248945.77169241,
            16024973674.257639,
            1209521863.3633444,
            5184214151.395373,
            2319792690.4272084,
            3135164009.7221417,
            604659177.8010114,
            3642778023.002527,
            2743629731.074762,
            362210403.89915055,
        ],
        index=three_cities_io.sector_names,
        name="Imports",
    )


@pytest.fixture
def correct_liverpool_2017_letter_sector_employment() -> Series:
    """Example Liverpool talies for testing.

    Todo:
        * Check results currently commented out.
    """
    return Series(
        {
            "A": 125.0,
            "B": 20.0,
            "C": 23000.0,
            "D": 550.0,
            "E": 1850.0,
            "F": 10500.0,
            "G": 44000.0,
            "H": 17000.0,
            "I": 23000.0,
            "J": 7600.0,
            "K": 11250.0,
            "L": 5600.0,
            "M": 20000.0,
            "N": 26000.0,
            "O": 18750.0,
            "P": 30000.0,
            "Q": 57000.0,
            "R": 12000.0,
            "S": 5500.0,
            "T": 0.0,
            "U": 0.0,
        },
        name="Liverpool",
    )


@pytest.fixture
def correct_agg_uk_nation_final_demand(three_cities_io) -> DataFrame:
    """Correct Final Demand columns aggregated from ONS IO table."""
    return DataFrame(
        {
            "Household Purchase": [
                82160000000.0,
                1152630000000.0,
                15370000000.0,
                2829080000000.0,
                315460000000.0,
                782130000000.0,
                2973710000000.0,
                348590000000.0,
                588810000000.0,
                651900000000.0,
            ],
            "Government Purchase": [
                0.0,
                162690000000.0,
                0.0,
                28020000000.0,
                32850000000.0,
                0.0,
                0.0,
                0.0,
                3727050000000.0,
                39830000000.0,
            ],
            "Non-profit Purchase": [
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                0.0,
                11260000000.0,
                27970000000.0,
                417230000000.0,
                72200000000.0,
            ],
        },
        index=three_cities_io.sectors,
    )
