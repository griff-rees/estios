#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from copy import deepcopy
from logging import getLogger
from string import ascii_uppercase
from typing import Any, Callable, Generator, Sequence

import pytest
from filelock import FileLock
from geopandas import GeoDataFrame
from pandas import DataFrame, Series

from estios.models import InterRegionInputOutput, InterRegionInputOutputTimeSeries
from estios.sources import MetaData, MonthDay
from estios.spatial import GenericRegionsManager
from estios.uk.input_output_tables import InputOutputTableUK2017
from estios.uk.models import InterRegionInputOutputUK2017
from estios.uk.nomis_contemporary_employment import (
    NOMIS_API_KEY,
    NOMIS_LETTER_SECTOR_QUERY_PARAM_DICT,
    NOMIS_SECTOR_EMPLOYMENT_TABLE_CODE,
    APIKeyNomisError,
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


def xdist_session_data_wrapper(
    tmp_path_factory: pytest.TempPathFactory,
    worker_id: str,
    func: Callable[..., Generator | Any],
    is_generator: bool = True,
    include_fixture_path: bool = True,
    *args,
    **kwargs,
) -> Any:
    """A wrapper to ensure `pytest` `session` `fixtures` run once under `xdist`.

    This is derived from the instruction:
    https://pytest-xdist.readthedocs.io/en/latest/how-to.html#making-session-scoped-fixtures-execute-only-once

    Args:
        tmp_path_factory:
            `pytest` temporary path provided from caller needed for
            *at least* saving parallel results
        worker_id:
            a `str` to distinguish which worker called `func`
        func:
            a `Generator` that `yield` the fixture
        is_generator:
            whether the `func` is a `Generator` or a normal function
        include_fixture_path:
            whether to pass the `tmp_path_factory` to `func`

    Yields:
        A `Generator` of fixture data

    Returns:
        A fixture data object
    """
    if include_fixture_path:
        kwargs["tmp_path_factory"] = tmp_path_factory
    if worker_id == "master":
        yield from func(*args, **kwargs)

    # get the temp directory shared by all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent

    fn = root_tmp_dir / "data.json"
    with FileLock(str(fn) + ".lock"):
        if fn.is_file():
            data = json.loads(fn.read_text())
        else:
            data = (
                yield from func(*args, **kwargs)
                if is_generator
                else func(*args, **kwargs)
            )
            fn.write_text(json.dumps(data))
    if is_generator:
        yield data
    else:
        return data


@pytest.fixture
def uk_sector_letter_codes() -> tuple[str, ...]:
    """Return a tuple of uppercase UK sector code `strs`."""
    return tuple(ascii_uppercase[:21])


@pytest.fixture(scope="session")
def three_cities() -> dict[str, str]:
    """Return a `dict` of cities to regions they occupy."""
    return THREE_UK_CITY_REGIONS


@pytest.fixture(scope="session")
def three_city_names(three_cities) -> tuple[str, ...]:
    """The a tuple of the names of the `three_cities` fixtures."""
    return tuple(three_cities.keys())


@pytest.fixture
def ten_sector_aggregation_dict() -> dict[str, Sequence[str]]:
    """Return a `dict` of aggregation names to relevant sectors."""
    return SECTOR_10_CODE_DICT


@pytest.fixture
def ten_sector_aggregation_names() -> tuple[str, ...]:
    """Return a `tuple` of sector aggregation names."""
    return tuple(SECTOR_10_CODE_DICT.keys())


@pytest.fixture
def region_geo_data() -> GeoDataFrame:
    """Return import and return spatial date from Centre for Cities."""
    return load_and_join_centre_for_cities_data()


@pytest.fixture(scope="session")
def three_cities_io(three_cities: dict[str, str]) -> InterRegionInputOutputUK2017:
    """Return an `InterRegionInputOutputUK2017` from `three_cities` fixture."""
    return InterRegionInputOutputUK2017(regions=three_cities)


@pytest.mark.remote_data
@pytest.mark.nomis
@pytest.fixture(scope="session")
def three_cities_results(
    three_cities_io: InterRegionInputOutputUK2017,
    tmp_path_factory: pytest.TempPathFactory,
    worker_id: str,
) -> InterRegionInputOutput:
    """Three cities convergence results fixture."""

    # three_cities_io.import_export_convergence()
    # xdist_session_data_wrapper(
    #     tmp_path_factory=tmp_path_factory,
    #     worker_id=worker_id,
    #     func=
    #
    # )

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
    return InputOutputTableUK2017()


@pytest.fixture
def month_day() -> MonthDay:
    return MonthDay()


def _pop_projection_fixture(tmp_path_factory) -> Generator[MetaData, None, None]:
    """Extract ONS population projection for testing and remove when concluded."""
    pop_projection: MetaData = ONS_ENGLAND_POPULATION_META_DATA
    # pop_projection.auto_download = True
    pop_projection._package_data = False
    pop_projection.set_folder(tmp_path_factory.mktemp("test-session"))
    pop_projection.save_local()
    yield pop_projection
    pop_projection.delete_local()


@pytest.fixture(scope="session")
def pop_projection(tmp_path_factory, worker_id):
    yield from xdist_session_data_wrapper(
        tmp_path_factory=tmp_path_factory,
        worker_id=worker_id,
        func=_pop_projection_fixture,
        is_generator=True,
        include_fixture_path=True,
    )


@pytest.fixture(scope="session")
def english_pop_projections(pop_projection) -> Generator[MetaData, None, None]:
    """Extract ONS population projection as DataFrame."""
    assert isinstance(pop_projection, MetaData)
    yield pop_projection.read()


@pytest.fixture(scope="session")
def uk_pua_manager(tmp_path_factory, worker_id) -> PUASManager:
    puas_manager: Generator[
        PUASManager | GenericRegionsManager, None, None
    ] = xdist_session_data_wrapper(
        tmp_path_factory=tmp_path_factory,
        func=generate_uk_puas,
        worker_id=worker_id,
        is_generator=False,
        include_fixture_path=False,
    )
    if isinstance(puas_manager, Generator):
        assert False
        yield from puas_manager
    else:
        return puas_manager


@pytest.fixture(scope="session")
def working_puas_manager() -> PUASManager:
    return get_working_cities_puas_manager()


@pytest.fixture
def ons_2018_projection(pop_projection, three_cities) -> ONSPopulationProjection:
    return ONSPopulationProjection(regions=three_cities, meta_data=pop_projection)


@pytest.fixture(scope="session")
def york_leeds_bristol() -> list[str]:
    return ["York", "Leeds", "Bristol"]


@pytest.fixture(scope="session")
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
        raise APIKeyNomisError(
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
        raise APIKeyNomisError(
            f"To run these tests a `NOMIS_API_KEY` is required in `.env`"
        )
    return national_employment_query(
        2017, download_path=tmp_path_factory.mktemp("test-nomis"), api_key=api_key
    )


# @pytest.mark.remote_data
# @pytest.mark.nomis
# @pytest.fixture(scope="session")
# def baseline_england_scenario() -> InterRegionInputOutputTimeSeries:

#         (
#             annual_projection_config,
#             first_io_time_point,
#         ) = baseline_england_annual_population_projection_config()
#     return baseline_england_annual_projection()


@pytest.fixture
def correct_uk_ons_X_m_national(three_cities_io) -> Series:
    """Example X_m_national talies for testing."""
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
    """Example I_m_national talies for testing."""
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
    """Example S_m_national talies for testing."""
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
    """Example Leeds talies for testing."""
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
    """Example Leeds talies for testing."""
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
    """Example Leeds talies for testing."""
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
    """Example Liverpool talies for testing."""
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


@pytest.fixture
def correct_three_cities_net_constraints(three_cities_io) -> Series:
    """Correct net_constrinats."""
    return Series(
        [
            -575776500.1558461,
            -81570228.6040039,
            -10193219683.637207,
            7705115207.893555,
            -18357526148.28531,
            -1970161805.208252,
            1611277983.1295166,
            -12741971716.03125,
            4888086242.563377,
            -1613001982.3864212,
            581180638.516919,
            -1051041169.2158203,
            8506352742.499756,
            -1192563557.612915,
            18074416781.87053,
            -569175352.1763611,
            1538901797.2315178,
            6790536394.212891,
            -6670343869.816658,
            -7273559935.526257,
            -5404138.36107254,
            1132611397.8183594,
            1686866941.1357422,
            -6512551650.280273,
            283109366.4147949,
            2539337157.3846436,
            -3150179780.3610535,
            5951435321.818359,
            1782257627.2532806,
            8886561917.912674,
        ],
        index=three_cities_io._i_m_index,
    )


@pytest.fixture
def correct_three_cities_exogenous_i_m(three_cities_io) -> Series:
    """Correct net_constrinats."""
    return Series(
        [
            475275297.49803674,
            2629027936965.446,
            2720291888458.155,
            1414943499027.0188,
            145782219852.89478,
            443683899612.4415,
            -17539174548.47934,
            1723367377100.4407,
            42471898723.38682,
            24461507289.852673,
            686285818.2823073,
            1989476544024.3135,
            1373748906813.3958,
            1073425838631.5372,
            80447520262.31895,
            185120016905.1777,
            -13781460228.055567,
            746244518297.525,
            30130472517.656296,
            18076657412.483974,
            2012615313.1343825,
            8550926253780.018,
            6892911187489.468,
            4323943747647.6636,
            390115006119.21747,
            727139829610.5262,
            -83171713573.1552,
            3872009753751.092,
            109226721548.12585,
            81677900302.91348,
        ],
        index=three_cities_io._i_m_index,
    )


@pytest.fixture
def correct_three_cities_convergence_by_region(three_cities_io) -> Series:
    """Correct net_constrinats."""
    return Series(
        [
            1051051797.6538829,
            2629109507194.05,
            2730485108141.792,
            1407238383819.1252,
            164139746001.18008,
            445654061417.6498,
            -19150452531.608856,
            1736109348816.472,
            37583812480.82344,
            26074509272.239094,
            105105179.76538828,
            1990527585193.5293,
            1365242554070.896,
            1074618402189.1501,
            62373103480.448425,
            185689192257.35406,
            -15320362025.287085,
            739453981903.3121,
            36800816387.47295,
            25350217348.01023,
            2018019451.495455,
            8549793642382.199,
            6891224320548.332,
            4330456299297.944,
            389831896752.8027,
            724600492453.1416,
            -80021533792.79414,
            3866058318429.2734,
            107444463920.87257,
            72791338385.00081,
        ],
        index=three_cities_io._i_m_index,
    )


@pytest.fixture(scope="session")
def correct_three_city_e_i_m_model(three_cities_results) -> DataFrame:
    return three_cities_results.e_m_model


@pytest.fixture(scope="session")
def correct_three_city_y_ij_m_model(three_cities_results) -> DataFrame:
    return three_cities_results.y_ij_m_model


@pytest.fixture
def correct_three_city_y_ij_m(three_cities_results) -> Series:
    return Series(
        [
            187110052.03173688,
            -171707454.7924319,
            2380595430.4575033,
            -13957566.226788364,
            4657647135.474765,
            13894551.508998472,
            195525822.0428764,
            2536461009.587525,
            -782621964.1941453,
            -1258416526.3393195,
            -32577569.16872643,
            402290052.1768232,
            1979351991.4088275,
            -475505472.1741341,
            -510963002.63355726,
            1651376893.3006194,
            -933322642.8213824,
            5575187561.585223,
            2124218926.1281013,
            2650861567.6576333,
            -24117971.448799916,
            23951166.84773069,
            -984029201.7312167,
            1441064321.6112256,
            -1977920848.518567,
            -72804209.53653301,
            155348205.39896077,
            -805622544.7808626,
            1570972046.4403393,
            -69664847.16669689,
            -3263584.3019685945,
            305123022.85524696,
            991446300.6394811,
            -363762796.592353,
            -194513259.6434869,
            689304512.6766819,
            -747993715.5551211,
            2378864573.1835184,
            2083684947.6571276,
            2581836595.341186,
            -467326523.10371447,
            103822751.19287463,
            -5012714636.48337,
            5860587811.602413,
            -12475769731.024534,
            -286712688.69495565,
            818884232.0966249,
            -4250766848.3055334,
            4628853887.863873,
            -201878528.6345359,
            363205931.9276654,
            -564534766.0355452,
            6074307243.627688,
            -43424044.36434833,
            11183681280.011944,
            22840198.491409708,
            826012340.146109,
            5710499536.015272,
            -2261985556.864712,
            -3551751386.726035,
        ],
        index=three_cities_results._ij_m_index,
    )
