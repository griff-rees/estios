#!/usr/bin/env python
# -*- coding: utf-8 -*-

from logging import getLogger
from os import environ
from pathlib import Path
from pprint import pprint
from typing import Callable, Final, Sequence

import ukcensusapi.Nomisweb as census_api
from dotenv import load_dotenv
from pandas import DataFrame

from ..sources import MetaData, OpenGovernmentLicense

logger = getLogger(__name__)


load_dotenv()

NOMIS_API_KEY: str = ""


class APIKeyNommisError(Exception):
    ...


try:
    # Following the KEY as defined in https://github.com/virgesmith/UKCensusAPI/blob/main/ukcensusapi/Nomisweb.py
    NOMIS_API_KEY = environ["NOMIS_API_KEY"]
except KeyError:
    logger.warning("NOMIS token not found in local .env file.")

NOMIS_GEOGRAPHY_CODE_COLUMN_NAME: Final[str] = "GEOGRAPHY_CODE"
NOMIS_GEOGRAPHY_NAME_COLUMN_NAME: Final[str] = "GEOGRAPHY_NAME"
NOMIS_EMPLOYMENT_STATUS_COLUMN_NAME: Final[str] = "EMPLOYMENT_STATUS_NAME"
NOMIS_OBSERVATION_VALUE_COLUMN_NAME: Final[str] = "OBS_VALUE"
NOMIS_MEASURE_TYPE_COLUMN_NAME: Final[str] = "MEASURE_NAME"
NOMIS_INDUSTRY_CODE_COLUMN_NAME: Final[str] = "INDUSTRY_CODE"
NOMIS_EMPLOYMENT_COUNT_VALUE: Final[str] = "Employment"
NOMIS_MEASURE_COUNT_VALUE: Final[str] = "Count"

NOMIS_TOTAL_EMPLOYMENT_TABLE_CODE: Final[str] = "NM_57_1"
NOMIS_SECTOR_EMPLOYMENT_TABLE_CODE: Final[str] = "NM_189_1"

DEFAULT_PATH: Final[Path] = Path("estios/uk/data/nomisweb")

NOMIS_FIRST_YEAR: Final[int] = 1981
NOMIS_LAST_YEAR: Final[int] = 2022
NOMIS_LOCAL_AUTHORITY_LAST_EMPLOYMENT_YEAR: Final[int] = 2021
NOMIS_YEAR_RANGE: tuple[int, ...] = tuple(range(NOMIS_FIRST_YEAR, NOMIS_LAST_YEAR + 1))
NOMIS_LOCAL_AUTHORITY_EMPLOYMENT_YEAR_RANGE: tuple[int, ...] = tuple(
    range(NOMIS_FIRST_YEAR, NOMIS_LOCAL_AUTHORITY_LAST_EMPLOYMENT_YEAR + 1)
)
NOMIS_4_DIGIT_INDUSTRY_CLASSES: Final[str] = (
    "138412132,138412143...138412148,138412151,138412153...138412162,138412173...138412179,138412181,"
    "138412182,138412193...138412196,138412202,138412242,138412252,138412262,138412272,138412343,138412344,"
    "138412353,138412354,138412542,138412552,138412642,138412652,138412742,138412753,138412761,138412843,"
    "138412844,138412923...138412925,138412931,138412942,138413022,138413043...138413045,138413052,138413063,"
    "138413064,138413071,138413073,138413074,138413083,138413084,138413093,138413094,138413103...138413105,"
    "138413113...138413118,138413121,138413123,138413124,138413133...138413139,138413232,138413342,138413352,"
    "138413362,138413423...138413428,138413431,138413443...138413446,138413451,138413452,138413463,138413471,"
    "138413543,138413544,138413552,138413642,138413653...138413656,138413661,138413743,138413744,"
    "138413753...138413756,138413761,138413843...138413846,138413852,138413942,138413952,138414043...138414049,"
    "138414052,138414062,138414073,138414074,138414083...138414085,138414091,138414092,138414142,138414152,"
    "138414243,138414251,138414253...138414255,138414261,138414343...138414346,138414351,138414352,138414363,"
    "138414364,138414373...138414376,138414381,138414383,138414384,138414393...138414397,138414401,138414402,"
    "138414423,138414431,138414442,138414452,138414463...138414466,138414473...138414478,138414483...138414486,"
    "138414543,138414544,138414553,138414561,138414562,138414572,138414582,138414593,138414594,"
    "138414603...138414605,138414623...138414626,138414631,138414643,138414644,138414652,138414662,138414672,"
    "138414683,138414684,138414692,138414702,138414712,138414743,138414744,138414752,138414763...138414765,138414772,"
    "138414783,138414784,138414822,138414843...138414847,138414853...138414857,138414861,138414862,138414873,"
    "138414881,138414923...138414928,138414931,138414942,138414952,138414963,138414964,138415043,138415044,"
    "138415052,138415062,138415072,138415123,138415124,138415131,138415133...138415135,138415141,"
    "138415243...138415245,138415252,138415262,138415272,138415282,138415323,138415331,138415343...138415349,"
    "138415351,138415352,138415543...138415546,138415553...138415555,138415562,138415632,138415732,138415843,"
    "138415844,138415853,138415854,138415863,138415864,138415932,138416142,138416152,138416243...138416245,"
    "138416253,138416254,138416323,138416331,138416343...138416345,138416353,138416354,138416361,"
    "138416363...138416366,138416371,138416423,138416431,138416543,138416551,138416552,138416563,138416564,"
    "138416572,138416643...138416651,138416653...138416656,138416663...138416671,138416673...138416681,138416683,"
    "138416684,138416693...138416698,138416701,138416703...138416709,138416722,138416743,138416751,"
    "138416753...138416758,138416761,138416762,138416773...138416775,138416783...138416786,138416791,"
    "138416793...138416797,138416803...138416811,138416813,138416814,138416821,138416823,138416831,138416942,"
    "138416952,138416963,138416964,138416971,138416973,138416974,138416982,138417042,138417052,138417062,138417072,"
    "138417142,138417153,138417154,138417242,138417253...138417256,138417261,138417342,138417352,138417542,138417552,"
    "138417562,138417622,138417642,138417653,138417661,138417662,138417843...138417846,138417851,138417853,138417861,"
    "138417943...138417946,138417952,138418042,138418052,138418142,138418152,138418162,138418222,138418233...138418235,"
    "138418241,138418343,138418344,138418423,138418431,138418443,138418451,138418452,138418462,138418523,138418524,"
    "138418531,138418543,138418544,138418552,138418562,138418643,138418644,138418651,138418653,138418654,138418661,"
    "138418662,138418842,138418852,138418863,138418864,138418942,138418952,138419042,138419053,138419054,138419143,"
    "138419144,138419152,138419243,138419251,138419252,138419343,138419344,138419352,138419442,138419452,138419462,"
    "138419522,138419532,138419743,138419744,138419753,138419754,138419761,138419763...138419767,138419771,138419772,"
    "138419842,138419852,138419862,138419943,138419944,138420022,138420042,138420052,138420062,138420142,138420153,"
    "138420154,138420161,138420162,138420243,138420251,138420252,138420262,138420323,138420324,138420331,"
    "138420443...138420445,138420453...138420457,138420462,138420542,138420552,138420563,138420564,138420573,138420574,"
    "138420583...138420585,138420591,138420592,138420642,138420653...138420655,138420722,138420742,138420752,138420762,"
    "138420822,138420842,138420923,138420931,138421033...138421036,138421133...138421136,138421232,138421343...138421345,"
    "138421351,138421353,138421361,138421443,138421444,138421452,138421523,138421524,138421531,138421543,138421544,"
    "138421553...138421557,138421561,138421633...138421636,138421641,138421732,138421842,138421852,138421932"
)
URL: Final[str] = "https://www.nomisweb.co.uk"
INFO_URL: Final[str] = "https://www.nomisweb.co.uk/default.asp"

# NOMIS_2017_SECTOR_EMPLOYMENT_METADATA: Final[MetaData] = MetaData(
#     name="NOMIS England City Employment",
#     region="England",
#     path=CITY_SECTOR_EMPLOYMENT_CSV_FILE_NAME,
#     year=2017,
#     auto_download=False,
#     license=OpenGovernmentLicense,
#     _package_data=True,
#     _reader_func=pandas_from_path_or_package,
#     _reader_kwargs=CITY_SECTOR_READ_KWARGS,
# )
# NM_57_1.data.csv?geography=2092957698,1946157057...1946157462&date=latestMINUS4&item=1,3&measures=20100&select=date_name,geography_name,geography_code,item_name,measures_name,obs_value,obs_status_name


# https://www.nomisweb.co.uk/api/v01/dataset/NM_189_1.data.csv?geography=1879048193...1879048572&date=latestMINUS4&industry=138412132,138412143...138412148,138412151,138412153...138412162,138412173...138412179,138412181,138412182,138412193...138412196,138412202,138412242,138412252,138412262,138412272,138412343,138412344,138412353,138412354,138412542,138412552,138412642,138412652,138412742,138412753,138412761,138412843,138412844,138412923...138412925,138412931,138412942,138413022,138413043...138413045,138413052,138413063,138413064,138413071,138413073,138413074,138413083,138413084,138413093,138413094,138413103...138413105,138413113...138413118,138413121,138413123,138413124,138413133...138413139,138413232,138413342,138413352,138413362,138413423...138413428,138413431,138413443...138413446,138413451,138413452,138413463,138413471,138413543,138413544,138413552,138413642,138413653...138413656,138413661,138413743,138413744,138413753...138413756,138413761,138413843...138413846,138413852,138413942,138413952,138414043...138414049,138414052,138414062,138414073,138414074,138414083...138414085,138414091,138414092,138414142,138414152,138414243,138414251,138414253...138414255,138414261,138414343...138414346,138414351,138414352,138414363,138414364,138414373...138414376,138414381,138414383,138414384,138414393...138414397,138414401,138414402,138414423,138414431,138414442,138414452,138414463...138414466,138414473...138414478,138414483...138414486,138414543,138414544,138414553,138414561,138414562,138414572,138414582,138414593,138414594,138414603...138414605,138414623...138414626,138414631,138414643,138414644,138414652,138414662,138414672,138414683,138414684,138414692,138414702,138414712,138414743,138414744,138414752,138414763...138414765,138414772,138414783,138414784,138414822,138414843...138414847,138414853...138414857,138414861,138414862,138414873,138414881,138414923...138414928,138414931,138414942,138414952,138414963,138414964,138415043,138415044,138415052,138415062,138415072,138415123,138415124,138415131,138415133...138415135,138415141,138415243...138415245,138415252,138415262,138415272,138415282,138415323,138415331,138415343...138415349,138415351,138415352,138415543...138415546,138415553...138415555,138415562,138415632,138415732,138415843,138415844,138415853,138415854,138415863,138415864,138415932,138416142,138416152,138416243...138416245,138416253,138416254,138416323,138416331,138416343...138416345,138416353,138416354,138416361,138416363...138416366,138416371,138416423,138416431,138416543,138416551,138416552,138416563,138416564,138416572,138416643...138416651,138416653...138416656,138416663...138416671,138416673...138416681,138416683,138416684,138416693...138416698,138416701,138416703...138416709,138416722,138416743,138416751,138416753...138416758,138416761,138416762,138416773...138416775,138416783...138416786,138416791,138416793...138416797,138416803...138416811,138416813,138416814,138416821,138416823,138416831,138416942,138416952,138416963,138416964,138416971,138416973,138416974,138416982,138417042,138417052,138417062,138417072,138417142,138417153,138417154,138417242,138417253...138417256,138417261,138417342,138417352,138417542,138417552,138417562,138417622,138417642,138417653,138417661,138417662,138417843...138417846,138417851,138417853,138417861,138417943...138417946,138417952,138418042,138418052,138418142,138418152,138418162,138418222,138418233...138418235,138418241,138418343,138418344,138418423,138418431,138418443,138418451,138418452,138418462,138418523,138418524,138418531,138418543,138418544,138418552,138418562,138418643,138418644,138418651,138418653,138418654,138418661,138418662,138418842,138418852,138418863,138418864,138418942,138418952,138419042,138419053,138419054,138419143,138419144,138419152,138419243,138419251,138419252,138419343,138419344,138419352,138419442,138419452,138419462,138419522,138419532,138419743,138419744,138419753,138419754,138419761,138419763...138419767,138419771,138419772,138419842,138419852,138419862,138419943,138419944,138420022,138420042,138420052,138420062,138420142,138420153,138420154,138420161,138420162,138420243,138420251,138420252,138420262,138420323,138420324,138420331,138420443...138420445,138420453...138420457,138420462,138420542,138420552,138420563,138420564,138420573,138420574,138420583...138420585,138420591,138420592,138420642,138420653...138420655,138420722,138420742,138420752,138420762,138420822,138420842,138420923,138420931,138421033...138421036,138421133...138421136,138421232,138421343...138421345,138421351,138421353,138421361,138421443,138421444,138421452,138421523,138421524,138421531,138421543,138421544,138421553...138421557,138421561,138421633...138421636,138421641,138421732,138421842,138421852,138421932&employment_status=1,4&measure=1,2&measures=20100&select=date_name,geography_name,geography_code,industry_name,employment_status_name,measure_name,measures_name,obs_value,obs_status_name
# geography=1879048193...1879048572&
# date=latestMINUS4&
# industry=138412132,138412143...138412148,138412151,138412153...138412162,138412173...138412179,138412181,138412182,138412193...138412196,138412202,138412242,138412252,138412262,138412272,138412343,138412344,138412353,138412354,138412542,138412552,138412642,138412652,138412742,138412753,138412761,138412843,138412844,138412923...138412925,138412931,138412942,138413022,138413043...138413045,138413052,138413063,138413064,138413071,138413073,138413074,138413083,138413084,138413093,138413094,138413103...138413105,138413113...138413118,138413121,138413123,138413124,138413133...138413139,138413232,138413342,138413352,138413362,138413423...138413428,138413431,138413443...138413446,138413451,138413452,138413463,138413471,138413543,138413544,138413552,138413642,138413653...138413656,138413661,138413743,138413744,138413753...138413756,138413761,138413843...138413846,138413852,138413942,138413952,138414043...138414049,138414052,138414062,138414073,138414074,138414083...138414085,138414091,138414092,138414142,138414152,138414243,138414251,138414253...138414255,138414261,138414343...138414346,138414351,138414352,138414363,138414364,138414373...138414376,138414381,138414383,138414384,138414393...138414397,138414401,138414402,138414423,138414431,138414442,138414452,138414463...138414466,138414473...138414478,138414483...138414486,138414543,138414544,138414553,138414561,138414562,138414572,138414582,138414593,138414594,138414603...138414605,138414623...138414626,138414631,138414643,138414644,138414652,138414662,138414672,138414683,138414684,138414692,138414702,138414712,138414743,138414744,138414752,138414763...138414765,138414772,138414783,138414784,138414822,138414843...138414847,138414853...138414857,138414861,138414862,138414873,138414881,138414923...138414928,138414931,138414942,138414952,138414963,138414964,138415043,138415044,138415052,138415062,138415072,138415123,138415124,138415131,138415133...138415135,138415141,138415243...138415245,138415252,138415262,138415272,138415282,138415323,138415331,138415343...138415349,138415351,138415352,138415543...138415546,138415553...138415555,138415562,138415632,138415732,138415843,138415844,138415853,138415854,138415863,138415864,138415932,138416142,138416152,138416243...138416245,138416253,138416254,138416323,138416331,138416343...138416345,138416353,138416354,138416361,138416363...138416366,138416371,138416423,138416431,138416543,138416551,138416552,138416563,138416564,138416572,138416643...138416651,138416653...138416656,138416663...138416671,138416673...138416681,138416683,138416684,138416693...138416698,138416701,138416703...138416709,138416722,138416743,138416751,138416753...138416758,138416761,138416762,138416773...138416775,138416783...138416786,138416791,138416793...138416797,138416803...138416811,138416813,138416814,138416821,138416823,138416831,138416942,138416952,138416963,138416964,138416971,138416973,138416974,138416982,138417042,138417052,138417062,138417072,138417142,138417153,138417154,138417242,138417253...138417256,138417261,138417342,138417352,138417542,138417552,138417562,138417622,138417642,138417653,138417661,138417662,138417843...138417846,138417851,138417853,138417861,138417943...138417946,138417952,138418042,138418052,138418142,138418152,138418162,138418222,138418233...138418235,138418241,138418343,138418344,138418423,138418431,138418443,138418451,138418452,138418462,138418523,138418524,138418531,138418543,138418544,138418552,138418562,138418643,138418644,138418651,138418653,138418654,138418661,138418662,138418842,138418852,138418863,138418864,138418942,138418952,138419042,138419053,138419054,138419143,138419144,138419152,138419243,138419251,138419252,138419343,138419344,138419352,138419442,138419452,138419462,138419522,138419532,138419743,138419744,138419753,138419754,138419761,138419763...138419767,138419771,138419772,138419842,138419852,138419862,138419943,138419944,138420022,138420042,138420052,138420062,138420142,138420153,138420154,138420161,138420162,138420243,138420251,138420252,138420262,138420323,138420324,138420331,138420443...138420445,138420453...138420457,138420462,138420542,138420552,138420563,138420564,138420573,138420574,138420583...138420585,138420591,138420592,138420642,138420653...138420655,138420722,138420742,138420752,138420762,138420822,138420842,138420923,138420931,138421033...138421036,138421133...138421136,138421232,138421343...138421345,138421351,138421353,138421361,138421443,138421444,138421452,138421523,138421524,138421531,138421543,138421544,138421553...138421557,138421561,138421633...138421636,138421641,138421732,138421842,138421852,138421932
# &employment_status=1,4&measure=1,2&measures=20100&select=date_name,geography_name,geography_code,industry_name,employment_status_name,measure_name,measures_name,obs_value,obs_status_name


NOMIS_GEO_PARAM_STR: Final[str] = "geography"
NOMIS_DATE_PARAM_STR: Final[str] = "date"
NOMIS_INDUSTRY_PARAM_STR: Final[str] = "industry"
NOMIS_EMPLOYMENT_STATUS_PARAM_STR: Final[str] = "employment_status"
NOMIS_MEASURE_PARAM_STR: Final[str] = "measure"
NOMIS_MEASURES_PARAM_STR: Final[str] = "measures"
NOMIS_SELECT_PARAM_STR: Final[str] = "select"
NOMIS_SEX_PARAM_STR: Final[str] = "sex"
NOMIS_ITEM_PARAM_STR: Final[str] = "item"

# K02000001 for all UK, but not available in NM_189_1
#                  United Kingdom (not including Northern Ireland), all other local authorities (missing data from Northern Ireland)
NOMIS_LOCAL_AUTHORITY_GEOGRAPHY_CODES_STR: Final[
    str
] = "K03000001,1879048193...1879048572"
NOMIS_INDUSTRY_SECTIONS_BY_LETTER_CODES_STR: Final[str] = "150994945...150994965"
NOMIS_EMPLOYMENT_STATUS_CODES_STR: Final[str] = "1,4"
NOMIS_EMPLOYMENT_MEASURE_CODES_STR: Final[str] = "1,2"
NOMIS_EMPLOYMENT_MEASURES_CODE_STR: Final[str] = "20100"
NOMIS_EMPLOYMENT_SELECT_COLUMNS: Final[
    str
] = "date_name,geography_name,geography_code,industry_name,employment_status_name,measure_name,measures_name,obs_value,obs_status_name,industry_code"

NOMIS_LETTER_SECTOR_QUERY_PARAM_DICT: dict[str, str] = {
    NOMIS_GEO_PARAM_STR: NOMIS_LOCAL_AUTHORITY_GEOGRAPHY_CODES_STR,
    NOMIS_INDUSTRY_PARAM_STR: NOMIS_INDUSTRY_SECTIONS_BY_LETTER_CODES_STR,
    NOMIS_EMPLOYMENT_STATUS_PARAM_STR: NOMIS_EMPLOYMENT_STATUS_CODES_STR,
    NOMIS_MEASURE_PARAM_STR: NOMIS_EMPLOYMENT_MEASURE_CODES_STR,
    NOMIS_MEASURES_PARAM_STR: NOMIS_EMPLOYMENT_MEASURES_CODE_STR,
    NOMIS_SELECT_PARAM_STR: NOMIS_EMPLOYMENT_SELECT_COLUMNS,
}

# https://www.nomisweb.co.uk/api/v01/dataset/NM_131_1.data.csv?geography=2092957699,2092957702,2092957701,2092957697,2092957700&date=latestMINUS21&industry=150994945...150994964&sex=1...4,7&item=1...5&measures=20100&select=date,date_name,geography,geography_name,geography_code,geography_typecode,industry,industry_name,industry_code,industry_typecode,sex_name,item_name,measures_name,obs_value,obs_status_name
# geography=2092957699,2092957702,2092957701,2092957697,2092957700&
# date=latestMINUS21
# &industry=150994945...150994964
# &sex=1...4,7
# &item=1...5
# &measures=20100
# &select=date,date_name,geography,geography_name,geography_code,geography_typecode,industry,industry_name,industry_code,industry_typecode,sex_name,item_name,measures_name,obs_value,obs_status_name

NOMIS_ALL_SEXES_VALUE: Final[str] = "Total"
NOMIS_TOTAL_WORKFORCE_VALUE: Final[str] = "total workforce jobs"
NOMIS_NATIONAL_EMPLOYMENT_TABLE_CODE: Final[str] = "NM_131_1"
NOMIS_NATIONAL_GEOGRAPHY_CODES_STR: Final[
    str
] = "2092957699,2092957702,2092957701,2092957697,2092957700"
NOMIS_NATIONAL_GEOGRAPHY_DATE_STR: Final[str] = "latestMINUS21"
NOMIS_NATIONAL_SEX_CODES_STR: Final[str] = "1...4,7"
NOMIS_NATIONAL_ITEM_CODES_STR: Final[str] = "1...5"
NOMIS_NATIONAL_EMPLOYMENT_SELECT_COLUMNS: Final[
    str
] = "date,date_name,geography,geography_name,geography_code,geography_typecode,industry,industry_name,industry_code,industry_typecode,sex_name,item_name,measures_name,obs_value,obs_status_name"
NOMIS_NATIONAL_LETTER_SECTOR_QUERY_PARAM_DICT: dict[str, str] = {
    NOMIS_GEO_PARAM_STR: NOMIS_NATIONAL_GEOGRAPHY_CODES_STR,
    NOMIS_INDUSTRY_PARAM_STR: NOMIS_INDUSTRY_SECTIONS_BY_LETTER_CODES_STR,
    NOMIS_SEX_PARAM_STR: NOMIS_NATIONAL_SEX_CODES_STR,
    NOMIS_ITEM_PARAM_STR: NOMIS_NATIONAL_ITEM_CODES_STR,
    NOMIS_MEASURES_PARAM_STR: NOMIS_EMPLOYMENT_MEASURES_CODE_STR,
    NOMIS_SELECT_PARAM_STR: NOMIS_NATIONAL_EMPLOYMENT_SELECT_COLUMNS,
}
# https://www.nomisweb.co.uk/api/v01/dataset/NM_189_1.data.csv?
# geography=1879048193...1879048572
# &date=latestMINUS4&industry=150994945...150994965
# &employment_status=1,4&measure=1,2&measures=20100&select=date_name,geography_name,geography_code,industry_name,employment_status_name,measure_name,measures_name,obs_value,obs_status_name
# &measure=1,2&measures=20100&select=date_name,geography_name,geography_code,industry_name,employment_status_name,measure_name,measures_name,obs_value,obs_status_name

NOMIS_UK_QUARTER_STRS: Final[tuple[str, ...]] = (
    "March",
    "June",
    "September",
    "December",
)
NOMIS_LATEST_AVAILABLE_QUARTER_STR: Final[str] = NOMIS_UK_QUARTER_STRS[2]


def gen_date_query(
    year: int = 2017,
    quarter: str = "June",
    default_str: str = "latest",
    modify_str: str = "MINUS",
    valid_quarters: tuple[str, ...] = NOMIS_UK_QUARTER_STRS,
    valid_year_range: tuple[str, ...] = NOMIS_LOCAL_AUTHORITY_EMPLOYMENT_YEAR_RANGE,
    latest_quarter: str = NOMIS_LATEST_AVAILABLE_QUARTER_STR,
) -> str:
    if not quarter:
        return gen_year_query(
            year=year,
            default_str=default_str,
            modify_str=modify_str,
            valid_year_range=valid_year_range,
        )
    else:
        logger.info(
            f"Running `gen_date_query` for {quarter} {year}, assuming quarterly releases."
        )
        if year not in valid_year_range:
            raise ValueError(
                f"`year`: {year} not available within NOMIS `valid_year_range`: {valid_year_range}"
            )
        if year < max(valid_year_range):
            years_prior_to_latest: int = max(valid_year_range) - year
        quarter_difference: int = valid_quarters.index(
            latest_quarter
        ) - valid_quarters.index(quarter)
        if quarter_difference < 0:
            quarter_difference = len(valid_quarters) + quarter_difference
        total_difference: int = (years_prior_to_latest + 1) * len(
            valid_quarters
        ) + quarter_difference
        if total_difference:
            return f"{default_str}{modify_str}{total_difference}"
        else:
            return default_str


def gen_year_query(
    year: int = 2017,
    default_str: str = "latest",
    modify_str: str = "MINUS",
    valid_year_range: tuple[str, ...] = NOMIS_LOCAL_AUTHORITY_EMPLOYMENT_YEAR_RANGE,
) -> str:
    logger.info(f"Running `gen_year_query` for {year}, assuming annual releases.")
    if year not in valid_year_range:
        raise ValueError(
            f"`year`: {year} not available within NOMIS `valid_year_range`: {valid_year_range}"
        )
    if year < max(valid_year_range):
        years_prior_to_latest: int = max(valid_year_range) - year
        return f"{default_str}{modify_str}{years_prior_to_latest}"
    else:
        return default_str


def nomis_query(
    year: int,
    nomis_table_code: str = NOMIS_TOTAL_EMPLOYMENT_TABLE_CODE,
    query_params: dict[str, str] | None = None,
    download_path: Path = DEFAULT_PATH,
    quarter: str | None = None,
    api_key: str | None = None,
    require_api_key: bool = True,
    # simlink_api_key=True,
    date_func: Callable[[int, str, str, tuple[str, ...], ...], str] = gen_year_query,
    valid_years: Sequence[int] = NOMIS_YEAR_RANGE,
    # class_4_industries: str | None = None,
    # default_employment_by_letter_sections_config: bool = False,
) -> DataFrame:
    """Query Nomisweb for Local Authority employment data at `year`."""
    if not year in valid_years:
        raise ValueError(
            f"`year`: {year} not available within NOMIS `valid_years`: {valid_years}"
        )
    api = census_api.Nomisweb(download_path)
    if not api_key and NOMIS_API_KEY:
        api_key = NOMIS_API_KEY
    if require_api_key:
        assert api_key
    if api_key:
        api.key = api_key
    if not query_params:
        logger.info(f"Querying default NOMIS {year} employment status")
        query_params = {}
        # query_params["date"] = "latest"
        # query_params["select"] = "GEOGRAPHY_CODE,INDUSTRY,SEX,ITEM,OBS_VALUE"
        # query_params["INDUSTRY"] = "INDUSTRY"
        # query_params["SEX"] = "7"
        # query_params["ITEM"] = "2"
        # query_params["MEASURES"] = "20100"
        # query_params["geography"] = "2013265921...2013265930"
        # query_parmas["date"] = "latestMINUS21"
        # query_params["cell"] = "403308801...403308810,403309057...403309066,403309313...403309322,403309569...403309578,403309825...403309834,403310081...403310090,403310337...403310346,403310593...403310602,403310849...403310858"
        # query_params["geography"] = "1807745125...1807745129,1807745111,1807745112,1807745130...1807745133,1807745113...1807745115,1807745134...1807745137,1807745116,1807745117,1807745138,1807745118,1807745119,1807745139,1807745120,1807745140,1807745141,1807745121,1807745142,1807745122,1807745143,1807745123,1807745124,1807745039,1807745040,1807745046,1807745047,1807745041,1807745042,1807745045,1807745043,1807745057,1807745056,1807745058,1807745048...1807745051,1807745059,1807745060,1807745052...1807745054,1807745044,1807745055,1807745061"
        # query_params["measures"] = "20100,20701"
        # query_params["select"] = "date_name,geography_name,geography_code,cell_name,measures_name,obs_value,obs_status_name"
        # # https://www.nomisweb.co.uk/api/v01/dataset/NM_100_1.data.csv?&select=date_name,geography_name,geography_code,cell_name,measures_name,obs_value,obs_status_name
        # NM_131_1 = api.get_data(table, query_params)
        query_params["geography"] = "2092957698,1946157057...1946157462"
        # query_params["date"] = "latestMINUS4"
        query_params["ITEM"] = "1,3"
        query_params["MEASURES"] = "20100,404423937...404423945"
        # query_params["measures"] = "20100,20701"
        query_params[
            "select"
        ] = "date_name,geography_name,geography_code,item_name,measures_name,obs_value,obs_status_name"
        # https://www.nomisweb.co.uk/api/v01/dataset/NM_100_1.data.csv?&select=date_name,geography_name,geography_code,cell_name,measures_name,obs_value,obs_status_name
    else:
        logger.info(f"Querying with:\n{pprint(query_params)}")

    query_params["date"] = date_func(year)
    return api.get_data(nomis_table_code, query_params)


def trim_df_for_employment_count(
    df: DataFrame,
    first_column_name: str = NOMIS_MEASURE_TYPE_COLUMN_NAME,
    first_value: str = NOMIS_MEASURE_COUNT_VALUE,
    second_column_name: str = NOMIS_EMPLOYMENT_STATUS_COLUMN_NAME,
    second_value: str = NOMIS_EMPLOYMENT_COUNT_VALUE,
) -> DataFrame:
    """Trim rows to just incude Employment Count rather than Employees Count"""
    return df[
        (df[first_column_name] == first_value)
        & (df[second_column_name] == second_value)
    ]


def clean_nomis_employment_query(
    year: int = 2017,
    sector_employment_df: DataFrame | None = None,
    download_path: Path = DEFAULT_PATH,
    api_key: str | None = None,
    nomis_table_code: str = NOMIS_SECTOR_EMPLOYMENT_TABLE_CODE,
    query_params: dict[str, str] = NOMIS_LETTER_SECTOR_QUERY_PARAM_DICT,
) -> DataFrame:
    """Return cleaned DataFrame with only employment counts per row."""
    if not sector_employment_df:
        sector_employment_df = nomis_query(
            year=year,
            nomis_table_code=nomis_table_code,
            query_params=query_params,
            download_path=download_path,
            api_key=api_key,
        )
    return trim_df_for_employment_count(sector_employment_df)


def national_employment_query(
    year: int = 2017,
    quarter: str = "June",
    sector_employment_df: DataFrame | None = None,
    download_path: Path = DEFAULT_PATH,
    api_key: str | None = None,
    nomis_table_code: str = NOMIS_NATIONAL_EMPLOYMENT_TABLE_CODE,
    query_params: dict[str, str] = NOMIS_NATIONAL_LETTER_SECTOR_QUERY_PARAM_DICT,
    date_func: Callable[[int, str, str, tuple[str, ...]], str] = gen_date_query,
) -> DataFrame:
    """Query wrapper for national level emloyment."""
    if not sector_employment_df:
        sector_employment_df = nomis_query(
            year=year,
            quarter=quarter,
            nomis_table_code=nomis_table_code,
            query_params=query_params,
            download_path=download_path,
            date_func=date_func,
            api_key=api_key,
        )
    return trim_df_for_employment_count(
        sector_employment_df,
        first_column_name="SEX_NAME",
        first_value=NOMIS_ALL_SEXES_VALUE,
        second_column_name="ITEM_NAME",
        second_value=NOMIS_TOTAL_WORKFORCE_VALUE,
    )


NOMIS_METADATA: Final[MetaData] = MetaData(
    name="NOMIS UK Census Data",
    year=2023,
    dates=NOMIS_YEAR_RANGE,
    region="UK",
    url=URL,
    info_url=INFO_URL,
    description=(
        "Nomis is a service provided by Office for National Statistics (ONS),"
        "the UK’s largest independent producer of official statistics. On this"
        "website, we publish statistics related to population, society and the"
        "labour market at national, regional and local levels. These include"
        "data from current and previous censuses."
    ),
    # path=ONS_UK_2018_FILE_NAME,
    license=OpenGovernmentLicense,
    auto_download=True,
    needs_scaling=True,
    _package_data=False,
    # _save_func=download_and_extract_zip_file,  # type: ignore
    # _save_kwargs=dict(zip_file_path=ONS_UK_2018_FILE_NAME),
    _reader_func=nomis_query,
    _reader_kwargs=dict(year=2017),
)