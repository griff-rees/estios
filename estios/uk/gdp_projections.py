#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Use OECD projections as a baseline for scenarios.
"""

from datetime import datetime
from logging import getLogger
from typing import Callable, Final, Generator, Sequence

from pandas import DataFrame, Series, read_csv

from ..sources import MetaData, OECDTermsAndConditions, pandas_from_path_or_package
from ..utils import df_column_to_single_value

# OECD_API_URL: Final[str] = "https://stats.oecd.org/sdmx-json/data"
#
#
# def oecd_query_url(query: str) -> str:

logger = getLogger(__name__)

UK_CURRENCY_ABBREVIATION: Final[str] = "GBR"
PANDAS_REGION_YEAR_QUERY_STR: Final[
    str
] = "LOCATION == @currency_abbrev & TIME == @year"

OECD_DATE_CITATION_FORMAT: Final[str] = "%d %B %y"
VALUE_COLUMN_NAME: Final[str] = "Value"

MAX_CONVERSION_YEAR: Final[int] = 2021
THREE_YEARS: tuple[int, int, int] = (2017, 2020, 2025)


def oecd_query_to_float(
    df: DataFrame,
    year: int,
    query_str: str = PANDAS_REGION_YEAR_QUERY_STR,
    currency_abbrev: str = UK_CURRENCY_ABBREVIATION,
    post_processing: None
    | Callable[[DataFrame, str], DataFrame] = df_column_to_single_value,
    results_column_name: str = VALUE_COLUMN_NAME,
) -> DataFrame | float:
    assert isinstance(year, int)
    assert isinstance(currency_abbrev, str)
    assert "year" and "currency_abbrev" in query_str
    result: DataFrame = df.query(query_str)
    if post_processing:
        return post_processing(result, results_column_name)
    else:
        return result


# @dataclass
# class PandasQueryManager:
#
#     data: DataFrame
#     query_str: str
#     query_wrapper: Callable[..., Any]
#     # kwargs: dict[str, Any] = field(default_factory=dict)
#
#     # def __post_init__(self) -> None:
#     #     for key, val in self.kwargs:
#     #         setattr(self, key, val)
#
#     def __call__(self, **kwargs) -> float:
#         return self.query_wrapper(self._query(), **kwargs)
#
#     def _query(self) -> DataFrame:
#         return self.data.query(self.query_str)
#
#
# @dataclass
# class OECDPandasQueryManager(PandasQueryManager):
#
#     query_str: Final[str] = 'LOCATION == @self.currency_abbrev & TIME == year'
#     query_wrapper: Callable[[DataFrame, str], Any] = df_to_float
#     currency_abbrev: str = UK_CURRENCY_ABBREVIATION


def gen_oecd_cite_str(
    cite_date: datetime, prefix_str: str, time_template: str = OECD_DATE_CITATION_FORMAT
) -> str:
    return f"{prefix_str} (Accessed on {cite_date.strftime(time_template)})"


OECD_PPP_DOI: Final[str] = "10.1787/1290ee5a-en"
OECD_PPP_CITE_PREFIX: Final[
    str
] = f"OECD (2022), Purchasing power parities (PPP) (indicator). doi: {OECD_PPP_DOI}"

OECD_GDP_LONG_TERM_FORECAST_DOI: Final[str] = "10.1787/d927bc18-en"
OECD_GDP_LONG_TERM_CITE_PREFIX: Final[
    str
] = f"OECD (2022), Real GDP long-term forecast (indicator). doi: {OECD_GDP_LONG_TERM_FORECAST_DOI}"

FIRST_YEAR: Final[int] = 1990
LAST_YEAR: Final[int] = 2060
LONG_TERM_FORCAST_URL: Final[str] = (
    "https://stats.oecd.org/sdmx-json/data/DP_LIVE/.GDPLTFORECAST.../OECD"
    "?contentType=csv&detail=code&separator=comma&csv-lang=en"
)
PPP_CONVERTER_URL: Final[str] = (
    "https://stats.oecd.org/sdmx-json/data/DP_LIVE/.PPP.../OECD"
    "?contentType=csv&detail=code&separator=comma&csv-lang=en"
)

OECD_GDP_LONG_TERM_FORCASTS: Final[MetaData] = MetaData(
    name="OECD Real GDP long-term forcast",
    year=2022,
    dates=list(range(FIRST_YEAR, LAST_YEAR + 1)),
    authors="Organisation for Economic Co-operation and Development (OECD)",
    region="UK",
    url=LONG_TERM_FORCAST_URL,
    unit="USD with PPP of 2010",
    doi=OECD_GDP_LONG_TERM_FORECAST_DOI,
    description=(
        "Trend gross domestic product (GDP), including long-term "
        "baseline projections (up to 2060), in real terms. Forecast "
        "is based on an assessment of the economic climate in individual "
        "countries and the world economy, using a combination of "
        "model-based analyses and expert judgement. This indicator is "
        "measured in USD at constant prices and Purchasing Power "
        "Parities (PPPs) of 2010."
    ),
    # path=ONS_UK_2018_FILE_NAME,
    # license=OpenGovernmentLicense,
    path="oecd_long_term_forcasts.csv",
    file_name_from_url=False,
    auto_download=False,
    needs_scaling=False,
    _package_data=True,
    # _save_func=download_and_extract_zip_file,  # type: ignore
    # _save_kwargs=dict(zip_file_path=ONS_UK_2018_FILE_NAME),
    _reader_func=pandas_from_path_or_package,
    _reader_kwargs=dict(reader=read_csv),
    # _reader_kwargs=dict(),
)

# OECD_DATE_CITATION_TEMPLATE: Final[str] = "OECD (2022), Purchasing power parities (PPP) (indicator). doi: 10.1787/1290ee5a-en (Accessed on 14 December 2022)"


def gen_oecd_ppp_citation(date_obtained: datetime) -> str:
    return gen_oecd_cite_str(cite_date=date_obtained, prefix_str=OECD_PPP_CITE_PREFIX)


def gen_oecd_forcast_citation(date_obtained: datetime) -> str:
    return gen_oecd_cite_str(
        cite_date=date_obtained, prefix_str=OECD_GDP_LONG_TERM_CITE_PREFIX
    )


OECD_PPP_CONVERTER: MetaData = MetaData(
    name="OECD Purchasing power parities (PPP)",
    year=2022,
    dates=list(range(FIRST_YEAR, 2022)),
    authors="Organisation for Economic Co-operation and Development (OECD)",
    region="UK",
    url=PPP_CONVERTER_URL,
    unit="USD with PPP of 2010",
    doi=OECD_PPP_DOI,
    description=(
        "Purchasing power parities (PPPs) are the rates of currency "
        "conversion that try to equalise the purchasing power of "
        "different currencies, by eliminating the differences in "
        "price levels between countries. The basket of goods and "
        "services priced is a sample of all those that are part of "
        "final expenditures: final consumption of households and "
        "government, fixed capital formation, and net exports. This "
        "indicator is measured in terms of national currency per US dollar."
    ),
    # path=ONS_UK_2018_FILE_NAME,
    license=OECDTermsAndConditions,
    auto_download=False,
    file_name_from_url=False,
    needs_scaling=False,
    path="oecd_ppp_currency_converter.csv",
    _package_data=True,
    # _save_func=download_and_extract_zip_file,  # type: ignore
    # _save_kwargs=dict(zip_file_path=ONS_UK_2018_FILE_NAME),
    _reader_func=pandas_from_path_or_package,
    _reader_kwargs=dict(reader=read_csv),
)
# OECD_PPP_CONVERTER.cite_as = gen_oecd_ppp_citation(OECD_PPP_CONVERTER.date)


def gdp_projection(
    year: int,
    gdp_df: DataFrame,
    ppp_df: DataFrame,
    approximation_year: int | None = MAX_CONVERSION_YEAR,
    use_approximation_for_missing_years: bool = True,
    use_constant_rate: bool = False,
) -> float:
    if not approximation_year:
        approximation_year = ppp_df["Time"].max()
    assert approximation_year
    converter_rate: float
    if use_constant_rate or (
        use_approximation_for_missing_years and year != approximation_year
    ):
        converter_rate = oecd_query_to_float(ppp_df, year=approximation_year)
        logger.info(
            f"Using {converter_rate} converter rate from "
            f"{approximation_year} for {year}"
        )
    else:
        converter_rate = oecd_query_to_float(ppp_df, year=year)
    gdp_in_dollars: float = oecd_query_to_float(gdp_df, year=year)
    return gdp_in_dollars * converter_rate


def get_uk_gdp_ts(
    years: Sequence = THREE_YEARS,
    gdp_df: DataFrame = OECD_GDP_LONG_TERM_FORCASTS.read(),
    ppp_df: DataFrame = OECD_PPP_CONVERTER.read(),
    # gdp_metadata: MetaData = OECD_GDP_LONG_TERM_FORCASTS,
    # ppp_converter_metadata: MetaData = OECD_PPP_CONVERTER,
    **kwargs,
) -> Generator[tuple[int, float], None, None]:
    # gdp_df: DataFrame = gdp_metadata.read()
    # ppp_df: DataFrame = ppp_converter_metadata.read()
    for year in years:
        yield year, gdp_projection(year=year, gdp_df=gdp_df, ppp_df=ppp_df, **kwargs)


def get_uk_gdp_ts_as_series(
    years: Sequence = THREE_YEARS,
    gdp_df: DataFrame = OECD_GDP_LONG_TERM_FORCASTS.read(),
    ppp_df: DataFrame = OECD_PPP_CONVERTER.read(),
    **kwargs,
) -> Series:
    return Series(
        {
            year: gdp
            for year, gdp in get_uk_gdp_ts(
                years=years, gdp_df=gdp_df, ppp_df=ppp_df, **kwargs
            )
        }
    )
