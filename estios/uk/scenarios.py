#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import OrderedDict
from datetime import date
from logging import getLogger
from typing import Final, Sequence, Type

from pandas import DataFrame, Series

from ..calc import calc_ratio
from ..models import InterRegionInputOutput, InterRegionInputOutputTimeSeries
from ..sources import MetaData, MonthDay
from ..temporal import annual_io_time_series, date_io_time_series
from ..utils import (
    AnnualConfigType,
    DateConfigType,
    RegionsIterableType,
    gen_filter_by_func,
)
from .gdp_projections import get_uk_gdp_ts_as_series
from .models import InterRegionInputOutputUK2017
from .ons_employment_2017 import EMPLOYMENT_QUARTER_JUN_2017
from .ons_population_estimates import (
    ONS_2017_ALL_AGES_COLUMN_NAME,
    ONS_2017_POPULATION_META_DATA,
    ONS_CONTEMPORARY_POPULATION_META_DATA,
)
from .ons_population_projections import (
    NATIONAL_RETIREMENT_AGE,
    ONS_ENGLAND_POPULATION_META_DATA,
    WORKING_AGE_MINIMUM,
)
from .ons_uk_population_history import ONS_UK_POPULATION_HISTORY_META_DATA
from .ons_uk_population_projections import (
    get_uk_pop_scaled_all_ages_ts,
    get_uk_pop_scaled_working_ages_ts,
)
from .populations import regional_population_projections_all_ages
from .utils import (
    THREE_UK_CITY_REGIONS,
    GenericRegionsManager,
    generate_uk_puas,
    sum_for_regions_by_la_code,
)

logger = getLogger(__name__)


# To fit mid year annual population estimates
DEFAULT_MIDYEAR_MONTH_DAY: Final[MonthDay] = MonthDay(month=6)

WORKING_AGE_LIST = list(range(WORKING_AGE_MINIMUM, NATIONAL_RETIREMENT_AGE + 1))
# WORKING_AGE_LIST_STR = [str(a) for a in WORKING_AGE_LIST]

REGION_CODES: dict[str, str] = {
    "UK": "K02000001",
    "Leeds": "E08000035",
    "Liverpool": "E11000002",
    "Manchester": "E11000001",
}

# for meta_source in :
#     if not meta_source.is_local:
#         meta_source.save_local()
# ppp_df = ppp_converter_metadata.read()
# rates:

# converter_rate: float = OECDPandasQueryManager(ppp_df, year=2017)
# converter_rate: float = oecd_query_to_float(ppp_df, year=2017)
# gdp_in_dollars: float = oecd_query_to_float(gdp_df, year=2017)
# assert False
TWO_YEARS: tuple[int, int] = (2020, 2025)


def annual_io_time_series_ons_2017(
    annual_config: AnnualConfigType, **kwargs
) -> InterRegionInputOutputTimeSeries:
    return annual_io_time_series(
        annual_config=annual_config,
        input_output_model_cls=InterRegionInputOutputUK2017,
        **kwargs,
    )


def date_io_time_series_ons_2017(
    date_conf: DateConfigType, **kwargs
) -> InterRegionInputOutputTimeSeries:
    return date_io_time_series(
        date_conf=date_conf,
        input_output_model_cls=InterRegionInputOutputUK2017,
        **kwargs,
    )


def baseline_england_annual_population_projection_config(
    first_io_time: InterRegionInputOutput | None = None,
    date_check: date | None = None,  # Bowan's suggestion
    regions: RegionsIterableType = list(THREE_UK_CITY_REGIONS.keys()),
    ons_population_projection: MetaData = ONS_ENGLAND_POPULATION_META_DATA,
    ons_population_history: MetaData = ONS_UK_POPULATION_HISTORY_META_DATA,
    ons_contemporary_populations: MetaData = ONS_CONTEMPORARY_POPULATION_META_DATA,
    ons_2017_pop_meta_data: MetaData = ONS_2017_POPULATION_META_DATA,
    uk_regions: GenericRegionsManager | None = None,
    month_day: MonthDay | None = None,
    years: Sequence[int] | None = TWO_YEARS,
    working_age_columns: Sequence[int] = WORKING_AGE_LIST,
    all_ages_column: str = ONS_2017_ALL_AGES_COLUMN_NAME,
    national_gdp_projections: Series | None = None,
    regional_pop_projections: DataFrame | None = None,
) -> tuple[AnnualConfigType, InterRegionInputOutput]:
    """Generate a baseline configuration for England time series projection.

    Todo:
        * Check the point in the year defaulted to reflect mid-year population checks
        * Make the function names for national population projections clearer
    """
    # def sum_by_ind_col(index: str | int, df: DataFrame, columns: list[str|int]) -> float:
    #     """Return sums of DataFrame df grouped by passed indexs and columns."""
    #     return df.loc[index][columns].sum()

    if not month_day:
        month_day = MonthDay(month=6)
    if not ons_population_history.is_local:
        ons_population_history.save_local()
    if not ons_2017_pop_meta_data.is_local:
        ons_2017_pop_meta_data.save_local()
    if not ons_contemporary_populations.is_local:
        ons_contemporary_populations.save_local()
    if not uk_regions:
        uk_regions = generate_uk_puas()

    ons_2017_pop_df = ons_2017_pop_meta_data.read()
    assert ons_2017_pop_df is not None
    if not regional_pop_projections:
        if not ons_population_projection.is_local:
            ons_population_projection.save_local()
        regional_pop_projections = ons_population_projection.read()
    if not national_gdp_projections:
        national_gdp_projections = get_uk_gdp_ts_as_series()

    # regional_pop_projections = ONSPopulationProjection(
    #     regions=regions,
    #     meta_data=ons_population_projection,
    #     # ons_path=ons_population_projection.path,
    #     age_projections=ons_population_projection.read(),
    # )
    assert isinstance(regional_pop_projections, DataFrame)
    if not years:
        years = list(
            int(x) for x in gen_filter_by_func(regional_pop_projections.columns)
        )
    scaled_national_at_working_ages: Series = get_uk_pop_scaled_working_ages_ts()
    scaled_national_all_ages: Series = get_uk_pop_scaled_all_ages_ts()

    historical_national_population: DataFrame = ons_population_history.read()
    if not first_io_time and not date_check:
        # national_poulation source: https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/bulletins/annualmidyearpopulationestimates/mid2017
        # working population source: https://www.ons.gov.uk/employmentandlabourmarket/peopleinwork/employmentandemployeetypes/timeseries/lf2o/lms
        first_io_time = InterRegionInputOutputUK2017(
            regions=regions,
            employment_date=EMPLOYMENT_QUARTER_JUN_2017,
        )
        date_check = first_io_time.date

        first_national_population: float = historical_national_population["UKPOP"][
            first_io_time.year
        ]
        # first_io_time.national_population = 66040229  # https://statswales.gov.wales/catalogue/population-and-migration/population/estimates/nationallevelpopulationestimates-by-year-age-ukcountry
        first_io_time.national_population = first_national_population  # https://statswales.gov.wales/catalogue/population-and-migration/population/estimates/nationallevelpopulationestimates-by-year-age-ukcountry

    if first_io_time is None or date_check is None:
        raise ValueError(
            f"Cannot calculate if either "
            f"first_io_time: {first_io_time} "
            f"or date_check: {date_check} are None"
        )
    try:
        assert first_io_time.date == date_check
    except AssertionError:
        raise AssertionError(
            f"{first_io_time} has a different date " f"from date_check {date_check}"
        )

    annual_config: OrderedDict[int, dict] = OrderedDict()
    last_national_at_working_age: float = ons_2017_pop_df.loc[
        REGION_CODES["UK"], working_age_columns
    ].sum()

    last_regional_at_working_ages: Series = Series(
        sum_for_regions_by_la_code(
            df=ons_2017_pop_df,
            region_names=regions,
            column_names=working_age_columns,
            regions=uk_regions,
        )
    )
    last_national_employment_projection: DataFrame = first_io_time.national_employment
    last_regional_employment_projection: DataFrame = first_io_time.employment_table

    first_io_time.national_working_population = last_national_at_working_age
    first_io_time.regional_working_populations = last_regional_at_working_ages

    first_io_time.regional_populations = Series(
        sum_for_regions_by_la_code(
            df=ons_2017_pop_df,
            region_names=regions,
            column_names=all_ages_column,
            regions=uk_regions,
        )
    )

    for year in years:
        new_year_str: str = str(year)
        new_national_population = scaled_national_all_ages[new_year_str]
        new_national_working_age_pop = scaled_national_at_working_ages[new_year_str]
        new_national_employment_projection = calc_ratio(
            last_national_employment_projection,  # previous time point
            last_national_at_working_age,  # previous time point
            scaled_national_at_working_ages[new_year_str],  # next time point
        )

        new_regional_populations: Series = regional_population_projections_all_ages(
            year=year,
            population_projections_df=regional_pop_projections,
            region_names=regions,
            regions_manager=uk_regions,
        )
        new_regional_at_working_ages: Series = regional_population_projections_all_ages(
            year=year,
            population_projections_df=regional_pop_projections,
            region_names=regions,
            regions_manager=uk_regions,
        )
        # new_regional_populations: Series = Series(
        #     sum_for_regions_by_attr(
        #         df=regional_pop_projections.full_population_projections,
        #         region_names=regions,
        #         column_names=[new_year_str],
        #         regions=uk_regions,
        #         attr="la_names",
        #     )
        # )

        # new_regional_at_working_ages: Series = Series(
        #     sum_for_regions_by_attr(
        #         df=regional_pop_projections.working_age_projections,
        #         region_names=regions,
        #         column_names=[new_year_str],
        #         regions=uk_regions,
        #         attr="la_names",
        #     )
        # )

        new_regional_employment_table = calc_ratio(
            last_regional_employment_projection,
            last_regional_at_working_ages,
            new_regional_at_working_ages,
        )
        last_regional_at_working_ages = new_regional_at_working_ages

        annual_config[year] = dict(
            # regional_populations = ,
            national_population=new_national_population,
            national_working_population=new_national_working_age_pop,
            national_employment=new_national_employment_projection,
            regional_employment=new_regional_employment_table,
            national_employment_scale=1.0,
            regions=regions,
            employment_date=month_day.from_year(year),
            regional_populations=new_regional_populations,
            regional_working_populations=new_regional_at_working_ages,
            # regional_employtment = regional_pop_projections.working_age_projections[str(year)],
            # employment_date = ,
            # raw_io_table = national_raw_io_table,
            # sector_aggregation=None,
            # national_employment_scale=1,
            # raw_sectors=first_io_time.sectors,
        )
    return annual_config, first_io_time


def baseline_england_annual_projection(
    first_io_time: InterRegionInputOutput | None = None,
    date_check: date | None = None,  # Bowan's suggestion
    regions: list[str] = list(THREE_UK_CITY_REGIONS.keys()),
    # regions: dict | list[str] = THREE_UK_CITY_REGIONS,
    # ons_population_projection: MetaData = ONS_ENGLAND_POPULATION_META_DATA,
    years: Sequence[int] | None = TWO_YEARS,
    input_output_model_cls: Type[InterRegionInputOutput] = InterRegionInputOutputUK2017,
) -> InterRegionInputOutputTimeSeries:
    time_series_config, first_io = baseline_england_annual_population_projection_config(
        first_io_time=first_io_time, date_check=date_check, regions=regions, years=years
    )
    io_time_series: InterRegionInputOutputTimeSeries = annual_io_time_series(
        time_series_config,
        default_month_day=DEFAULT_MIDYEAR_MONTH_DAY,
        input_output_model_cls=input_output_model_cls,
    )
    io_time_series.insert(0, first_io)
    return io_time_series
