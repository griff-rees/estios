from datetime import date
from logging import INFO

import pytest

from estios.models import InterRegionInputOutputTimeSeries
from estios.temporal import annual_io_time_series, date_io_time_series
from estios.uk.models import InterRegionInputOutputUK2017
from estios.uk.ons_employment_2017 import EMPLOYMENT_QUARTER_DEC_2017
from estios.uk.ons_population_projections import (
    FIRST_YEAR,
    LAST_YEAR,
    ONS_PROJECTION_YEARS,
)
from estios.uk.scenarios import (
    annual_io_time_series_ons_2017,
    baseline_england_annual_population_projection_config,
    baseline_england_annual_projection,
    date_io_time_series_ons_2017,
)


class TestAnnualProjection:

    """Test examples of annual Input-Output models."""

    # TEST_REGIONS: Final[list[str]] = ["York", "Leeds", "Bristol"]

    def test_annual_ons_projection(self) -> None:
        projections: InterRegionInputOutputTimeSeries = annual_io_time_series(
            annual_config=ONS_PROJECTION_YEARS,
            input_output_model_cls=InterRegionInputOutputUK2017,
        )
        assert (
            str(projections)
            == "26 Annual Spatial Input-Output models from 2018 to 2043: 10 sectors, 10 regions"
        )

    def test_annual_ons_2017_projection(self) -> None:
        projections: InterRegionInputOutputTimeSeries = annual_io_time_series_ons_2017(
            annual_config=ONS_PROJECTION_YEARS
        )
        assert (
            str(projections)
            == "26 Annual Spatial Input-Output models from 2018 to 2043: 10 sectors, 10 regions"
        )

    @pytest.mark.remote_data
    def test_annual_projection_config_default(self) -> None:
        (
            annual_projection_config,
            first_io_time_point,
        ) = baseline_england_annual_population_projection_config()
        assert isinstance(annual_projection_config, dict)
        assert tuple(annual_projection_config.keys()) == (2020, 2025)
        assert first_io_time_point.year == 2017

    @pytest.mark.remote_data
    def test_annual_projection_config_years_none(self) -> None:
        (
            annual_projection_config,
            first_io_time_point,
        ) = baseline_england_annual_population_projection_config(years=None)
        assert len(annual_projection_config) == 26
        assert first_io_time_point.year == 2017

    @pytest.mark.remote_data
    def test_baseline_annual_projection_default(self) -> None:
        io_model_ts: InterRegionInputOutputTimeSeries = (
            baseline_england_annual_projection()
        )
        assert len(io_model_ts.national_employment_ts) == 3
        assert (
            str(io_model_ts)
            == "3 Annual Spatial Input-Output models from 2017 to 2025: 10 sectors, 3 regions"
        )

    @pytest.mark.remote_data
    def test_baseline_annual_projection_years_none(self) -> None:
        io_model_ts: InterRegionInputOutputTimeSeries = (
            baseline_england_annual_projection(years=None)
        )
        assert (
            str(io_model_ts)
            == "27 Annual Spatial Input-Output models from 2017 to 2043: 10 sectors, 3 regions"
        )
        assert len(io_model_ts.national_employment_ts) == 27


class TestInputOutputTimeSeries:

    """Test a TimeSeries of InputOutput models."""

    def test_one_time_point(self) -> None:
        employment_date_list = [
            EMPLOYMENT_QUARTER_DEC_2017,
        ]
        time_series = date_io_time_series(
            date_conf=employment_date_list,
            input_output_model_cls=InterRegionInputOutputUK2017,
        )
        assert len(time_series) == 1
        assert (
            str(time_series)
            == "1 Spatial Input-Output model from 2017-12-01 to 2017-12-01: 10 sectors, 10 regions"
        )

    def test_one_time_point_preset_io_model_cls(self) -> None:
        employment_date_list = [
            EMPLOYMENT_QUARTER_DEC_2017,
        ]
        time_series = date_io_time_series_ons_2017(date_conf=employment_date_list)
        assert len(time_series) == 1
        assert (
            str(time_series)
            == "1 Spatial Input-Output model from 2017-12-01 to 2017-12-01: 10 sectors, 10 regions"
        )

    def test_two_dupe_time_points(self) -> None:
        employment_date_list = [
            EMPLOYMENT_QUARTER_DEC_2017,
            EMPLOYMENT_QUARTER_DEC_2017,
        ]
        time_series = date_io_time_series_ons_2017(date_conf=employment_date_list)
        assert len(time_series) == 2
        assert len(time_series[1:]) == 1

    def test_two_dupe_time_points_append(self, three_cities_io) -> None:
        employment_date_list = [
            EMPLOYMENT_QUARTER_DEC_2017,
            EMPLOYMENT_QUARTER_DEC_2017,
        ]
        time_series = date_io_time_series_ons_2017(date_conf=employment_date_list)
        time_series.append(three_cities_io)
        assert len(time_series) == 3
        assert len(time_series[1:]) == 2
        assert list(time_series.years) == 3 * [EMPLOYMENT_QUARTER_DEC_2017.year]

    def test_pass_1_io_model(self, three_cities_io, caplog) -> None:
        caplog.set_level(INFO)
        time_series = InterRegionInputOutputTimeSeries(
            io_models=[
                three_cities_io,
            ]
        )
        assert len(time_series) == 1
        assert list(time_series.years) == [EMPLOYMENT_QUARTER_DEC_2017.year]
        assert str(time_series) == (
            "1 Spatial Input-Output model from 2017-12-01 to 2017-12-01: 10 sectors, 3 regions"
        )

    def test_pass_dupe_io_model(self, three_cities_io, caplog) -> None:
        with caplog.at_level(INFO):
            time_series = InterRegionInputOutputTimeSeries(
                io_models=[three_cities_io, three_cities_io]
            )
            assert "Duplicate(s) of {'2017-12-01': 2}" in caplog.messages
        assert len(time_series) == 2
        assert list(time_series.years) == [
            EMPLOYMENT_QUARTER_DEC_2017.year,
            EMPLOYMENT_QUARTER_DEC_2017.year,
        ]
        assert str(time_series) == (
            "2 Spatial Input-Output models from 2017-12-01 to 2017-12-01: 10 sectors, 3 regions"
        )

    def test_2017_quarters(
        self, quarterly_2017_employment_dates, three_cities, caplog
    ) -> None:
        caplog.set_level(INFO)
        config_dict = {
            date: {"employment_date": date} for date in quarterly_2017_employment_dates
        }
        time_series = date_io_time_series_ons_2017(
            date_conf=config_dict, regions=three_cities
        )
        assert time_series[0].date == date(2017, 3, 1)
        assert time_series.dates.index(time_series[0].date) == 0
        assert len(time_series) == 4
        assert str(time_series) == (
            f"4 Spatial Input-Output models from 2017-03-01 to "
            f"2017-12-01: 10 sectors, 3 regions"
        )
        assert repr(time_series) == (
            "InterRegionInputOutputTimeSeries(dates=4, "
            "start=2017-03-01, end=2017-12-01, sectors=10, "
            "regions=3)"
        )
        time_series.calc_models()
        for model in time_series:
            assert hasattr(model, "y_ij_m_model")
            assert hasattr(model, "e_m_model")
            assert f"Scaling national_employment of {model} by 1000" in caplog.messages

    def test_2020_to_2043(self, three_cities_2018_2043, month_day, caplog) -> None:
        """Test generating a longer time series with three cities.

        Todo:
            * Check if employment scaling log should be present
        """
        caplog.set_level(INFO)
        assert three_cities_2018_2043[0].year == FIRST_YEAR  # date(2018, 1, 1)
        assert three_cities_2018_2043[-1].year == LAST_YEAR
        assert three_cities_2018_2043.dates.index(month_day.from_year(FIRST_YEAR)) == 0
        assert len(three_cities_2018_2043) == 26
        assert str(three_cities_2018_2043) == (
            "26 Annual Spatial Input-Output models from 2018 to 2043: 10 sectors, 3 regions"
        )
        assert repr(three_cities_2018_2043) == (
            "InterRegionInputOutputTimeSeries(dates=26, "
            "start=2018, end=2043, sectors=10, regions=3)"
        )
        three_cities_2018_2043.calc_models()
        for model in three_cities_2018_2043:
            assert hasattr(model, "y_ij_m_model")
            assert hasattr(model, "e_m_model")
            assert (
                f"Scaling national_employment of {model} by 1000" not in caplog.messages
            )
            assert (
                f"{model} `raw_io_table` attribute needs conversion "
                "from type <class 'estios.sources.MetaData'>. "
                "Will try running `self._get_meta_file_or_data_fields()`."
                in caplog.messages
            )
