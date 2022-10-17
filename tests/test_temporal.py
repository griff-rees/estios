from datetime import date
from logging import INFO

from estios.models import InterRegionInputOutputTimeSeries
from estios.temporal import annual_io_time_series, date_io_time_series
from estios.uk.employment import EMPLOYMENT_QUARTER_DEC_2017
from estios.uk.ons_population_projections import FIRST_YEAR, LAST_YEAR


class TestAnnualProjection:

    """Test examples of annual Input-Output models."""

    def test_annual_default(self) -> None:
        projections: InterRegionInputOutputTimeSeries = annual_io_time_series()
        assert (
            str(projections)
            == "26 Annual Spatial Input-Output models from 2018 to 2043: 10 sectors, 10 regions"
        )


class TestInputOutputTimeSeries:

    """Test a TimeSeries of InputOutput models."""

    def test_date_default(self) -> None:
        projections: InterRegionInputOutputTimeSeries = date_io_time_series()
        assert (
            str(projections)
            == "1 Spatial Input-Output model from 2017-12-01 to 2017-12-01: 10 sectors, 10 regions"
        )

    def test_one_time_point(self) -> None:
        employment_date_list = [
            EMPLOYMENT_QUARTER_DEC_2017,
        ]
        time_series = date_io_time_series(employment_date_list)
        assert len(time_series) == 1

    def test_two_dupe_time_points(self) -> None:
        employment_date_list = [
            EMPLOYMENT_QUARTER_DEC_2017,
            EMPLOYMENT_QUARTER_DEC_2017,
        ]
        time_series = date_io_time_series(employment_date_list)
        assert len(time_series) == 2
        assert len(time_series[1:]) == 1

    def test_two_dupe_time_points_append(self, three_cities_io) -> None:
        employment_date_list = [
            EMPLOYMENT_QUARTER_DEC_2017,
            EMPLOYMENT_QUARTER_DEC_2017,
        ]
        time_series = date_io_time_series(employment_date_list)
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

    def test_pass_2_io_models(self, three_cities_io, caplog) -> None:
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
        config_dict = {
            date: {"employment_date": date} for date in quarterly_2017_employment_dates
        }
        time_series = date_io_time_series(config_dict, regions=three_cities)
        assert time_series[0].date == date(2017, 3, 1)
        assert time_series.dates.index(time_series[0].date) == 0
        assert len(time_series) == 4
        assert str(time_series) == (
            "4 Spatial Input-Output models from 2017-03-01 to 2017-12-01: 10 sectors, 3 regions"
        )
        time_series.calc_models()
        for model in time_series:
            assert hasattr(model, "y_ij_m_model")
            assert hasattr(model, "e_m_model")

    def test_2020_to_2043(self, three_cities_2018_2043, month_day, caplog) -> None:
        """Test generating a longer time series with three cities."""
        assert three_cities_2018_2043[0].year == FIRST_YEAR  # date(2018, 1, 1)
        assert three_cities_2018_2043[-1].year == LAST_YEAR
        assert three_cities_2018_2043.dates.index(month_day.from_year(FIRST_YEAR)) == 0
        assert len(three_cities_2018_2043) == 26
        assert str(three_cities_2018_2043) == (
            "26 Annual Spatial Input-Output models from 2018 to 2043: 10 sectors, 3 regions"
        )
        three_cities_2018_2043.calc_models()
        for model in three_cities_2018_2043:
            assert hasattr(model, "y_ij_m_model")
            assert hasattr(model, "e_m_model")
