#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tools for managing projections of time series.

Todo:
    * Work out feasibility of tables for configuring runs
    * Ways to save the results and configuration from a model run
    * Especially with ease to share with Alan (perhas excel is safest)

Potential table implementation:
    * Preconfigured pandas Series "template" of needed parameters
    * Ease iterating over row configs and managing their results
    * Feasibility of iterating as subprocesses or async

Index
| year | "io_table_file_path"|
------------------------------
| 2017 | "a/path.xls"        |
| 2018 | "another/path.xls"  |

Index
| year | "io_table_file_path"| National Population |
----------------------------------------------------
| 2017 | "a/path.xls"        |  200k               |
| 2018 | None                |  220K               |

Index
| Beta | "io_table_file_path"  | delta function | Start Year |
|------------------------------------------------------------|
| .2   | "a/path.xls"          |  linear        | 2017       |
| .3   | "a/path.xls"          |  exponential   | 2017       |


"""

from logging import getLogger
from typing import Any, Optional, Protocol, Type

from .models import InterRegionInputOutput, InterRegionInputOutputTimeSeries
from .sources import DEFAULT_ANNUAL_MONTH_DAY, MonthDay
from .utils import AnnualConfigType, DateConfigType

logger = getLogger(__name__)


class TemporalConfigProtocol(Protocol):

    """A protocol for standardising different ways of managing data sources."""

    def __call__(
        self,
        date_conf: DateConfigType,
        annual: bool,
        io_model_config_index: Optional[int] = None,
        input_output_model_cls: Type[InterRegionInputOutput] = InterRegionInputOutput,
        **kwargs: Any,
    ) -> InterRegionInputOutputTimeSeries:
        ...


def date_io_time_series(
    date_conf: DateConfigType,
    annual: bool = False,
    io_model_config_index: Optional[int] = None,
    input_output_model_cls: Type[InterRegionInputOutput] = InterRegionInputOutput,
    **kwargs,
) -> InterRegionInputOutputTimeSeries:
    """Generate an InterRegionInputOutputTimeSeries from a list of io_time_series.

    Note:
        * io_model_config_index may be removed in future
        * Better way of handling annual without relying on bool...

    Todo:
        * Way of managing copying aspects of singe InterRegionInputOutput config
    """
    logger.info(
        "Generating an InputOutputTimeSeries with dates and passed general config."
    )
    io_models: list[InterRegionInputOutput] = []
    if type(date_conf) is dict:
        logger.debug(f"Iterating over {len(date_conf)} with dict configs")
        for date, config_dict in date_conf.items():
            io_model: InterRegionInputOutput = input_output_model_cls(
                date=date, **(config_dict | kwargs)
            )
            io_models.append(io_model)
            logger.debug(f"Added {io_model} to list for generating time series.")
        return InterRegionInputOutputTimeSeries(
            io_models=io_models,
            annual=annual,
            _input_output_model_cls=input_output_model_cls,
            _io_model_config_index=io_model_config_index,
        )
    else:
        io_models = [input_output_model_cls(date=date, **kwargs) for date in date_conf]
        return InterRegionInputOutputTimeSeries(
            io_models=io_models,
            annual=annual,
            _input_output_model_cls=input_output_model_cls,
            _io_model_config_index=io_model_config_index,
        )


def annual_io_time_series(
    annual_config: AnnualConfigType,
    default_month_day: MonthDay = DEFAULT_ANNUAL_MONTH_DAY,
    date_io_time_series_func: TemporalConfigProtocol = date_io_time_series,
    **kwargs,
) -> InterRegionInputOutputTimeSeries:
    """Generate an InterRegionInputOutputTimeSeries from a list of dates."""
    logger.info(
        f"Generating an InputOutputTimeSeries using {default_month_day} for each year."
    )
    date_config: DateConfigType
    if isinstance(annual_config, dict):
        date_config = {
            default_month_day.from_year(year): config_dict
            for year, config_dict in annual_config.items()
        }
    else:
        date_config = [default_month_day.from_year(year) for year in annual_config]
    return date_io_time_series_func(date_config, annual=True, **kwargs)
