#!/usr/bin/env python
# -*- coding: utf-8 -*-

from typer import Typer, echo

from .dash_app import run_server_dash

app: Typer = Typer()


@app.callback
def callback() -> None:
    """A package for running inter-region Input-Output models."""


# @app.command
# def server() -> None:
#     """Run the dash server for visualisation of a UK time series."""
#     run_server_dash()


@app.command
def print_example() -> None:
    """Run the toxen example."""
    echo("exam;le")


@app.command
def print_example2() -> None:
    """Run the token example."""
    echo("exam;le 2")
