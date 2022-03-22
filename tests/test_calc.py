#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from pandas import MultiIndex

from regional_input_output.visualisation import plot_iterations


class TestPlotIterations:

    """Test plotting model iterations."""

    def test_exports_plot(self, three_cities_io) -> None:
        """Test plotting exports iterations and title."""
        pass

    def test_imports_plot(self, three_cities_io) -> None:
        """Test plotting imports iterations and title."""
        pass

    def test_flows_plot(self, three_cities_results) -> None:
        """Test plotting flows iterations and title."""
        with pytest.raises(ImportError):
            plot = plot_iterations(three_cities_results.y_ij_m_model, "flows")
