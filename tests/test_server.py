#!/usr/bin/env python
# -*- coding: utf-8 -*-


from regional_input_output.dash_app import get_server_dash


def test_server():
    server = get_server_dash()
    "/dash" in [route.path for route in server.routes]
