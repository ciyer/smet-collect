#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
config_file_test.py

Tests for the config_file module.

Created by Chandrasekhar Ramakrishnan on 2015-08-08.
Copyright (c) 2015 Chandrasekhar Ramakrishnan. All rights reserved.
"""


def test_config_basics(smet_bundle):
    config = smet_bundle.config
    assert config is not None
    race_configs = config.race_configs
    assert len(race_configs) == 1
    chicago = race_configs[0]
    assert chicago['race'] == "Chicago Mayor Runoff 2015"
    assert chicago['year'] == '2015'
    candidates = chicago['candidates']
    assert len(candidates) == 2
    rahm = candidates[0]
    assert rahm['name'] == 'Rahm Emanuel'
    chuy = candidates[1]
    assert len(chuy['search']) == 2


def test_creds_basics(smet_bundle):
    creds = smet_bundle.credentials
    assert creds is not None
    assert creds.app_key == 'an_app_key_string'
    assert creds.access_token == 'an_access_token_string'

