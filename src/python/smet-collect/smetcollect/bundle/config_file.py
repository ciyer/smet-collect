#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
config_file.py

A module for reading config files for SMET.

A bundle is defined as a folder with a config file that describes the data that should be captured.

Created by Chandrasekhar Ramakrishnan on 2015-08-07.
Copyright (c) 2015 Chandrasekhar Ramakrishnan. All rights reserved.
"""

import os

import yaml


def parse_yaml_config_file(config_path):
    """Read the config file and return the search configurations"""
    with open(config_path) as f:
        configs = [config for config in yaml.load_all(f) if config]
    return configs[0]


class SmetCollectConfigYaml(object):
    """A yaml-file-based SMET configuration"""

    def __init__(self, config_path):
        """Create a SmetConfigYaml for the specified file path."""
        self.config_path = config_path
        self.race_configs = parse_yaml_config_file(config_path)
        self.is_valid = None  # Call validate to set this value

    def __eq__(self, other):
        return (isinstance(other, self.__class__)
                and os.path.realpath(self.config_path) == os.path.realpath(other.config_path))

    def __ne__(self, other):
        return not self.__eq__(other)

    def validate(self):
        """Ensure that this configuration is valid, throw an exception otherwise"""
        self.is_valid = True


class TwitterCredentialsYaml(object):
    """The credentials for accessing the Twitter API, stored in YAML format"""

    def __init__(self, creds_path):
        """Create a TwitterCredentialsYaml for the specified file path."""
        self.credentials_path = creds_path
        with open(self.credentials_path) as f:
            creds = yaml.load(f)
        self.app_key = creds['app_key']
        self.access_token = creds['access_token']
