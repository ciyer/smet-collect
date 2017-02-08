#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
conftest.py


Code that supports tests that use the serialized search results as a fixture.


Created by Chandrasekhar Ramakrishnan on 2016-08-29.
Copyright (c) 2016 Chandrasekhar Ramakrishnan. All rights reserved.
"""

import os
import shutil

import pytest

from . import bundle


@pytest.fixture
def test_data_folder_path():
    module_path = os.path.dirname(__file__)
    return os.path.join(os.path.dirname(module_path), "..", "..", "..", "test_data", "collect")


@pytest.fixture
def smet_bundle():
    the_smet_bundle = bundle.Bundle(test_data_folder_path())
    yield the_smet_bundle
    if os.path.exists(the_smet_bundle.status_db_path):
        os.remove(the_smet_bundle.status_db_path)


@pytest.fixture
def smet_bundle2(test_data_folder_path, tmpdir_factory):
    # Ensure that a new engine is created and do not reuse the existing one
    bundle_folder = str(tmpdir_factory.mktemp('smet_bundle'))
    shutil.copy(os.path.join(test_data_folder_path, 'config.yaml'), bundle_folder)
    shutil.copy(os.path.join(test_data_folder_path, 'credentials.yaml'), bundle_folder)
    the_smet_bundle = bundle.Bundle(bundle_folder)
    yield the_smet_bundle
    if os.path.exists(the_smet_bundle.status_db_path):
        os.remove(the_smet_bundle.status_db_path)
