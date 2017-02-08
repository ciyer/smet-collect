#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
__init__.py

TODO: Rename this package to process.


Created by Chandrasekhar Ramakrishnan on 2016-04-20.
Copyright (c) 2016 Chandrasekhar Ramakrishnan. All rights reserved.
"""

from . import prune
from . import analyze
from . import jq

# TODO Import classes directly -- can hide that the processing is done using jq, spark, etc.
