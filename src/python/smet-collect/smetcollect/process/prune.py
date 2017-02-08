#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
prune.py

Module for pruning acquired data to just the relevant subset.

Created by Chandrasekhar Ramakrishnan on 2015-10-30.
Copyright (c) 2015 Chandrasekhar Ramakrishnan. All rights reserved.
"""

import os

from . import command
from ..bundle import slug_for_race


class PrunerConfig(command.ProcessCommandConfig):
    """Gathers configuration information for the Pruner"""

    def __init__(self, status, max_depth=5, just_config=False):
        super(PrunerConfig, self).__init__("PruneTweets", "prune.rb", "Pruning", max_depth, just_config)
        self.output_path_components = lambda race, run=None: status.pruned_data_file_path_components(race, run)


class Pruner(command.ProcessCommand):
    """Prune runs to the relevant data"""

    def __init__(self, status, config=None, race=None):
        """Constructor for the pruner.
        :param status: The CollectorStatus object that tracks status state
        :param config: The configuration for the pruner
        :param race: The slug for a race if should restrict to one race
        """
        self.status = status
        config = config if config is not None else PrunerConfig(status)
        super(Pruner, self).__init__(self.status, config, race)

    def queue_processing(self, race, run):
        pruned_data_path_components = self.status.pruned_data_file_path_components(race, run)
        raw_data_path = self.status.raw_data_folder_path_for_run(race, run)
        msg = "{} run: {}".format(self.process_description(), raw_data_path.encode('utf-8'))
        self.status.progress_func({'type': 'prune', 'message': msg})
        self.add_spark_task(raw_data_path, pruned_data_path_components, slug_for_race(race))

    def should_process_run(self, race, run):
        """Prune the runs in the race down to the most relevant data
        """
        return not self.status.has_pruned_data_for_run(race, run)

