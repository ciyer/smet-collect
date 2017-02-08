#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
analyze.py

Module for analyzing data. Runs against pruned data.

TODO: Rename to process.

Created by Chandrasekhar Ramakrishnan on 2015-12-11.
Copyright (c) 2015 Chandrasekhar Ramakrishnan. All rights reserved.
"""

import json
import os

from . import command
from ..bundle import slug_for_race


class CandidateConfigToJson(object):
    """Describe the candidate configurations as JSON"""

    def __init__(self, status):
        """Constructor for the analyzer.
        :param status: The CollectorStatus object that tracks status state
        """
        self.status = status

    def default_path(self):
        return os.path.join(self.status.analyzed_data_folder_path(), "analysis_config.json")

    def save(self, path=None):
        """Write out JSON describing the candidate config to path"""

        if not path:
            path = self.default_path()
        races = []
        for race in self.status.races():
            race_dict = {"slug": race.slug, "candidates": []}
            races.append(race_dict)
            for candidate in race.candidates.all():
                terms = [term.term for term in candidate.search_terms.all()]
                candidate_dict = {"name": candidate.name, "terms": terms}
                race_dict["candidates"].append(candidate_dict)
        with open(path, "w") as f:
            json.dump(races, f)


class AnalyzerConfig(command.ProcessCommandConfig):
    """Gathers configuration information for the Analyzer"""
    def __init__(self, status, driver, script, description, max_depth=5, just_config=False):
        """The config determines which driver is run and where the results end up"""
        super(AnalyzerConfig, self).__init__(driver, script, description, max_depth, just_config)
        self.output_path_components = lambda race, run=None: status.analysis_result_path_components(race, None, run)


class MetadataAnalyzerConfig(command.ProcessCommandConfig):
    """Configuration for running metadata analysis"""
    def __init__(self, status, max_depth=5, just_config=False):
        super(MetadataAnalyzerConfig, self).__init__("MetadataSummary", "mdsummary.rb", "Analyzing", max_depth, just_config)
        self.output_path_components = lambda race, run=None: status.analysis_result_path_components(race, "metadata", run)


class MetadataPlusAnalyzerConfig(command.ProcessCommandConfig):
    """Configuration for running metadata analysis"""
    def __init__(self, status, max_depth=5, just_config=False):
        super(MetadataPlusAnalyzerConfig, self).__init__(None, "mdsummary_plus.rb", "Analyzing", max_depth, just_config)
        self.output_path_components = lambda race, run=None: status.analysis_result_path_components(race, "mdplus", run)


class HashtagAnalyzerConfig(command.ProcessCommandConfig):
    """Configuration for running metadata analysis"""
    def __init__(self, status, max_depth=5, just_config=False):
        super(HashtagAnalyzerConfig, self).__init__("HashtagSummary", "hashtags.rb", "Analyzing Hashtags", max_depth, just_config)
        self.output_path_components = lambda race, run=None: status.analysis_result_path_components(race, "hashtag", run)


class GenericAnalyzer(command.ProcessCommand):
    """Analyze pruned runs based on the configuration"""

    def __init__(self, status, config=None, race=None):
        """Constructor for the analyzer.
        :param status: The CollectorStatus object that tracks status state
        :param config: The configuration for the pruner
        :param race: The slug for a race if should restrict to one race
        """
        self.status = status
        if not config:
            # Default to metadata analyzer
            config = MetadataAnalyzerConfig(status)
        self.config = config
        super(GenericAnalyzer, self).__init__(self.status, config, race)

    def prepare_processing(self, races):
        """Do any preparation necessary to process the races. Default does nothing."""
        for race in races:
            parent = self.config.output_path_components(race)
            self.status.ensure_folder_exists(self.status.path_from_components(parent))
        if len(races) > 0:
            CandidateConfigToJson(self.status).save()

    def queue_processing(self, race, run):
        pruned_data_path = self.status.pruned_data_file_path_for_run(race, run)
        analyzed_data_path_components = self.config.output_path_components(race, run)
        msg = "{} run: {}".format(self.process_description(), pruned_data_path.encode('utf-8'))
        self.status.progress_func({'type': 'analyze', 'message': msg})
        self.add_spark_task(pruned_data_path, analyzed_data_path_components, slug_for_race(race))
