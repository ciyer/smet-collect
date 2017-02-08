#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
command.py

Abstract class that defines the structure of commands that can be run against a command engine (Spark, jq, etc.)

Created by Chandrasekhar Ramakrishnan on 2016-04-20.
Copyright (c) 2016 Chandrasekhar Ramakrishnan. All rights reserved.
"""
import abc
import os
from collections import defaultdict

from .task_config import AnalysisTaskDef, AnalysisTaskConfigToJson
from ..bundle.status_db import Run


class ProcessCommandConfig(object):
    """Configuration parameters for an analysis command"""

    def __init__(self, spark_driver, jq_script, description, max_depth=5, just_config=False):
        """
        :param spark_driver: Driver to use when run against spark engine
        :param jq_script: Script to run when processing with jq
        :param description: Description of the command
        :param max_depth: The maximum number of runs per race to prune. Use None or non-positive to prune all.
        :param just_config: Only output config, do not run the command
        """
        self.spark_driver = spark_driver
        self.jq_script = jq_script
        self.process_description = description
        self.max_depth = max_depth if max_depth > 0 else None
        self.just_config = just_config


class ProcessCommand(object):
    """Superclass with helper methods for commands that interact with Spark"""
    __metaclass__ = abc.ABCMeta

    def __init__(self, status, config, race=None):
        """
        Initialize the SparkCommand.
        :param status: The CollectorStatus object that tracks status state
        :param config: The configuration for the pruner
        :param race: The slug for a race if should restrict to one race
        :return:
        """
        self.status = status
        self.config = config
        self.race_slug = race
        self.runs_to_process = defaultdict(list)
        self.tasks = []

    def process_description(self):
        """
        :return: a string describing what this class does.
        """
        return self.config.process_description

    def add_spark_task(self, in_path, out_path_components, race_slug):
        """Add a task to the list of tasks to run.
        :param in_path: The path to the input file
        :param out_path: The location to write the output file
        :param race_slug: The slug of the race
        :return:
        """
        task = AnalysisTaskDef(in_path, out_path_components[0], out_path_components[1], race_slug)
        self.tasks.append(task)

    def generate_task_config(self, slug):
        """
        Write out a file that describes the task to run.
        :param slug: A slug that identifies this spark command.
        :return: The command config object
        """
        log_file_path = self.status.generate_running_log_file_path("submit")
        cmd_config = AnalysisTaskConfigToJson(self.status, slug, self.tasks)
        cmd_config.save()
        msg = "Task configuration written to {}".format(cmd_config.default_path())
        self.status.progress_func({'type': 'progress', 'message': msg})
        return cmd_config

    def log_intermediate_progress_update(self):
        races = self.runs_to_process.keys()
        if len(races) < 1:
            self.status.progress_func({'type': 'progress', 'message': "No runs to process."})
            return
        for key in races:
            runs = self.runs_to_process[key]
            msg = "Race {} has {} runs to process".format(key.name.encode('utf-8'), len(runs))
            self.status.progress_func({'type': 'progress', 'message': msg})
        max_depth = self.config.max_depth
        if max_depth is not None and max_depth > 0:
            msg = "\tLimiting to {} runs per race".format(self.config.max_depth)
            self.status.progress_func({'type': 'progress', 'message': msg})

    def queue_tasks(self):
        races = self.runs_to_process.keys()
        self.prepare_processing(races)
        max_depth = self.config.max_depth
        for race in races:
            runs = self.runs_to_process[race]
            if max_depth and max_depth > 0:
                runs = runs[0:self.config.max_depth]
            for run in runs:
                self.queue_processing(race, run)

    @abc.abstractmethod
    def queue_processing(self, race, run):
        """Queue the parameters needed for processing the race/run."""
        return

    @staticmethod
    def prepare_processing(races):
        """Do any preparation necessary to process the races. Default does nothing."""
        return

    def collect_runs_to_process(self):
        """Find runs that need to be pruned"""
        races = self.status.races_matching_slug(self.race_slug)
        if self.race_slug:
            if len(races) < 1:
                msg = "Found no races matching slug {}.".format(self.race_slug)
                self.status.progress_func({'type': 'error', 'message': msg})
                return
            if len(races) > 1:
                msg = "Found multiple races matching slug {}.".format(self.race_slug, races)
                self.status.progress_func({'type': 'error', 'message': msg})
                return
        for race in races:
            self.collect_runs_from_race(race)

    def collect_runs_from_race(self, race):
        """Prune the runs in the race down to the most relevant data
        """
        msg = "Collecting runs from race {}".format(race.name.encode('utf-8'))
        self.status.progress_func({'type': 'progress', 'message': msg})
        for run in race.runs.order_by(Run.start.desc()):
            if self.should_process_run(race, run):
                self.runs_to_process[race].append(run)

    def should_process_run(self, race, run):
        """Prune the runs in the race down to the most relevant data
        """
        analyzed_data_path_components = self.config.output_path_components(race, run)
        return not os.path.exists(self.status.path_from_components(analyzed_data_path_components))

    @staticmethod
    def process_description():
        """
        :return: a string describing what this class does.
        """
        return "Processing"
