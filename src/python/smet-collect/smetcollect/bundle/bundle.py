#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
bundle.py

Classes and functions for working with the smetcollect bundle format.

A bundle is a directory with a particular structure:

[bundle root]/      -- Any directory
    raw/            -- The raw search results from twitter
    pruned/         -- The raw search results pruned down to the core fields
    config.yaml     -- The configuration that describes the races
    credentials.yaml - Credentials for twitter
    status.db       -- The db that maintains the status for the raw data

Created by Chandrasekhar Ramakrishnan on 2015-09-25.
Copyright (c) 2015 Chandrasekhar Ramakrishnan. All rights reserved.
"""

from datetime import datetime
import os

from sqlalchemy import create_engine

from .status_db import Session, Base, get_or_create, Race, Candidate, SearchTerm
from . import config_file


def default_progress_func(structure_msg):
    pass


def default_current_datetime_provider():
    """Return the current time in UTC"""
    return datetime.utcnow()


def smet_filename_to_datetime(filename, suffix):
    return datetime.strptime(filename, "%Y-%m-%d-%H-%M-%S-%f{}".format(suffix))


def datetime_to_results_filename(now):
    return now.strftime("%Y-%m-%d-%H-%M-%S-%f.json")


def results_filename_to_datetime(filename):
    return datetime.strptime(filename, "%Y-%m-%d-%H-%M-%S-%f.json")


def datetime_to_run_folder_name(now):
    return now.strftime("%Y-%m-%d-%H-%M-%S-%f_run")


def run_folder_name_to_datetime(filename):
    return datetime.strptime(filename, "%Y-%m-%d-%H-%M-%S-%f_run")


def slug_for_race(race):
    return slug_for_string(race.name)


def slug_for_string(s):
    return s.lower().replace(' ', '-')


def ensure_folder_exists(folder):
    if not os.path.isdir(folder):
        os.makedirs(folder)


# TODO rename to BasicCollectBundle
class Bundle(object):
    """Interface for interacting with bundles as defined in module comment"""

    def __init__(self, path):
        self.bundle_root_path = path
        self.output_data_path = self.bundle_root_path
        self.config_path = os.path.join(self.bundle_root_path, 'config.yaml')
        self.credentials_path = os.path.join(self.bundle_root_path, 'credentials.yaml')
        self.status_db_path = os.path.join(self.bundle_root_path, 'status.db')
        self._config = None
        self._credentials = None
        self._collector_status_engine = None

    @property
    def config(self):
        if self._config:
            return self._config
        self._config = config_file.SmetCollectConfigYaml(self.config_path)
        return self._config

    @property
    def credentials(self):
        if self._credentials:
            return self._credentials
        self._credentials = config_file.TwitterCredentialsYaml(self.credentials_path)
        return self._credentials

    @property
    def collector_status_engine(self):
        if self._collector_status_engine:
            return self._collector_status_engine
        if self.status_db_path is not None:
            self._collector_status_engine = create_engine('sqlite:///{}'.format(self.status_db_path))
        else:
            # The in-memory db is sed for testing
            self._collector_status_engine = create_engine('sqlite:///:memory:')
        return self._collector_status_engine


# TODO rename to CollectBundle
class BundleStatus(object):
    """The status of the searches for a bundle. These are stored in a db"""

    def __init__(self, smet_bundle, config={}):
        """
        :param smet_bundle: The bundle this status is for
        :param config: A dictionary with configuration parameters. Keys include:
            datetime_provider : Return a function that gives the current time
            progress_func : A function invoked as progress occurs.
        """
        # TODO Rename to basic_bundle
        self.bundle = smet_bundle
        self.config = smet_bundle.config
        self.output_path = smet_bundle.output_data_path
        self.engine = smet_bundle.collector_status_engine
        self.datetime_provider = config["datetime_provider"] if config.get("datetime_provider") is not None \
            else default_current_datetime_provider
        self.progress_func = config["progress_func"] if config.get("progress_func") else default_progress_func

        Session.configure(bind=self.engine)
        self.session = Session()

    def create_tables(self):
        """Create the tables if they have not been initialized"""
        Base.metadata.create_all(self.engine)

    def sync_config(self):
        """Insert the rows that represent the config to the db"""

        # Validate the config if it has not yet been
        if self.config.is_valid is None:
            self.config.validate()
        assert self.config.is_valid, "The configuration should have been validated"

        for race_config in self.config.race_configs:
            race, created = get_or_create(self.session, Race, name=race_config['race'])
            if created:
                race.slug = slug_for_race(race)
                race.year = race_config['year']
            race.active = True

            for candidate_config in race_config['candidates']:
                candidate, created = get_or_create(self.session, Candidate, name=candidate_config['name'])
                if created:
                    candidate.race = race
                active = candidate_config['active'] if candidate_config.get('active') is not None else True
                candidate.active = active

                for search_term_config in candidate_config['search']:
                    search_term, created = get_or_create(self.session, SearchTerm, term=search_term_config)
                    if created:
                        search_term.candidate = candidate
                    search_term.active = True
        # TODO: need to handle races/candidates/terms being removed
        self.session.commit()

    def races(self):
        return self.session.query(Race).all()

    def raw_data_folder_path_from_root_for_race(self, root, race):
        return os.path.join(root, "raw", race.slug)

    def raw_data_folder_path_for_race(self, race):
        return self.raw_data_folder_path_from_root_for_race(self.output_path, race)

    def pruned_data_folder_path_for_race(self, race):
        return os.path.join(self.output_path, "pruned", race.slug)

    def compressed_data_folder_path_for_race(self, race):
        return os.path.join(self.output_path, "compressed", race.slug)

    def analyzed_data_folder_path(self):
        return os.path.join(self.output_path, "analyzed")

    def polls_data_folder_path(self):
        return os.path.join(self.output_path, "polls")

    def tmp_folder_path(self):
        return os.path.join(self.output_path, "tmp")

    def analysis_result_path_components(self, race, analysis_type=None, run=None, results_type="json"):
        """Returns a folder and file name for the results. If no run is provided, filename is None"""
        base = os.path.join(self.analyzed_data_folder_path(), race.slug)
        folder = os.path.join(base, analysis_type) if analysis_type is not None else base
        if run is not None:
            filename = "{}.{}".format(run.results_folder, results_type)
            return folder, filename
        else:
            return folder, None

    @staticmethod
    def path_from_components(components):
        return os.path.join(*components) if components[1] is not None else components[0]

    def analysis_result_path(self, race, analysis_type=None, run=None, results_type="json"):
        # TODO Rename to processed result path
        """Returns a path for analysis results"""
        path_components = self.analysis_result_path_components(race, analysis_type, run, results_type)
        return self.path_from_components(path_components)

    def raw_data_folder_path_for_run(self, race, run):
        return os.path.join(self.raw_data_folder_path_for_race(race), run.results_folder)

    def pruned_data_file_path_components(self, race, run):
        return self.pruned_data_folder_path_for_race(race), run.results_folder

    def pruned_data_file_path_for_run(self, race, run):
        """Return the folder file path. Clients rely on this behavior, but callers can choose the robust version."""
        return self.path_from_components(self.pruned_data_file_path_components(race, run))

    def robust_pruned_data_file_path_for_run(self, run):
        """Return the file path as a folder if it exists, otherwise as json."""
        pruned_data_path = self.pruned_data_file_path_for_run(run.race, run)
        if os.path.exists(pruned_data_path):
            return pruned_data_path
        pruned_data_json = pruned_data_path + ".json"
        if os.path.exists(pruned_data_json):
            return pruned_data_json
        return None

    def has_pruned_data_for_run(self, race, run):
        pruned_data_path = self.pruned_data_file_path_for_run(race, run)
        pruned_data_json = pruned_data_path + ".json"
        return os.path.exists(pruned_data_path) or os.path.exists(pruned_data_json)

    def compressed_data_file_path_for_run(self, race, run):
        return os.path.join(self.compressed_data_folder_path_for_race(race), run.results_folder + ".tar.bz2")

    def polls_data_folder_path_for_race(self, race):
        return os.path.join(self.polls_data_folder_path(), race.slug)

    def path_relative_to_bundle(self, path):
        return os.path.relpath(path, self.output_path)

    def running_log_path(self):
        return os.path.join(self.output_path, "log", "running")

    def fail_log_path(self):
        return os.path.join(self.output_path, "log", "failed")

    def success_log_path(self):
        return os.path.join(self.output_path, "log", "succeeded")

    def generate_running_log_file_path(self, operation):
        now = self.datetime_provider()
        time_str = now.strftime("%Y-%m-%d-%H-%M-%S-%f")
        filename = "{}_{}_log.txt".format(time_str, operation)
        parent = self.running_log_path()
        self.ensure_folder_exists(parent)
        output_path = os.path.join(parent, filename)
        return output_path

    def move_log_to_success(self, log_file_path):
        base = os.path.basename(log_file_path)
        self.ensure_folder_exists(self.success_log_path())
        # msg = '{} moved to {}'.format(log_file_path, os.path.join(self.success_log_path(), base))
        # self.progress_func({'type': 'progress', 'message': msg})
        os.rename(log_file_path, os.path.join(self.success_log_path(), base))

    def move_log_to_fail(self, log_file_path):
        base = os.path.basename(log_file_path)
        self.ensure_folder_exists(self.fail_log_path())
        # msg = '{} moved to {}'.format(log_file_path, os.path.join(self.fail_log_path(), base))
        # self.progress_func({'type': 'progress', 'message': msg})
        os.rename(log_file_path, os.path.join(self.fail_log_path(), base))

    def races_matching_slug(self, slug=None):
        """
        :param slug: A race slug or None
        :return: Return all races if slug is none, otherwise return those that match
        """

        if slug:
            return [race for race in self.races() if slug_for_race(race) == slug]
        else:
            return self.races()

    @staticmethod
    def ensure_folder_exists(folder):
        ensure_folder_exists(folder)
