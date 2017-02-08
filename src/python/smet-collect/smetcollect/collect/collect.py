#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
collect.py

Module for acquiring data from twitter.

Created by Chandrasekhar Ramakrishnan on 2015-08-08.
Copyright (c) 2015 Chandrasekhar Ramakrishnan. All rights reserved.
"""

import json
import os
import time
from datetime import datetime
import sys
import shutil

import dateutil.parser
import pytz
import six
from twython import Twython
from twython import TwythonRateLimitError

if six.PY2:
    import urlparse
else:
    import urllib.parse as urlparse

    long = int

from ..bundle import datetime_to_results_filename, results_filename_to_datetime, datetime_to_run_folder_name, \
    run_folder_name_to_datetime, slug_for_race
from ..bundle.status_db import Run, Search, SearchTerm, Candidate

# The default limit for running searches is 2h between search requets
default_collector_wait_period = 2


def write_json_string(json_str, now, folder_path):
    output_filename = datetime_to_results_filename(now)
    out_path = os.path.join(folder_path, output_filename)
    with open(out_path, "w") as f:
        f.write(json_str)
    return output_filename


def default_results_save_func(now, results, folder_path):
    """Write the twitter results to a new file in folder_path.
    :param results: The search results to store
    :param folder_path: The path to write it to. Assumes that the parent directory exists.
    """
    if None == results:
        return
    results_str = json.dumps(results, separators=(',', ': '))
    return write_json_string(results_str, now, folder_path)


def pretty_print_results_save_func(now, results, folder_path):
    """Write the twitter results pretty printed to a new file in folder_path.
    :param results: The search results to store
    :param folder_path: The path to write it to. Assumes that the parent directory exists.
    """
    if None == results:
        return
    results_str = json.dumps(results, indent=4, separators=(',', ': '))
    return write_json_string(results_str, now, folder_path)


def rate_limit_info(limit_remaining, limit_reset, progress_func):
    """Given rate limit remaining and rate limit reset info from twitter, return the limit and sleep dur"""
    limit = int(limit_remaining) if limit_remaining else 0
    try:
        sleep_until_ts = float(limit_reset)
        sleep_until = datetime.utcfromtimestamp(sleep_until_ts)
        sleep_dur = sleep_until - datetime.utcnow()
        return limit, sleep_dur
    except TypeError:
        msg = "x-rate-limit-reset({}) is not a float".format(limit_reset)
        progress_func({'type': 'error', 'message': msg})
        now = datetime.utcnow()
        return limit, now - now


def earliest_and_latest_tweet_dates(results):
    tweet_date_strings = [status['created_at'] for status in results['statuses']]
    if len(tweet_date_strings) < 1:
        return None, None
    tweet_dates = [dateutil.parser.parse(datestr).astimezone(pytz.utc) for datestr in tweet_date_strings]
    tweet_dates.sort()
    earliest_tweet_date = tweet_dates[0]
    latest_tweet_date = tweet_dates[-1]
    return earliest_tweet_date, latest_tweet_date


class CollectorConfig(object):
    """Gathers configuration information for the TweetCollector"""

    def __init__(self, wait_period=None, save_func=None, max_depth=5):
        """
        :param wait_period: The minimum number of hours to wait between searches (float)
        :param save_func: A function that saves twitter data to disk
        :param max_depth: The maximum number of calls per search term. Defaults to 5, use None for unlimited
        """
        self.save_func = save_func if save_func else default_results_save_func
        self.wait_period = wait_period if wait_period is not None else default_collector_wait_period
        self.max_depth = max_depth


class TweetCollector(object):
    """Performs searches, stores results to disk, updates the status db"""

    def __init__(self, status, config=None, resume=False, race=None, until=None):
        """Constructor for the tweet collector
        :param status: The CollectorStatus object that tracks status state
        :param config: Configuration for the tweet collector
        :param resume: Resume the last run if true, otherwise start a new run
        :param race: The slug for a race if should restrict to one race
        :param until: Return only tweets before the provided date (YYYY-MM-DD)
        """
        self.status = status
        self.config = config if config else CollectorConfig()
        self.resume = resume
        self.race_slug = race
        self.until = until
        credentials = status.bundle.credentials
        self.twitter = Twython(credentials.app_key, access_token=credentials.access_token)
        self.called_twitter = False
        self.collector_run = None  # To be filled in

    def run(self):
        """Run searches for all the races"""
        if self.race_slug:
            matching_races = [race for race in self.status.races() if slug_for_race(race) == self.race_slug]
            if len(matching_races) < 1:
                msg = "Found no races matching slug {}.".format(self.race_slug)
                self.status.progress_func({'type': 'error', 'message': msg})
                return
            if len(matching_races) > 1:
                msg = "Found multiple races matching slug {}.".format(self.race_slug, matching_races)
                self.status.progress_func({'type': 'error', 'message': msg})
                return
            self.run_searches_for_race(matching_races[0])
        else:
            for race in self.status.races():
                self.run_searches_for_race(race)

        if self.called_twitter:
            remaining = self.twitter.get_lastfunction_header('x-rate-limit-remaining')
            limit_reset = self.twitter.get_lastfunction_header('x-rate-limit-reset')
            limit, sleep_dur = rate_limit_info(remaining, limit_reset, self.status.progress_func)
            msg = 'Run finished : {} searches remain in period. Next period starts in {:.2f}s'.format(
                limit, sleep_dur.total_seconds())
        else:
            msg = 'No calls made'
        self.status.progress_func({'type': 'progress', 'message': msg})

    def run_searches_for_race(self, race):
        """Get the most recent tweets for the particular race
        """
        race_collector = TweetRaceCollector(self.status, self.config, self.twitter, self.resume, race, self.until)
        race_collector.run()
        self.called_twitter = self.called_twitter or race_collector.called_twitter


class TweetRaceCollector(object):
    """Collects search results for one race"""

    def __init__(self, status, config, twitter, resume, race, until=None):
        """Constructor for the tweet collector
        :param status: The CollectorStatus object that tracks status state
        :param config: Configuration for the tweet collector
        :param twitter: The twitter object
        :param resume: Resume the last run if true, otherwise start a new run
        :param race: The race to run a search for
        """
        self.status = status
        self.config = config
        self.twitter = twitter
        self.resume = resume
        self.race = race
        self.until = until
        self.result_type = 'recent'  # The type of results we want from twitter: 'recent', 'mixed', or 'popular'
        self.called_twitter = False

        # To be filled in
        self.collector_run = None
        self.previous_collector_run = None
        self.output_folder_path = None
        self.current_time = None

    def initialize_state(self):
        self.move_time_forward()
        now = self.current_time

        # Get the recent runs which we need to initialize previous_collector_run and maybe collector_run
        recent_runs = self.race.runs.order_by(Run.start.desc()).slice(0, 2).all()
        if self.resume:
            if len(recent_runs) < 1:
                return  # No runs to resume
            # Assume we want to resume the last run
            last_run = recent_runs[0]
            if len(recent_runs) > 1:
                self.previous_collector_run = recent_runs[1]
            self.collector_run = last_run
        else:
            if len(recent_runs) > 0:
                self.previous_collector_run = recent_runs[0]
            run_folder_name = datetime_to_run_folder_name(now)
            self.collector_run = Run(start=now, results_folder=run_folder_name, race=self.race)
            self.status.session.commit()

        # self.output_folder_path = os.path.join(self.status.raw_data_folder_path_for_race(self.race), run_folder_name)
        self.output_folder_path = self.status.raw_data_folder_path_for_run(self.race, self.collector_run)
        self.ensure_output_folder_exists()

    def run(self):
        """Run searches for all the candidates"""

        self.initialize_state()
        if self.resume and self.collector_run is None:
            self.status.progress_func({'type': 'progress', 'message': "No run to resume"})
            return  # There is no run to resume

        candidates = self.race.candidates.filter(Candidate.active == True).all()
        for candidate in candidates:
            self.run_searches_for_candidate(candidate)

        self.collector_run.end = self.current_time
        self.status.session.commit()

    def run_searches_for_candidate(self, candidate):
        """Get the most recent tweets for the particular race
        """
        search_terms = [search_term for search_term in candidate.search_terms.all()]
        for search_term in search_terms:
            if not search_term.active:
                continue
            last_run_max_id = self.get_last_run_max_id(search_term)
            results = self.collect_first_tweets(search_term, last_run_max_id)
            if results is None:
                continue
            self.collect_subsequent_tweets(search_term, last_run_max_id, results)

    def get_last_run_max_id(self, search_term):
        if self.previous_collector_run is None:
            last_run_max_id = None
        elif self.until is not None:
            # Take the highest max_id from the latest run after the date
            until_date = datetime.strptime(self.until, "%Y-%m-%d")
            last_run_max_id_search = search_term.searches.filter(
                Search.latest < until_date).order_by(Search.max_id.desc()).first()
            if last_run_max_id_search is None:
                last_run_max_id = None
            else:
                last_run_max_id = last_run_max_id_search.max_id
        else:
            # Take the highest max_id from a previously completed run
            last_run_max_id_search = search_term.searches.filter(
                Search.date < self.collector_run.start).order_by(Search.max_id.desc()).first()
            if last_run_max_id_search is None:
                last_run_max_id = None
            else:
                last_run_max_id = last_run_max_id_search.max_id
        return last_run_max_id

    def collect_first_tweets(self, search_term, last_run_max_id):
        """Get the first results for this search term for this run.

        The implementation depends on whether or not this is a resume operation.
        - On a resume, it reads the results from the disk.
        - Otherwise, check if we are in the waiting period, if so, return None
        - If we are out of the waiting period, call twitter to get the latest results

        :param search_term: The term to search for
        :param last_run_max_id: The max_id from the last run
        :return: results: Results is none if searching should not proceed, otherwise the results used to continue the search.
        """
        query_str = search_term.term
        result_type = self.result_type

        if self.resume:
            search_to_continue = search_term.searches.filter(
                Search.run_id == self.collector_run.id).order_by(Search.date.desc()).first()
            if search_to_continue:
                return self.read_search_results(search_to_continue)
                # If we didn't find a search to continue, then run a new search

        # Advance time
        self.move_time_forward()
        last_search = search_term.searches.order_by(Search.date.desc()).first()
        if self.check_waiting_period(last_search):
            return None

        kwargs = {}
        if last_run_max_id:
            kwargs['since_id'] = last_run_max_id
        if self.until:
            kwargs['until'] = self.until

        results = self.search_twitter(query_str, result_type, **kwargs)

        self.process_results(results, search_term)
        return results

    def collect_subsequent_tweets(self, search_term, last_run_max_id, results):
        """
        :param search_term: The term object to search for
        :param last_run_max_id: The highest id from the last run
        :param results: Results of the first search
        """
        query_str = search_term.term
        result_type = self.result_type

        reached_depth = 0 if self.resume else 1

        while results['search_metadata'].get('next_results'):
            if self.reached_max_depth(reached_depth):
                break
            query_params = urlparse.parse_qs(urlparse.urlparse(results['search_metadata'].get('next_results')).query)
            # Advance time
            self.move_time_forward()
            results = self.search_twitter(query_str, result_type, since_id=last_run_max_id,
                                          max_id=query_params['max_id'][0])
            reached_depth += 1
            self.process_results(results, search_term)

    def reached_max_depth(self, reached_depth):
        max_depth = self.config.max_depth
        return reached_depth >= max_depth if max_depth is not None else False

    def check_waiting_period(self, last_search):
        """Check if we are still waiting for the wait period to expire.
        :param last_search:
        :return: True if still in waiting period, False if the search can proceed
        """

        if last_search is None:
            return False
        now = self.current_time
        diff = now - last_search.date
        if diff.total_seconds() < self.config.wait_period * 3600:
            return True
        return False

    def process_results(self, results, search_term):
        now = self.current_time
        output_filename = self.config.save_func(now, results, self.output_folder_path)
        result_max_id = long(results['search_metadata']['max_id'])
        earliest_tweet_date, latest_tweet_date = earliest_and_latest_tweet_dates(results)

        self.update_status_db(now, output_filename, result_max_id, search_term,
                              earliest_tweet_date, latest_tweet_date)
        self.check_rate_limit()

    def check_rate_limit(self):
        remaining = self.twitter.get_lastfunction_header('x-rate-limit-remaining')
        limit_reset = self.twitter.get_lastfunction_header('x-rate-limit-reset')
        limit, sleep_dur = rate_limit_info(remaining, limit_reset, self.status.progress_func)
        if limit > 0:
            return
        msg = 'Hit rate limit sleeping {}s'.format(sleep_dur.total_seconds())
        self.status.progress_func({'type': 'rate-limit', 'message': msg})
        if sleep_dur.total_seconds() > 0:
            time.sleep(sleep_dur.total_seconds())

    def search_twitter(self, query_str, result_type, **kwargs):
        """Fill in the values for standard parameters:
        The parameters include_entities and count are filled, the rest are passed as args or kwargs.
        """
        if self.status.progress_func:
            msg = 'Searching for {} : {}'.format(query_str, kwargs)
            self.status.progress_func({'type': 'search', 'message': msg})
        try:
            self.called_twitter = True
            results = self.twitter.search(q=query_str, result_type=result_type,
                                          include_entities=1, count=100, **kwargs)
        except TwythonRateLimitError as inst:
            limit_reset = self.twitter.get_lastfunction_header('x-rate-limit-reset')
            limit, sleep_dur = rate_limit_info(0, limit_reset, self.status.progress_func)
            msg = 'Hit rate limit {}, sleeping {}s'.format(inst.msg, sleep_dur.total_seconds())
            self.status.progress_func({'type': 'rate-limit', 'message': msg})
            if sleep_dur.total_seconds() > 0:
                time.sleep(sleep_dur.total_seconds())
            results = self.twitter.search(q=query_str, result_type=result_type,
                                          include_entities=1, count=100, **kwargs)
        return results

    def update_status_db(self, now, output_filename, result_max_id, search_term,
                         earliest_tweet_date, latest_tweet_date):

        search_obj = Search(date=now, max_id=result_max_id, results_path=output_filename,
                            earliest=earliest_tweet_date,
                            latest=latest_tweet_date, run=self.collector_run)
        search_obj.search_term = search_term
        self.status.session.commit()

    def ensure_output_folder_exists(self):
        self.status.ensure_folder_exists(self.output_folder_path)

    def read_search_results(self, search):
        data_path = os.path.join(self.output_folder_path, search.results_path)
        with open(data_path) as f:
            results = json.load(f)
        return results

    def move_time_forward(self):
        self.current_time = self.status.datetime_provider()


class RawImport(object):
    """Imports search results from disk into the db.

    The import works as follows. For each run in the import_root (defaults to the status root):
    - If there is already a run in the DB, do nothing
    - Hardlink copy the run to the status folder if it is not yet in the status folder
    - Import the run into the DB.
    """

    def __init__(self, status, import_root=None, config=None):
        """Constructor for the tweet collector
        :param status: The CollectorStatus object that tracks status state
        :param import_root: The folder to import data from. Should also adhere to the bundle structure.
        :param config: Configuration for the tweet collector
        """
        self.status = status
        self.import_root = import_root if import_root is not None else status.output_path
        self.imported_run_folders = []
        self.skipped_run_folders = []
        self.config = config if config else CollectorConfig()

    def run(self):
        """Look for data for each race"""
        for race in self.status.races():
            self.read_data_for_race(race)

    def read_data_for_race(self, race):
        """See if there are any data files for this race
        """
        import_raw_data_path = self.status.raw_data_folder_path_from_root_for_race(self.import_root, race)
        if not os.path.exists(import_raw_data_path):
            return
        bundle_raw_data_path = self.status.raw_data_folder_path_for_race(race)
        self.status.ensure_folder_exists(bundle_raw_data_path)

        msg = 'Importing data for race {}'.format(race.name)
        self.status.progress_func({'type': 'import', 'message': msg})
        for path in os.listdir(import_raw_data_path):
            self.import_runs(race, import_raw_data_path, bundle_raw_data_path, path)

    def prepare_import_run(self, race, import_run_path, bundle_run_path, run_folder_name):
        now = run_folder_name_to_datetime(run_folder_name)
        # If the run is already in the db, skip it
        existing_run = self.status.session.query(Run).filter_by(start=now, results_folder=run_folder_name,
                                                                race=race).first()
        if existing_run is not None:
            return None

        # Copy the files to the bundle path if necessary and create a run
        self.status.ensure_folder_exists(bundle_run_path)
        self.hardlink_copy(import_run_path, bundle_run_path)

        collector_run = Run(start=now, results_folder=run_folder_name, race=race)
        return collector_run

    @staticmethod
    def hardlink_copy(import_run_path, bundle_run_path):
        for path in os.listdir(import_run_path):
            src = os.path.join(import_run_path, path)
            dst = os.path.join(bundle_run_path, path)
            if not os.path.exists(dst):
                os.link(src, dst)

    def import_runs(self, race, import_raw_data_path, bundle_raw_data_path, run_folder_name):
        import_run_path = os.path.join(import_raw_data_path, run_folder_name)
        bundle_run_path = os.path.join(bundle_raw_data_path, run_folder_name)
        collector_run = self.prepare_import_run(race, import_run_path, bundle_run_path, run_folder_name)
        if collector_run is None:
            self.skipped_run_folders.append(run_folder_name)
            msg = '\tSkipping run {}'.format(run_folder_name)
            self.status.progress_func({'type': 'import', 'message': msg})
            return

        # After preparing, all the folders to import are now in the bundle run path
        msg = '\tImporting run {}'.format(run_folder_name)
        self.status.progress_func({'type': 'import', 'message': msg})
        self.imported_run_folders.append(run_folder_name)

        for path in os.listdir(bundle_run_path):
            self.import_search_results(race, collector_run, bundle_run_path, path)

        self.status.session.commit()

    def import_search_results(self, race, collector_run, run_data_path, path):
        """Import the information from the data files into the db"""
        data_path = os.path.join(run_data_path, path)
        with open(data_path) as f:
            results = json.load(f)
            search_metadata = results['search_metadata']
            query_params = urlparse.parse_qs(urlparse.urlparse(search_metadata.get('refresh_url')).query)
            search_term_str = query_params['q'][0]
            max_id = long(search_metadata['max_id'])
        # Find the candidate/search_term this belongs to
        candidate = race.candidates.join(SearchTerm).filter(SearchTerm.term == search_term_str).first()
        if not candidate:
            err_msg = "Did not find search term {} in db for race {}".format(search_term_str, race.name)
            print(err_msg, sys.stderr)
            return
        # This should be done in the above query for efficiency, but need running code now
        search_term = candidate.search_terms.filter(SearchTerm.term == search_term_str).first()
        search = search_term.searches.filter(Search.max_id == max_id).first()
        if not search:
            if self.status.progress_func:
                msg = 'Importing data at path {}'.format(data_path)
                self.status.progress_func({'type': 'import', 'message': msg})
            earliest_tweet_date, latest_tweet_date = earliest_and_latest_tweet_dates(results)
            self.update_status_db(path, collector_run, max_id, search_term, earliest_tweet_date, latest_tweet_date)

    def update_status_db(self, output_filename, collector_run, result_max_id, search_term,
                         earliest_tweet_date, latest_tweet_date):

        now = results_filename_to_datetime(output_filename)
        search_obj = Search(date=now, max_id=result_max_id, results_path=output_filename,
                            earliest=earliest_tweet_date,
                            latest=latest_tweet_date)
        search_obj.search_term = search_term
        search_obj.run = collector_run
