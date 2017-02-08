#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
collect_test.py


Created by Chandrasekhar Ramakrishnan on 2015-08-08.
Copyright (c) 2015 Chandrasekhar Ramakrishnan. All rights reserved.
"""

import json
import os
from collections import defaultdict
from datetime import datetime
from datetime import timedelta

import six

from .. import bundle
from . import collect
from .. import conftest

if not six.PY2:
    long = int

number_of_results_at_max_depth_5 = 7


def results_cache_path():
    return os.path.join(conftest.test_data_folder_path(), 'search_results')


def results_continuation_cache_path():
    return os.path.join(conftest.test_data_folder_path(), 'search_results_contd')


def race_output_folder_path(tmpdir):
    return tmpdir.join("raw", "chicago-mayor-runoff-2015")


def stdout_progress(progress_data):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print("{} {}".format(timestamp, progress_data['message']))


def initialized_bundle_status(smet_bundle, tmpdir=None):
    if tmpdir is not None:
        smet_bundle.output_data_path = str(tmpdir)
    # status = bundle.BundleStatus(smet_bundle, {"progress_func": stdout_progress})
    status = bundle.BundleStatus(smet_bundle)
    status.create_tables()
    status.sync_config()
    return status


class MockTwython(object):
    """Return search results from the file system"""

    def __init__(self, data_path):
        self.data_path = data_path
        self.results_folder_path = os.path.join(self.data_path, "chicago-mayor-runoff-2015")
        call_sequence = [os.path.join(self.results_folder_path, fn) for fn in os.listdir(self.results_folder_path)]
        # TODO Rename to original call sequences
        self.call_sequence = [fn for fn in call_sequence if os.path.isfile(fn)]
        self.call_sequence_index = 0
        self.seen_since_id_count = 0
        self.max_id_call_dict = defaultdict(dict)
        self.since_id_call_dict = defaultdict(dict)
        self.no_id_call_dict = {}
        self.initialize_call_dictionaries()
        self.requests = []

    def initialize_call_dictionaries(self):
        for call_result_path in self.call_sequence:
            with open(call_result_path) as f:
                result = json.load(f)
                key = result['search_metadata']['query']
                key = key.replace('+', ' ')
                max_id = result['search_metadata']['max_id_str']
                self.max_id_call_dict[key][max_id] = call_result_path

                since_id = result['search_metadata']['since_id_str']
                if since_id:
                    self.since_id_call_dict[key][since_id] = call_result_path
                if key not in self.no_id_call_dict:
                    self.no_id_call_dict[key] = call_result_path

    def results_path(self, search_term, max_id):
        return self.max_id_call_dict[search_term][max_id]

    def search(self, q, include_entities, result_type, count, since_id=None, max_id=None):

        if max_id is not None:
            result_path = self.results_path(q, max_id)
        elif since_id is not None:
            result_path = self.since_id_call_dict[q][str(since_id)]
        else:
            result_path = self.no_id_call_dict[q]
        self.requests.append({'q': q, 'since_id': since_id, 'max_id': max_id, 'path': result_path})

        assert result_path is not None
        with open(result_path) as f:
            result = json.load(f)
        if max_id is not None:
            expected_max_id = result['search_metadata']['max_id_str']
            assert max_id == expected_max_id
        elif since_id is not None:
            self.seen_since_id_count += 1
            expected_since_id = long(result['search_metadata']['since_id'])
            assert since_id == expected_since_id
        self.call_sequence_index += 1
        return result

    def get_lastfunction_header(self, header):
        if 'x-rate-limit-remaining' == header:
            return 100 - self.call_sequence_index
        elif 'x-rate-limit-reset' == header:
            return (datetime.utcnow() + timedelta(minutes=10) - datetime(1970, 1, 1)).total_seconds()
        else:
            assert header is not None, "Unknown header"


def test_status(smet_bundle, tmpdir):
    status = initialized_bundle_status(smet_bundle, tmpdir)

    races = status.races()
    assert len(races) == 1
    chicago = races[0]
    assert chicago.name == "Chicago Mayor Runoff 2015"
    assert chicago.year == 2015

    candidates = chicago.candidates.all()
    assert len(candidates) == 2
    rahm = candidates[0]
    assert rahm.name == 'Rahm Emanuel'
    chuy = candidates[1]
    assert len(chuy.search_terms.all()) == 2


def test_collector(smet_bundle, tmpdir):
    # Setup
    status = initialized_bundle_status(smet_bundle, tmpdir)

    # Run the collector
    collector = collect.TweetCollector(status)
    mock_twython = MockTwython(results_cache_path())
    collector.twitter = mock_twython
    collector.run()

    # The results are stored in a directory for the run
    race_output_dir = race_output_folder_path(tmpdir)
    first_run_output_dir = race_output_dir.listdir()[0]
    number_of_search_outputs = len(first_run_output_dir.listdir())
    assert number_of_search_outputs < len(os.listdir(mock_twython.results_folder_path))
    assert number_of_search_outputs == number_of_results_at_max_depth_5

    races = status.races()
    chicago = races[0]

    # There should be one run
    assert len(chicago.runs.all()) == 1
    first_run = chicago.runs.all()[0]
    assert first_run.results_folder == first_run_output_dir.basename
    assert first_run.start == bundle.run_folder_name_to_datetime(first_run_output_dir.basename)
    assert first_run.end is not None

    candidates = chicago.candidates.all()
    rahm = candidates[0]
    chuy = candidates[1]

    # Check that the searches were made
    searches = []
    for search_term in rahm.search_terms.all():
        searches.extend(search_term.searches.all())
    for search_term in chuy.search_terms.all():
        searches.extend(search_term.searches.all())
    assert len(searches) == mock_twython.call_sequence_index

    # Check the results of the searches
    for search in searches:
        source_path = mock_twython.results_path(search.search_term.term, str(search.max_id))
        assert search.run == first_run
        with open(source_path) as f:
            source_data_str = f.read()
            source_data = json.loads(source_data_str)
            assert search.max_id == long(source_data['search_metadata']['max_id'])
            retrieved_data_str = first_run_output_dir.join(search.results_path).read()
            assert len(retrieved_data_str) < len(source_data_str)
            # Check that the search results is the non-pretty-printed version of the source
            compressed_source_str = json.dumps(source_data, separators=(',', ': '))
            assert retrieved_data_str == compressed_source_str

    mock_twython_contd = MockTwython(results_continuation_cache_path())
    collector.twitter = mock_twython_contd
    collector.config.wait_period = 0
    collector.run()

    # There should now be two runs
    assert len(chicago.runs.all()) == 2

    second_run_output_dir = race_output_dir.listdir()[1]
    assert len(second_run_output_dir.listdir()) < len(os.listdir(mock_twython_contd.results_folder_path))
    assert mock_twython_contd.seen_since_id_count == 3


def test_collector_resuming(smet_bundle, tmpdir):
    # Setup
    status = initialized_bundle_status(smet_bundle, tmpdir)

    # Run the collector
    collector = collect.TweetCollector(status)
    mock_twython = MockTwython(results_cache_path())
    collector.twitter = mock_twython
    collector.run()

    # The results are stored in a directory for the run
    race_output_dir = race_output_folder_path(tmpdir)
    first_run_output_dir = race_output_dir.listdir()[0]
    number_of_search_outputs = len(first_run_output_dir.listdir())
    assert number_of_search_outputs < len(os.listdir(mock_twython.results_folder_path))
    assert number_of_search_outputs == number_of_results_at_max_depth_5

    races = status.races()
    chicago = races[0]

    # There should be one run
    assert len(chicago.runs.all()) == 1
    first_run = chicago.runs.all()[0]
    assert first_run.results_folder == first_run_output_dir.basename
    assert first_run.start == bundle.run_folder_name_to_datetime(first_run_output_dir.basename)
    assert first_run.end is not None

    # Resume the run
    collector.resume = True
    collector.run()

    # There should still be one run
    assert len(chicago.runs.all()) == 1
    first_run = chicago.runs.all()[0]
    assert first_run.results_folder == first_run_output_dir.basename
    assert first_run.start == bundle.run_folder_name_to_datetime(first_run_output_dir.basename)
    assert first_run.end is not None

    # And resume again
    collector.run()

    # We should now have all the results
    assert len(first_run_output_dir.listdir()) == len(os.listdir(mock_twython.results_folder_path))


def test_collector_resuming_no_previous_runs(smet_bundle, tmpdir):
    # Setup
    status = initialized_bundle_status(smet_bundle, tmpdir)

    # Run the collector
    collector = collect.TweetCollector(status)
    mock_twython = MockTwython(results_cache_path())
    collector.twitter = mock_twython
    collector.resume = True
    collector.run()

    # The results are stored in a directory for the run
    race_output_dir = race_output_folder_path(tmpdir)
    assert not race_output_dir.exists()

    races = status.races()
    chicago = races[0]

    # There should be no runs
    assert len(chicago.runs.all()) == 0


def test_collector_unlimited_depth(smet_bundle, tmpdir):
    # Setup
    status = initialized_bundle_status(smet_bundle, tmpdir)

    # Run the collector
    config = collect.CollectorConfig(max_depth=None)
    collector = collect.TweetCollector(status, config)
    mock_twython = MockTwython(results_cache_path())
    collector.twitter = mock_twython
    collector.run()

    # The results are stored in a directory for the run
    race_output_dir = race_output_folder_path(tmpdir)
    first_run_output_dir = race_output_dir.listdir()[0]
    assert len(first_run_output_dir.listdir()) == len(os.listdir(mock_twython.results_folder_path))

    races = status.races()
    chicago = races[0]

    # There should be one run
    assert len(chicago.runs.all()) == 1
    first_run = chicago.runs.all()[0]
    assert first_run.results_folder == first_run_output_dir.basename
    assert first_run.start == bundle.run_folder_name_to_datetime(first_run_output_dir.basename)
    assert first_run.end is not None

    candidates = chicago.candidates.all()
    rahm = candidates[0]
    chuy = candidates[1]

    # Check that the searches were made
    searches = []
    for search_term in rahm.search_terms.all():
        searches.extend(search_term.searches.all())
    for search_term in chuy.search_terms.all():
        searches.extend(search_term.searches.all())
    assert len(searches) == len(mock_twython.call_sequence)

    # Check the results of the searches
    for search, source_path in zip(searches, mock_twython.call_sequence):
        assert search.run == first_run
        with open(source_path) as f:
            source_data_str = f.read()
            source_data = json.loads(source_data_str)
            assert search.max_id == long(source_data['search_metadata']['max_id'])
            retrieved_data_str = first_run_output_dir.join(search.results_path).read()
            assert len(retrieved_data_str) < len(source_data_str)
            # Check that the search results is the non-pretty-printed version of the source
            compressed_source_str = json.dumps(source_data, separators=(',', ': '))
            assert retrieved_data_str == compressed_source_str

    mock_twython_contd = MockTwython(results_continuation_cache_path())
    collector.twitter = mock_twython_contd
    collector.config.wait_period = 0
    collector.run()

    # There should now be two runs
    assert len(chicago.runs.all()) == 2

    second_run_output_dir = race_output_dir.listdir()[1]
    assert len(second_run_output_dir.listdir()) == len(os.listdir(mock_twython_contd.results_folder_path))
    assert mock_twython_contd.seen_since_id_count == 3


def test_collector_wait_period(smet_bundle, tmpdir):
    status = initialized_bundle_status(smet_bundle, tmpdir)

    collector = collect.TweetCollector(status)
    mock_twython = MockTwython(results_cache_path())
    collector.twitter = mock_twython
    collector.run()

    mock_twython_contd = MockTwython(results_continuation_cache_path())
    collector.twitter = mock_twython_contd
    collector.run()

    race_output_dir = race_output_folder_path(tmpdir)
    first_run_output_dir = race_output_dir.listdir()[0]
    second_run_output_dir = race_output_dir.listdir()[1]
    assert len(race_output_dir.listdir()) == 2
    assert len(first_run_output_dir.listdir()) == number_of_results_at_max_depth_5
    assert len(second_run_output_dir.listdir()) == 0
    assert mock_twython_contd.seen_since_id_count == 0


def test_importer(smet_bundle, tmpdir, smet_bundle2):
    # First create an initial run structure to import
    test_collector_resuming(smet_bundle, tmpdir)

    # Import the bundle created above into a new db
    status = initialized_bundle_status(smet_bundle2)
    chicago = status.races()[0]
    assert 0 == len(chicago.runs.all())

    # Importing from the bundle does not import anything
    importer = collect.RawImport(status)
    importer.run()
    assert 0 == len(chicago.runs.all())
    assert 0 == len(importer.skipped_run_folders)
    assert 0 == len(importer.imported_run_folders)

    # Importing from the other bundle works
    importer = collect.RawImport(status, str(tmpdir))
    importer.run()
    assert 0 == len(importer.skipped_run_folders)
    assert 1 == len(importer.imported_run_folders)

    # There should be one run
    assert len(chicago.runs.all()) == 1
    status_other = bundle.BundleStatus(smet_bundle)
    assert len(status.races()) == len(status_other.races())
    other_chicago = status_other.races()[0]
    runs, other_runs = list(zip(chicago.runs.all(), other_chicago.runs.all()))[0]
    assert runs.start == other_runs.start

    assert len(runs.searches.all()) == len(other_runs.searches.all())

    for search, other_search in zip(runs.searches.all(), other_runs.searches.all()):
        assert search != other_search
        assert search.date == other_search.date
        assert search.max_id == other_search.max_id

    # Re-importing a successfully-imported folder should skip all runs
    importer = collect.RawImport(status)
    importer.run()

    assert 1 == len(importer.skipped_run_folders)
    assert 0 == len(importer.imported_run_folders)


def test_importer_directly_from_bundle(smet_bundle, tmpdir, smet_bundle2):
    # First create an initial run structure to import
    test_collector_resuming(smet_bundle, tmpdir)

    # Import the bundle created above into a new db
    status = initialized_bundle_status(smet_bundle2, tmpdir)
    chicago = status.races()[0]
    assert 0 == len(chicago.runs.all())

    # Importing from the bundle does not import anything
    importer = collect.RawImport(status)
    importer.run()
    assert 0 == len(importer.skipped_run_folders)
    assert 1 == len(importer.imported_run_folders)

    # There should be one run
    assert len(chicago.runs.all()) == 1
    status_other = bundle.BundleStatus(smet_bundle)
    assert len(status.races()) == len(status_other.races())
    other_chicago = status_other.races()[0]
    runs, other_runs = list(zip(chicago.runs.all(), other_chicago.runs.all()))[0]
    assert runs.start == other_runs.start


# TODO Add a test for the progress func

def retrieve_test_data():
    """Call the twitter API to retrieve and store some test data"""

    output_path = results_cache_path()
    if not os.path.isdir(output_path):
        os.makedirs(output_path)

    module_path = os.path.dirname(__file__)
    root_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(module_path))))

    smet_bundle = conftest.bundle()
    # Use real credentials for retrieving the data
    smet_bundle.credentials_path = os.path.join(os.path.join(root_path, 'config'), 'credentials.yaml')

    status = bundle.BundleStatus(smet_bundle)
    status.create_tables()
    status.sync_config()

    collector = collect.TweetCollector(status,
                                       save_func=collect.pretty_print_results_save_func)
    collector.run()


def retrieve_test_data_continuation():
    """Call the twitter API to retrieve and store some test data"""

    # The structure of the data on the file system has changed since the last rung
    # If new data is acquired, the MockTwython will need to be updated.

    output_path = results_cache_path()
    if not os.path.isdir(output_path):
        os.makedirs(output_path)

    module_path = os.path.dirname(__file__)
    root_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(module_path))))

    smet_bundle = conftest.bundle()
    # Use real credentials for retrieving the data
    smet_bundle.credentials_path = os.path.join(os.path.join(root_path, 'config'), 'credentials.yaml')

    status = bundle.BundleStatus(smet_bundle)
    status.create_tables()
    status.sync_config()

    importer = collect.RawImport(status)
    importer.run()

    # Do a new run
    collector = collect.TweetCollector(status)
    collector.run()


if __name__ == '__main__':
    # retrieve_test_data()
    # retrieve_test_data_continuation()
    pass
