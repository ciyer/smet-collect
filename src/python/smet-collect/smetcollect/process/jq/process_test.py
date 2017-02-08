#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
process_test.py

Test of all processing. More convenient to keep in one place because of the dependencies.


Created by Chandrasekhar Ramakrishnan on 2016-01-17.
Copyright (c) 2016 Chandrasekhar Ramakrishnan. All rights reserved.
"""

import json

from . import jq
from ...process import analyze, prune
from ...collect import collect
from ...collect import collect_test
from ...collect import compress


def race_pruned_data_folder_path(tmpdir):
    return tmpdir.join("pruned", "chicago-mayor-runoff-2015")


def race_compressed_data_folder_path(tmpdir):
    return tmpdir.join("compressed", "chicago-mayor-runoff-2015")


def race_analyzed_data_folder_path(tmpdir):
    return tmpdir.join("analyzed", "chicago-mayor-runoff-2015", "metadata")


def race_analyzed_hashtag_data_folder_path(tmpdir):
    return tmpdir.join("analyzed", "chicago-mayor-runoff-2015", "hashtag")


def race_log_folder_path(tmpdir):
    return tmpdir.join("log", "chicago-mayor-runoff-2015")


def setup_bundle(smet_bundle, tmpdir):
    # Setup
    status = collect_test.initialized_bundle_status(smet_bundle, tmpdir)

    # Run the collector
    collector = collect.TweetCollector(status)
    mock_twython = collect_test.MockTwython(collect_test.results_cache_path())
    collector.twitter = mock_twython
    collector.run()

    # Run the collector again
    mock_twython_contd = collect_test.MockTwython(collect_test.results_continuation_cache_path())
    collector.twitter = mock_twython_contd
    collector.config.wait_period = 0
    collector.run()

    return status


def test_processes(smet_bundle, tmpdir):
    status = setup_bundle(smet_bundle, tmpdir)
    run_process_test(status, tmpdir)


def run_process_test(status, tmpdir):
    run_pruner_test(status, tmpdir)
    run_compressor_test(status, tmpdir)
    run_analyzer_test(status, tmpdir)
    run_hashtag_analyzer_test(status, tmpdir)


def run_pruner_test(status, tmpdir):
    engine = jq.JqEngine(status, jq.JqEngineConfig())
    pruner = prune.Pruner(status)
    engine.run(pruner)

    log_dir = tmpdir.join("log")
    # There should be one dir for running, one for success
    assert 2 == len(log_dir.listdir())
    running_dir = log_dir.join("running")
    assert 0 == len(running_dir.listdir())
    success_dir = log_dir.join("succeeded")
    # There is one log for the master and one log for each run
    assert 2 == len(success_dir.listdir())

    race_output_dir = race_pruned_data_folder_path(tmpdir)
    # There should be one pruned data dir for each run
    assert 2 == len(race_output_dir.listdir())


def run_compressor_test(status, tmpdir):
    # Compress the run data
    compressor = compress.Compressor(status)
    compressor.run()

    compressed_output_dir = race_compressed_data_folder_path(tmpdir)
    assert 2 == len(compressed_output_dir.listdir())

    # Remove runs that have been compressed
    archiver = compress.Archiver(status)
    archiver.run()
    raw_output_dir = collect_test.race_output_folder_path(tmpdir)
    assert 0 == len(raw_output_dir.listdir())


def check_summary_dict(run, correct, keys):
    for key in keys:
        assert run[key] == correct[run['name']][key], run['name'] + " : " + key


def check_analyzer_output(analyzer_output_dir):
    # There should be one pruned data dir for each run
    assert 2 == len(analyzer_output_dir.listdir())
    with analyzer_output_dir.listdir()[0].open() as f:
        run1 = json.load(f)
    assert len(run1) == 2
    keys = ["idcount", "user_idcount", "rt_idcount", "rt_rtcount", "min_datetime", "max_datetime"]
    run_results = {'Rahm Emanuel': {"idcount": 500,
                                    "user_idcount": 393,
                                    "rt_idcount": 56,
                                    "rt_rtcount": 334,
                                    "min_datetime": "Wed Aug 05 18:48:36 +0000 2015",
                                    "max_datetime": "Sat Aug 08 22:31:36 +0000 2015"},
                   'Jesus G. "Chuy" Garcia': {"idcount": 93,
                                              "user_idcount": 74,
                                              "rt_idcount": 15,
                                              "rt_rtcount": 4027,
                                              "min_datetime": "Fri Jul 31 14:05:32 +0000 2015",
                                              "max_datetime": "Sat Aug 08 21:34:08 +0000 2015"}}
    check_summary_dict(run1[0], run_results, keys)
    check_summary_dict(run1[1], run_results, keys)

    with analyzer_output_dir.listdir()[1].open() as f:
        run2 = json.load(f)
    assert len(run2) == 2
    run_results = {'Rahm Emanuel': {"idcount": 500,
                                    "user_idcount": 437,
                                    "rt_idcount": 67,
                                    "rt_rtcount": 156,
                                    "min_datetime": "Wed Sep 09 11:16:57 +0000 2015",
                                    "max_datetime": "Fri Sep 11 10:43:23 +0000 2015"},
                   'Jesus G. "Chuy" Garcia': {"idcount": 107,
                                              "user_idcount": 84,
                                              "rt_idcount": 24,
                                              "rt_rtcount": 815,
                                              "min_datetime": "Wed Sep 02 02:06:59 +0000 2015",
                                              "max_datetime": "Fri Sep 11 03:35:01 +0000 2015"}}

    check_summary_dict(run2[0], run_results, keys)
    check_summary_dict(run2[1], run_results, keys)


def check_hashtag_analyzer_output(analyzer_output_dir):
    # There should be one pruned data dir for each run
    assert 2 == len(analyzer_output_dir.listdir())
    with analyzer_output_dir.listdir()[0].open() as f:
        run1 = json.load(f)
    assert run1[1]['name'] == 'Rahm Emanuel'
    assert run1[1]['counts'][0]['tag'] == "jonvoyage"
    assert run1[1]['counts'][0]['rtc'] == 182

    assert run1[0]['name'] == 'Jesus G. \"Chuy\" Garcia'
    assert run1[0]['counts'][0]['tag'] == "blue1647"
    assert run1[0]['counts'][0]['rtc'] == 20

    with analyzer_output_dir.listdir()[1].open() as f:
        run2 = json.load(f)
    assert run2[1]['name'] == 'Rahm Emanuel'
    assert run2[1]['counts'][0]['tag'] == "fightfordyett"
    assert run2[1]['counts'][0]['rtc'] == 235

    assert run2[0]['name'] == 'Jesus G. \"Chuy\" Garcia'
    assert run2[0]['counts'][0]['tag'] == "chibudget2016"
    assert run2[0]['counts'][0]['rtc'] == 2


def run_analyzer_test(status, tmpdir):
    engine = jq.JqEngine(status, jq.JqEngineConfig())
    analyzer = analyze.GenericAnalyzer(status, analyze.MetadataAnalyzerConfig(status))
    engine.run(analyzer)

    log_dir = tmpdir.join("log")
    # There should be one dir for running, one for success
    assert 2 == len(log_dir.listdir())
    running_dir = log_dir.join("running")
    assert 0 == len(running_dir.listdir())
    success_dir = log_dir.join("succeeded")
    # There is one log for the master and one log for each run
    assert 4 == len(success_dir.listdir())

    race_output_dir = race_analyzed_data_folder_path(tmpdir)
    check_analyzer_output(race_output_dir)


def run_hashtag_analyzer_test(status, tmpdir):
    engine = jq.JqEngine(status, jq.JqEngineConfig())
    analyzer = analyze.GenericAnalyzer(status, analyze.HashtagAnalyzerConfig(status))
    engine.run(analyzer)

    log_dir = tmpdir.join("log")
    # There should be one dir for running, one for success
    assert 2 == len(log_dir.listdir())
    running_dir = log_dir.join("running")
    assert 0 == len(running_dir.listdir())
    success_dir = log_dir.join("succeeded")
    # There is one log for the master and one log for each run
    assert 6 == len(success_dir.listdir())

    race_output_dir = race_analyzed_hashtag_data_folder_path(tmpdir)
    check_hashtag_analyzer_output(race_output_dir)


def test_write_candidate_status(smet_bundle, tmpdir):
    status = collect_test.initialized_bundle_status(smet_bundle, tmpdir)
    config_to_json = analyze.CandidateConfigToJson(status)
    path = tmpdir.join("candidates.json")
    config_to_json.save(str(path))

    result = json.load(path)
    assert 1 == len(result)
    assert 2 == len(result[0]["candidates"])
