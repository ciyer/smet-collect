#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
compress.py

Find runs that have already been pruned and compress them.

Created by Chandrasekhar Ramakrishnan on 2015-11-06.
Copyright (c) 2015 Chandrasekhar Ramakrishnan. All rights reserved.
"""

import os
import shutil
import subprocess
import tarfile
import json
from collections import defaultdict

from ..bundle import slug_for_race
from ..bundle.status_db import Run
from ..process.prune import Pruner
from ..process.jq import JqEngineConfig, JqEngine


class CompressorConfig(object):
    """Gathers configuration information for the TweetCollector"""

    def __init__(self, max_depth=5):
        """
        :param max_depth: The maximum number of runs per race to compress. Use None or non-positive to compress all.
        """
        self.max_depth = max_depth if max_depth > 1 else None


class Compressor(object):
    """Compress raw data"""

    def __init__(self, status, config=None, race=None):
        """Constructor for the compressor collector
        :param status: The CompressorConfig object that tracks status state
        :param config: The configuration for the compressor
        :param race: The slug for a race if should restrict to one race
        """
        self.status = status
        self.config = config if config else CompressorConfig()
        self.race_slug = race
        self.runs_to_compress = defaultdict(list)

    def run(self):
        """Run pruning for matching races"""
        self.collect_runs_to_compress()
        self.log_intermediate_progress_update()
        self.do_compress()
        msg = 'Compressing finished'
        self.status.progress_func({'type': 'progress', 'message': msg})

    def log_intermediate_progress_update(self):
        races = self.runs_to_compress.keys()
        if len(races) < 1:
            self.status.progress_func({'type': 'progress', 'message': "No runs to compress."})
            return
        for key in races:
            runs = self.runs_to_compress[key]
            msg = "Race {} has {} runs to compress".format(key.name.encode('utf-8'), len(runs))
            self.status.progress_func({'type': 'progress', 'message': msg})
        if self.config.max_depth is not None:
            msg = "\tLimiting to {} runs per race".format(self.config.max_depth)
            self.status.progress_func({'type': 'progress', 'message': msg})

    def do_compress(self):
        """Really compress the runs"""
        races = self.runs_to_compress.keys()
        for race in races:
            self.status.ensure_folder_exists(self.status.compressed_data_folder_path_for_race(race))
            runs = self.runs_to_compress[race]
            if self.config.max_depth:
                runs = runs[0:self.config.max_depth]
            for run in runs:
                self.compress_run(race, run)

    def compress_run(self, race, run):
        raw_data_path = self.status.raw_data_folder_path_for_run(race, run)
        if not os.path.exists(raw_data_path):
            msg = "No run found at {}. Skipping...".format(self.status.path_relative_to_bundle(raw_data_path).encode('utf-8'))
            self.status.progress_func({'type': 'compress', 'message': msg})
            return
        compressed_data_path = self.status.compressed_data_file_path_for_run(race, run)
        msg = "Compressing run {} to {}".format(
            self.status.path_relative_to_bundle(raw_data_path).encode('utf-8'),
            self.status.path_relative_to_bundle(compressed_data_path).encode('utf-8'))
        self.status.progress_func({'type': 'compress', 'message': msg})
        subprocess.call(['tar', '-cjf', compressed_data_path,
                         '-C', self.status.raw_data_folder_path_for_race(race),
                         run.results_folder])
        if not self.verify_archive(run, raw_data_path, compressed_data_path):
            self.status.progress_func({'type': 'compress', 'message': "Removing corrupt archive..."})
            os.remove(compressed_data_path)
            self.status.progress_func({'type': 'compress', 'message': "Done."})

    def verify_archive(self, run, raw_data_path, compressed_data_path):
        """Check that the archive is ok. Return True if it is, False if there is a problem."""
        archive = tarfile.open(compressed_data_path)
        for path in os.listdir(raw_data_path):
            try:
                archive_info = archive.getmember(os.path.join(run.results_folder, path))
                if archive_info.size < 0:
                    msg = "File {} is corrupt in archive".format(path)
                    self.status.progress_func({'type': 'compress', 'message': msg})
                    return False
            except KeyError:
                msg = "File {} is not in archive".format(path)
                self.status.progress_func({'type': 'compress', 'message': msg})
                return False

        msg = "Archive verified."
        self.status.progress_func({'type': 'compress', 'message': msg})
        return True

    def collect_runs_to_compress(self):
        """Find runs that need to be compressed"""
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
            self.collect_runs_from_race(matching_races[0])
        else:
            for race in self.status.races():
                self.collect_runs_from_race(race)

    def collect_runs_from_race(self, race):
        """Prune the runs in the race down to the most relevant data
        """
        msg = "Collecting runs from race {}".format(race.name.encode('utf-8'))
        self.status.progress_func({'type': 'progress', 'message': msg})
        for run in race.runs.order_by(Run.start.desc()):
            if self.status.has_pruned_data_for_run(race, run):
                compressed_data_path = self.status.compressed_data_file_path_for_run(race, run)
                if not os.path.exists(compressed_data_path):
                    self.runs_to_compress[race].append(run)


class Uncompressor(object):
    """Uncompress raw data"""

    def __init__(self, status, config=None, race=None):
        """Constructor for the uncompressor collector
        :param status: The bundle status object
        :param config: The configuration for the uncompressor
        :param race: The slug for a race if should restrict to one race
        """
        self.status = status
        self.config = config if config else CompressorConfig()
        self.race_slug = race
        self.runs_to_uncompress = defaultdict(list)

    def run(self):
        """Run pruning for matching races"""
        self.collect_runs_to_uncompress()
        self.log_intermediate_progress_update()
        self.do_uncompress()
        msg = 'Uncompressing finished'
        self.status.progress_func({'type': 'progress', 'message': msg})

    def log_intermediate_progress_update(self):
        races = self.runs_to_uncompress.keys()
        if len(races) < 1:
            self.status.progress_func({'type': 'progress', 'message': "No runs to uncompress."})
            return
        for key in races:
            runs = self.runs_to_uncompress[key]
            msg = "Race {} has {} runs to uncompress".format(key.name, len(runs))
            self.status.progress_func({'type': 'progress', 'message': msg})
        if self.config.max_depth is not None:
            msg = "\tLimiting to {} runs per race".format(self.config.max_depth)
            self.status.progress_func({'type': 'progress', 'message': msg})

    def do_uncompress(self):
        """Really uncompress the runs"""
        races = self.runs_to_uncompress.keys()
        for race in races:
            self.status.ensure_folder_exists(self.status.raw_data_folder_path_for_race(race))
            runs = self.runs_to_uncompress[race]
            if self.config.max_depth:
                runs = runs[0:self.config.max_depth]
            for run in runs:
                self.uncompress_run(race, run)

    def uncompress_run(self, race, run):
        raw_data_path = self.status.raw_data_folder_path_for_run(race, run)
        compressed_data_path = self.status.compressed_data_file_path_for_run(race, run)
        msg = "Uncompressing run {} to {}".format(
            self.status.path_relative_to_bundle(compressed_data_path),
            self.status.path_relative_to_bundle(raw_data_path))
        self.status.progress_func({'type': 'uncompress', 'message': msg})
        subprocess.call(['tar', '-xjf', compressed_data_path,
                         '-C', self.status.raw_data_folder_path_for_race(race)])

    def collect_runs_to_uncompress(self):
        """Find runs that need to be compressed"""
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
            self.collect_runs_from_race(matching_races[0])
        else:
            for race in self.status.races():
                self.collect_runs_from_race(race)

    def collect_runs_from_race(self, race):
        """Prune the runs in the race down to the most relevant data
        """
        msg = "Collecting runs from race {}".format(race.name)
        self.status.progress_func({'type': 'progress', 'message': msg})
        for run in race.runs.order_by(Run.start.desc()):
            compressed_data_path = self.status.compressed_data_file_path_for_run(race, run)
            raw_data_path = self.status.raw_data_folder_path_for_run(race, run)
            if os.path.exists(compressed_data_path) and not os.path.exists(raw_data_path):
                self.runs_to_uncompress[race].append(run)


class Rebuilder(object):
    """Rebuild faulty pruned data. -- This is WIP and has not yet been tested."""

    def __init__(self, engine, config=None, race=None):
        """Constructor for the uncompressor collector
        :param status: The bundle status object
        :param config: The configuration for the rebuilder
        :param race: The slug for a race if should restrict to one race
        """
        self.engine = engine
        self.engine_config = engine.config
        self.status = engine.status
        self.config = config if config else CompressorConfig()
        self.race_slug = race
        self.runs_to_rebuild = defaultdict(list)

    def run(self):
        """Run rebuilding for matching races"""
        self.collect_runs_to_rebuild()
        self.log_intermediate_progress_update()
        self.do_rebuild()
        msg = 'Rebuilding finished'
        self.status.progress_func({'type': 'progress', 'message': msg})

    def log_intermediate_progress_update(self):
        races = self.runs_to_rebuild.keys()
        if len(races) < 1:
            self.status.progress_func({'type': 'progress', 'message': "No runs to rebuild."})
            return
        for key in races:
            runs = self.runs_to_rebuild[key]
            msg = "Race {} has {} runs to rebuild".format(key.name, len(runs))
            self.status.progress_func({'type': 'progress', 'message': msg})
        if self.config.max_depth is not None:
            msg = "\tLimiting to {} runs per race".format(self.config.max_depth)
            self.status.progress_func({'type': 'progress', 'message': msg})

    def do_rebuild(self):
        """Really uncompress the runs"""
        races = self.runs_to_rebuild.keys()
        for race in races:
            self.status.ensure_folder_exists(self.status.raw_data_folder_path_for_race(race))
            runs = self.runs_to_rebuild[race]
            if self.config.max_depth:
                runs = runs[0:self.config.max_depth]
            for run in runs:
                self.rebuild_run(race, run)

    def rebuild_run(self, race, run):
        raw_data_path = self.status.raw_data_folder_path_for_run(race, run)
        # Uncompress the data to get the raw data to process
        no_raw_data = not os.path.exists(raw_data_path)
        if no_raw_data:
            compressed_data_path = self.status.compressed_data_file_path_for_run(race, run)
            msg = "Uncompressing run {} to {}".format(
                self.status.path_relative_to_bundle(compressed_data_path),
                self.status.path_relative_to_bundle(raw_data_path))
            self.status.progress_func({'type': 'uncompress', 'message': msg})
            subprocess.call(['tar', '-xjf', compressed_data_path,
                             '-C', self.status.raw_data_folder_path_for_race(race)])
        pruner = Pruner(self.status)
        pruner.queue_processing(race, run)
        self.engine.run_without_collect(pruner)

    def collect_runs_to_rebuild(self):
        """Find runs that need to be compressed"""
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
            self.collect_runs_from_race(matching_races[0])
        else:
            for race in self.status.races():
                self.collect_runs_from_race(race)

    def collect_runs_from_race(self, race):
        """Prune the runs in the race down to the most relevant data
        """
        msg = "Collecting runs from race {}".format(race.name)
        self.status.progress_func({'type': 'progress', 'message': msg})
        for run in race.runs.order_by(Run.start.desc()):
            if self.should_rebuild_run(race, run):
                self.runs_to_rebuild[race].append(run)

    def should_rebuild_run(self, race, run):
        compressed_data_path = self.status.compressed_data_file_path_for_run(race, run)
        raw_data_path = self.status.raw_data_folder_path_for_run(race, run)
        has_data = os.path.exists(compressed_data_path) or os.path.exists(raw_data_path)
        if not self.status.has_pruned_data_for_run(race, run):
            return True if has_data else False
        pruned_data_path = self.status.robust_pruned_data_file_path_for_run(run)
        with open(pruned_data_path) as f:
            json_data = json.load(f)
        if len(json_data) < 1 and has_data:
            return True
        return False


class Archiver(object):
    """Deletes raw data that has been compressed already."""

    def __init__(self, status, config=None, race=None):
        """Constructor for the archiver.
        :param status: The CompressorConfig object that tracks status state
        :param config: The configuration for the archiver (a CompressorConfig)
        :param race: The slug for a race if should restrict to one race
        """
        self.status = status
        self.config = config if config else CompressorConfig()
        self.race_slug = race
        self.runs_to_archive = defaultdict(list)

    def run(self):
        """Run pruning for matching races"""
        self.collect_runs_to_archive()
        self.log_intermediate_progress_update()
        self.do_archive()
        msg = 'Archiving finished'
        self.status.progress_func({'type': 'progress', 'message': msg})

    def log_intermediate_progress_update(self):
        races = self.runs_to_archive.keys()
        if len(races) < 1:
            self.status.progress_func({'type': 'progress', 'message': "No runs to archive."})
            return
        for key in races:
            runs = self.runs_to_archive[key]
            msg = "Race {} has {} runs to archive".format(key.name.encode('utf-8'), len(runs))
            self.status.progress_func({'type': 'progress', 'message': msg})
        if self.config.max_depth is not None:
            msg = "\tLimiting to {} runs per race".format(self.config.max_depth)
            self.status.progress_func({'type': 'progress', 'message': msg})

    def do_archive(self):
        """Really archive the runs"""
        races = self.runs_to_archive.keys()
        for race in races:
            runs = self.runs_to_archive[race]
            if self.config.max_depth:
                runs = runs[0:self.config.max_depth]
            for run in runs:
                compressed_data_path = self.status.compressed_data_file_path_for_run(race, run)
                if os.path.exists(compressed_data_path):
                    self.delete_raw_run(race, run)

    def delete_raw_run(self, race, run):
        raw_data_path = self.status.raw_data_folder_path_for_run(race, run)
        msg = "Deleting raw data for run {}".format(
            self.status.path_relative_to_bundle(raw_data_path).encode('utf-8'))
        self.status.progress_func({'type': 'archive', 'message': msg})
        shutil.rmtree(raw_data_path)

    def collect_runs_to_archive(self):
        """Find runs that need to be compressed"""
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
            self.collect_runs_from_race(matching_races[0])
        else:
            for race in self.status.races():
                self.collect_runs_from_race(race)

    def collect_runs_from_race(self, race):
        """Prune the runs in the race down to the most relevant data
        """
        msg = "Collecting runs from race {}".format(race.name.encode('utf-8'))
        self.status.progress_func({'type': 'progress', 'message': msg})
        for run in race.runs.order_by(Run.start):
            compressed_data_path = self.status.compressed_data_file_path_for_run(race, run)
            raw_data_path = self.status.raw_data_folder_path_for_run(race, run)
            if self.status.has_pruned_data_for_run(race, run) and os.path.exists(compressed_data_path) \
                    and os.path.exists(raw_data_path):
                self.runs_to_archive[race].append(run)


class PurgerConfig(object):
    """Gathers configuration information for the TweetCollector"""

    def __init__(self, execute=False):
        """
        :param execute: Should the runs actually be deleted?
        """
        self.execute = execute


class Purger(object):
    """Purge defective runs."""

    def __init__(self, status, config=None, race=None):
        """Constructor for the archiver.
        :param status: The CompressorConfig object that tracks status state
        :param config: The configuration for the archiver (a CompressorConfig)
        :param race: The slug for a race if should restrict to one race
        """
        self.status = status
        self.config = config if config else PurgerConfig()
        self.race_slug = race
        self.runs_to_purge = defaultdict(list)

    def run(self):
        """Run purging for matching races/runs"""
        self.collect_runs_to_purge()
        self.log_intermediate_progress_update()
        self.do_archive()
        msg = 'Purging finished'
        self.status.progress_func({'type': 'progress', 'message': msg})

    def log_intermediate_progress_update(self):
        races = self.runs_to_purge.keys()
        if len(races) < 1:
            self.status.progress_func({'type': 'progress', 'message': "No runs to purge."})
            return
        for key in races:
            runs = self.runs_to_purge[key]
            msg = "Race {} has {} runs to purge".format(key.name, len(runs))
            self.status.progress_func({'type': 'progress', 'message': msg})

    def do_archive(self):
        """Really archive the runs"""
        races = self.runs_to_purge.keys()
        for race in races:
            runs = self.runs_to_purge[race]
            for run in runs:
                self.delete_run(race, run)

    def delete_run(self, race, run):
        raw_data_path = self.status.raw_data_folder_path_for_run(race, run)
        self.delete_folder_or_file("raw data", raw_data_path)
        pruned_data_path = self.status.pruned_data_file_path_for_run(race, run)
        self.delete_folder_or_file("pruned data", pruned_data_path)
        compressed_data_path = self.status.compressed_data_file_path_for_run(race, run)
        self.delete_folder_or_file("compressed data", compressed_data_path)
        msg = "Removing run\n\t{} : {}\n\tfrom db".format(race.slug, run.start)
        self.status.progress_func({'type': 'progress', 'message': msg})
        if self.config.execute:
            self.status.session.delete(run)
            self.status.session.commit()

    def delete_folder_or_file(self, folder_desc, folder_or_file):
        if not os.path.exists(folder_or_file):
            return

        msg = "Deleting {} for run {}".format(folder_desc, folder_or_file)
        self.status.progress_func({'type': 'progress', 'message': msg})
        if self.config.execute:
            shutil.rmtree(folder_or_file)

    def collect_runs_to_purge(self):
        """Find runs that need to be compressed"""
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
            self.collect_runs_from_race(matching_races[0])
        else:
            for race in self.status.races():
                self.collect_runs_from_race(race)

    def collect_runs_from_race(self, race):
        """Purge the runs that have no data.
        """
        msg = "Collecting runs from race {}".format(race.name)
        self.status.progress_func({'type': 'progress', 'message': msg})
        for run in race.runs.order_by(Run.start.desc()):
            compressed_data_path = self.status.compressed_data_file_path_for_run(race, run)
            raw_data_path = self.status.raw_data_folder_path_for_run(race, run)
            if not os.path.exists(compressed_data_path) \
                    and not os.path.exists(raw_data_path):
                self.runs_to_purge[race].append(run)
