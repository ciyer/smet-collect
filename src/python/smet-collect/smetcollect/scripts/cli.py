#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
cli.py


Created by Chandrasekhar Ramakrishnan on 2017-02-08.
Copyright (c) 2017 Chandrasekhar Ramakrishnan. All rights reserved.
"""

from datetime import datetime

import click
import smetcollect


def click_echo(message):
    timestamp = datetime.now().strftime("%H:%M:%S")
    click.echo("{} {}".format(timestamp, message))


def click_progress(progress_data):
    if progress_data['type'] == 'progress':
        click_echo(progress_data['message'])


def click_progress_no_ts(progress_data):
    if progress_data['type'] == 'progress':
        click.echo("{}".format(progress_data['message']))


class FileProgress(object):
    def __init__(self, filename):
        self.filename = filename
        self.log_file = None

    def open(self):
        self.log_file = open(self.filename, "w")

    def close(self):
        self.log_file.close()

    def progress(self, progress_data):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_file.write("\n{} {}".format(timestamp, progress_data['message']))
        self.log_file.flush()


def status_for_bundle(bundle_root):
    smet_bundle = smetcollect.Bundle(bundle_root)
    return smetcollect.BundleStatus(smet_bundle, {"progress_func": click_progress})


def initialized_status_for_bundle(bundle_root):
    status = status_for_bundle(bundle_root)
    status.create_tables()
    status.sync_config()
    return status


@click.group()
@click.option('-q', '--quiet', default=False, is_flag=True, help='Suppress status reporting.')
@click.pass_context
def cli(ctx, quiet):
    ctx.obj['quiet'] = quiet


@cli.command()
@click.option('--limit', default=2.0, help="The number of hours to wait before performing a new run.")
@click.option('--resume', default=False, is_flag=True, help='Resume the last run.')
@click.option('--race', default=None, help="A single race to run a search for.")
@click.option('-d', '--maxdepth', default=3, help="The max depth to search for each race.")
@click.option('-u', '--until', default=None, help="Only retrieve tweets before date (YYYY-MM-DD).")
@click.argument('bundle', type=click.Path(exists=True))
@click.pass_context
def collect(ctx, limit, resume, race, maxdepth, until, bundle):
    """Collect data for a bundle.

    Perform a search against the twitter API to get the latest data for the races in the bundle. The search
    results are stored to files in the bundle.
    """
    quiet = ctx.obj['quiet']

    if not quiet:
        if resume:
            click.echo('Continue last run for bundle {}'.format(click.format_filename(bundle)))
        else:
            click.echo('Capturing data for bundle {}'.format(click.format_filename(bundle)))

    status = initialized_status_for_bundle(bundle)
    collector_config = smetcollect.CollectorConfig(wait_period=limit, max_depth=maxdepth)
    collector = smetcollect.TweetCollector(status, collector_config, resume=resume, race=race, until=until)
    collector.run()

    if not quiet:
        click.echo('Done.')


@cli.command()
@click.argument('importroot', type=click.Path(exists=True))
@click.argument('bundle', type=click.Path(exists=True))
@click.pass_context
def import_raw(ctx, importroot, bundle):
    """Reads raw data from a bundle into status db.
    """
    quiet = ctx.obj['quiet']

    if not quiet:
        click.echo('Synchronize data for bundle {}'.format(click.format_filename(bundle)))

    status = initialized_status_for_bundle(bundle)
    collector_config = smetcollect.CollectorConfig()
    importer = smetcollect.RawImport(status, importroot, collector_config)
    importer.run()

    if not quiet:
        click.echo('Done.')


@cli.command()
@click.option('--race', default=None, help="A single race to run prune.")
@click.option('-d', '--maxdepth', default=5, help="The max number of runs to prune.")
@click.option('-s', '--spark', 'master', default=None, help="Set non-empty to use spark, otherwise jq is used")
@click.argument('bundle', type=click.Path(exists=True))
@click.pass_context
def prune(ctx, race, maxdepth, master, bundle):
    """Prune down bundle run data to the relevant data.

    :param master: If empty, use jq. If local[n], use a local
    server with n threads, or a url of the form spark://.

    """
    quiet = ctx.obj['quiet']
    status = initialized_status_for_bundle(bundle)
    pruner_config = smetcollect.process.prune.PrunerConfig(None, maxdepth)
    if master is None:
        engine_config = smetcollect.process.jq.JqEngineConfig()
        engine = smetcollect.process.jq.JqEngine(status, engine_config)
    else:
        engine_config = smetcollect.process.spark.SparkEngineConfig(master)
        engine = smetcollect.process.spark.SparkEngine(status, engine_config)
    if not quiet:
        click.echo('Pruning data for bundle {}'.format(click.format_filename(bundle)))
        if master:
            click.echo('Using spark server {}'.format(engine_config.spark_master))
        else:
            click.echo('Using jq')

    pruner = smetcollect.process.prune.Pruner(status, pruner_config, race)
    if engine.prerequisites_satisfied():
        engine.run(pruner)

    if not quiet:
        click.echo('Done.')


@cli.command()
@click.option('--race', default=None, help="A single race to run compress.")
@click.option('-d', '--maxdepth', default=5, help="The max number of runs to compress.")
@click.argument('bundle', type=click.Path(exists=True))
@click.pass_context
def compress(ctx, race, maxdepth, bundle):
    """Compress pruned runs in a bundle.
    """
    quiet = ctx.obj['quiet']
    config = smetcollect.CompressorConfig(maxdepth)
    if not quiet:
        click.echo('Compressing data for bundle {}'.format(click.format_filename(bundle)))

    status = initialized_status_for_bundle(bundle)
    compressor = smetcollect.Compressor(status, config, race)
    compressor.run()

    if not quiet:
        click.echo('Done.')


@cli.command()
@click.option('--race', default=None, help="A single race to run uncompress.")
@click.option('-d', '--maxdepth', default=5, help="The max number of runs to uncompress.")
@click.argument('bundle', type=click.Path(exists=True))
@click.pass_context
def uncompress(ctx, race, maxdepth, bundle):
    """Uncompress runs in a bundle.
    """
    quiet = ctx.obj['quiet']
    config = smetcollect.CompressorConfig(maxdepth)
    if not quiet:
        click.echo('Uncompressing data for bundle {}'.format(click.format_filename(bundle)))

    status = initialized_status_for_bundle(bundle)
    uncompressor = smetcollect.Uncompressor(status, config, race)
    uncompressor.run()

    if not quiet:
        click.echo('Done.')


@cli.command()
@click.option('--race', default=None, help="A single race to run rebuild.")
@click.option('-d', '--maxdepth', default=5, help="The max number of runs to rebuild.")
@click.argument('bundle', type=click.Path(exists=True))
@click.pass_context
def rebuild(ctx, race, maxdepth, bundle):
    """Rebuild prune data in a bundle.
    """
    quiet = ctx.obj['quiet']
    config = smetcollect.CompressorConfig(maxdepth)
    if not quiet:
        click.echo('Rebuilding data for bundle {}'.format(click.format_filename(bundle)))

    status = initialized_status_for_bundle(bundle)
    engine_config = smetcollect.JqEngineConfig()
    engine = smetcollect.JqEngine(status, engine_config)
    rebuilder = smetcollect.Rebuilder(engine, config, race)
    rebuilder.run()

    if not quiet:
        click.echo('Done.')


@cli.command()
@click.option('--race', default=None, help="A single race to run compress.")
@click.option('-d', '--maxdepth', default=5, help="The max number of runs to compress.")
@click.argument('bundle', type=click.Path(exists=True))
@click.pass_context
def archive(ctx, race, maxdepth, bundle):
    """Delete compressed runs in a bundle.
    """
    quiet = ctx.obj['quiet']
    config = smetcollect.CompressorConfig(maxdepth)
    if not quiet:
        click.echo('Archiving data for bundle {}'.format(click.format_filename(bundle)))

    status = initialized_status_for_bundle(bundle)
    archiver = smetcollect.Archiver(status, config, race)
    archiver.run()

    if not quiet:
        click.echo('Done.')


@cli.command()
@click.option('--race', help="The race with the run to purge.")
@click.option('--run', help="The run to purge")
@click.option('-e', '--execute', default=False, is_flag=True, help="Actually execute the purge.")
@click.argument('bundle', type=click.Path(exists=True))
@click.pass_context
def purge(ctx, race, run, execute, bundle):
    """Purge defective runs.
    """
    quiet = ctx.obj['quiet']

    status = initialized_status_for_bundle(bundle)

    if not quiet:
        click.echo('Purging data for bundle {}'.format(click.format_filename(bundle)))

    config = smetcollect.PurgerConfig(execute)
    purger = smetcollect.Purger(status, config)
    purger.run()

    if not quiet:
        click.echo('Done.')


@cli.command()
@click.option('-d', '--maxdepth', default=3, help="The max number of runs to analyze.")
@click.option('-s', '--skipcollect', default=False, is_flag=True, help="Skip collecting data from twitter.")
@click.argument('bundle', type=click.Path(exists=True))
@click.pass_context
def pipeline(ctx, maxdepth, skipcollect, bundle):
    """Run the full SMET pipeline once. Logs are in the bundle log folder.
    - Collect runs
    - [start spark]
    - Prune
    - Analyze
    - Compress
    - Archive
    - [stop spark]
    """
    quiet = ctx.obj['quiet']
    status = initialized_status_for_bundle(bundle)

    # Log to a file instead of stdout
    log_file_path = status.generate_running_log_file_path("pipeline")
    file_progress = FileProgress(log_file_path)
    file_progress.open()
    status.progress_func = file_progress.progress

    if not quiet:
        click.echo('{} Running pipeline for bundle {}...'.format(datetime.now().strftime("%Y-%m-%d"),
                                                                 click.format_filename(bundle)))

    # collect
    if not skipcollect:
        if not quiet:
            click_echo('-- Collecting tweets')
        collector_config = smetcollect.CollectorConfig(wait_period=1.0, max_depth=maxdepth)
        collector = smetcollect.TweetCollector(status, collector_config)
        collector.run()
    else:
        if not quiet:
            click_echo('-- Skip Collecting tweets')

    engine_config = smetcollect.JqEngineConfig()
    engine = smetcollect.JqEngine(status, engine_config)
    if not quiet:
        click.echo('Using jq')

    # prune
    if not quiet:
        click_echo('-- Pruning tweets')
    pruner_config = smetcollect.process.prune.PrunerConfig(None, maxdepth)
    pruner = smetcollect.process.prune.Pruner(status, pruner_config)
    if engine.prerequisites_satisfied():
        engine.run(pruner)

    # compress
    if not quiet:
        click_echo('-- Compressing tweets')
    compressor_config = smetcollect.CompressorConfig(maxdepth)
    compressor = smetcollect.Compressor(status, compressor_config)
    compressor.run()

    # archive -- use the same config as for the compressor
    if not quiet:
        click_echo('-- Archiving tweets')
    archiver = smetcollect.Archiver(status, compressor_config)
    archiver.run()

    file_progress.close()
    status.move_log_to_success(log_file_path)

    if not quiet:
        click_echo('Done.')


def main():
    cli(obj={})


if __name__ == '__main__':
    main()
