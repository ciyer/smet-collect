#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
jq.py

A module with helper classes and functions for interacting with spark.

Created by Chandrasekhar Ramakrishnan on 2015-11-02.
Copyright (c) 2015 Chandrasekhar Ramakrishnan. All rights reserved.
"""
# python2 support
import abc
import os
import subprocess
import time
from datetime import datetime
from os.path import dirname

from builtins import str


def log_command_execution(log_file, cmd):
    now_str = datetime.utcnow().strftime("%H:%M:%S")
    log_file.write("\n{} ==== Executing {}\n".format(now_str, " ".join(cmd)))
    log_file.flush()


class JqSubprocess(object):
    """Represents a running jq process"""

    def __init__(self, cmd, log_file_path):
        self.log_file_path = log_file_path
        self.cmd = cmd
        self.log_file = None
        self.subprocess = None
        self.return_code = None

    def start(self):
        self.log_file = log_file = open(self.log_file_path, "w+")
        log_command_execution(log_file, self.cmd)
        self.subprocess = subprocess.Popen(self.cmd, stdout=log_file, stderr=log_file, shell=False)

    def wait(self):
        self.return_code = self.subprocess.wait()

    def poll(self):
        self.return_code = self.subprocess.poll()
        return self.return_code

    def cleanup(self):
        if self.log_file is None:
            return
        # Close the file
        self.log_file.close()
        self.log_file = None


class JqProxy(object):
    """Interface for running jq commands"""

    def __init__(self, status, script_parent_folder):
        self.status = status
        self.log_file = None
        self.script_parent_folder = script_parent_folder

    @staticmethod
    def run_command(cmd, log_file_path_or_file):
        """
        :param cmd: Command to run
        :param log_file_path_or_file: Path of file or file to log to
        :return: The return code from the command
        """
        if isinstance(log_file_path_or_file, str):
            with open(log_file_path_or_file, "w+") as log_file:
                log_command_execution(log_file, cmd)
                return subprocess.call(cmd, stdout=log_file, stderr=log_file, shell=False)
        else:
            log_file = log_file_path_or_file
            log_command_execution(log_file_path_or_file, cmd)
            return subprocess.call(cmd, stdout=log_file, stderr=log_file, shell=False)

    @staticmethod
    def prepare_command_async(cmd, log_file_path):
        """
        :param cmd: Command to run
        :param log_file: Path of file to log to
        :return: a SparkSubprocess object
        """
        return JqSubprocess(cmd, log_file_path)

    def command_available(self, cmd):
        """Check that the command is available.

        If the command is not available, log an error to the progress function.
        :return True if the command is available.
        """
        cmd_path = self.path_for_script(cmd)
        with open("/dev/null") as f:
            if 0 != subprocess.call(["type", cmd_path], stdout=f, stderr=f, shell=True):
                self.status.progress_func({'type': 'error', 'message': "{} not found in path.".format(cmd)})
                return False
        return True

    def prerequisites_satisfied(self):
        """Check that all the prereqs necessary for this class to run are fulfilled

        :return True if processing can run, False if it cannot.
        """

        # Check that all the spark commands we need are available
        can_run = True
        for cmd in ["mdsummary.rb", "prune.rb"]:
            if not self.command_available(cmd):
                can_run = False
                break
        return can_run

    def start_run(self, log_file_path):
        self.log_file = open(log_file_path, "w+")

    def stop_run(self):
        self.log_file.close()

    def path_for_script(self, cmd):
        return os.path.join(self.script_parent_folder, cmd)

    def jq_submit(self, log_file_path, script, cmd_args, async):
        """
        Run a jq script
        :param log_file_path: The file to log output to.
        :param script: the base-name of the command to run
        :param cmd_args: A list with arguments to the class.
        :param async: Pass true if the command should be run asynchronously.
        :return:
        """
        cmd = [self.path_for_script(script)]
        cmd.extend(cmd_args)
        log_command_execution(self.log_file, cmd)
        if async:
            return self.prepare_command_async(cmd, log_file_path)
        else:
            subprocess = self.prepare_command_async(cmd, log_file_path)
            subprocess.start()
            subprocess.wait()
            subprocess.cleanup()
            return subprocess

    def submit(self, log_file_path, cmd, cmd_args, async):
        """
        Run a jq command
        :param log_file_path: The file to log output to.
        :param cmd: The command to run
        :param cmd_args: A list with arguments to the command.
        :param async: Pass true if the command should be run asynchronously.
        :return:
        """
        return self.jq_submit(log_file_path, cmd, cmd_args, async)


class JqEngineConfig(object):
    """Configuration parameters for a spark command"""

    def __init__(self, cmd_parent_folder=None):
        """
        :param cmd_parent_folder: The folder where the command scripts are located
        """
        if cmd_parent_folder is not None:
            self.script_parent_folder = cmd_parent_folder
        else:
            self.script_parent_folder = self.default_script_parent_folder()

    @staticmethod
    def default_script_parent_folder():
        module_path = dirname(__file__)
        src_path = dirname(dirname(dirname(dirname(dirname(module_path)))))
        path = os.path.join(src_path, "ruby")
        return path


class JqEngine(object):
    """Engine that executes jq commands in the shell"""

    def __init__(self, status, config):
        """
        Initialize the JqEngine.
        :param status: The CollectorStatus object that tracks status state
        :param config: The configuration for the pruner
        :return:
        """
        self.status = status
        self.config = config
        # TODO This should be refactored into a runner object, but this will do for now.
        self.cmd = None

        self.jq_proxy = JqProxy(status, config.script_parent_folder)
        self.log_file_path = None
        self.queued_processes = []
        self.running_processes = []
        self.max_number_running_processes = 10
        self.completed_processes = []

    def prerequisites_satisfied(self):
        """Check that all the prerequisites necessary for this class to run are fulfilled

        :return True if pruner can run, False if it cannot.
        """

        # Check that all the spark commands we need are available
        return self.jq_proxy.prerequisites_satisfied()

    def start_run(self):
        self.log_file_path = self.status.generate_running_log_file_path("jq")
        self.jq_proxy.start_run(self.log_file_path)

    def stop_run(self):
        self.jq_proxy.stop_run()
        failed_processes = [proc for proc in self.completed_processes if proc.return_code != 0]
        if failed_processes:
            self.status.move_log_to_fail(self.log_file_path)
        else:
            self.status.move_log_to_success(self.log_file_path)
        self.queued_processes = []
        self.running_processes = []
        self.completed_processes = []

    def submit_spark_tasks(self, slug, script, async=False):
        """
        Run a jq command
        :param slug: A slug that identifies this spark command.
        :param script: The jq command to run
        :param async: Pass true if the command should be run asynchronously.
        :return: Either the return code of the command or a JqSubprocess obj
        """
        log_file_path = self.status.generate_running_log_file_path("run")
        cmd_config = self.cmd.generate_task_config(slug)

        if self.cmd.config.just_config:
            return

        cmd_args = [cmd_config.default_path()]
        result = self.jq_proxy.submit(log_file_path, script, cmd_args, async)
        if async:
            self.queued_processes.append(result)
        else:
            self.completed_processes.append(result)
        return result

    def spark_submit(self, slug, cmd, cmd_args, async=False):
        """
        Submit a job to spark.
        :param slug: A slug that identifies this spark command.
        :param cmd_class: The Scala class that implements to command.
        :param cmd_args: A list with arguments to the class.
        :param async: Pass true if the command should be run asynchronously.
        :return: Either the return code of the command or a SparkSubprocess obj
        """
        log_file_path = self.status.generate_running_log_file_path("submit")
        result = self.jq_proxy.submit(log_file_path, cmd, cmd_args, async)
        if async:
            self.queued_processes.append(result)
        else:
            self.completed_processes.append(result)
        return result

    def process_spark_queue(self):
        """Check if the queued commands have completed yet, if not keep polling"""
        running_procs = self.running_processes
        while self.queued_processes or running_procs:
            for proc in running_procs:
                if proc.poll() is not None:
                    self.running_processes.remove(proc)
                    proc.cleanup()
                    self.completed_processes.append(proc)
            while self.queued_processes and len(self.running_processes) <= self.max_number_running_processes:
                first = self.queued_processes[0]
                self.queued_processes.remove(first)
                first.start()
                self.running_processes.append(first)
            running_procs = self.running_processes
            if running_procs:
                time.sleep(1)
        for proc in self.completed_processes:
            if proc.return_code != 0:
                self.status.move_log_to_fail(proc.log_file_path)
            else:
                self.status.move_log_to_success(proc.log_file_path)

    def do_processing(self):
        """Really process the races"""
        if not self.cmd.config.just_config:
            self.start_run()

        self.cmd.queue_tasks()
        self.start_processing()
        self.process_spark_queue()

        if not self.cmd.config.just_config:
            self.stop_run()

    @abc.abstractmethod
    def queue_processing(self, race, run):
        """Queue the parameters needed for processing the race/run."""
        return

    def start_processing(self):
        self.submit_spark_tasks(self.cmd.config.jq_script, self.cmd.config.jq_script, async=True)

    def run(self, cmd):
        """Run the command for matching races"""
        self.cmd = cmd
        self.cmd.collect_runs_to_process()
        self.cmd.log_intermediate_progress_update()
        self.status.ensure_folder_exists(self.status.tmp_folder_path())
        self.do_processing()
        msg = '{} finished'.format(self.cmd.process_description())
        self.status.progress_func({'type': 'progress', 'message': msg})

    def run_without_collect(self, cmd):
        """Run the engine on the command, but do not collect runs to process"""
        self.cmd = cmd
        self.status.ensure_folder_exists(self.status.tmp_folder_path())
        self.do_processing()
