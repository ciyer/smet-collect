#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
task_config.py

Module for producing configuration files for analysis tasks.

Created by Chandrasekhar Ramakrishnan on 2016-04-20.
Copyright (c) 2016 Chandrasekhar Ramakrishnan. All rights reserved.
"""
import json
import os
from datetime import datetime
import six

if six.PY2:
    from urllib import quote as urllibquote
else:
    from urllib.parse import quote as urllibquote


def urlquote(string):
    return urllibquote(string).replace("%20", " ")


class AnalysisTaskConfigToJson(object):
    """Describe the task configurations as JSON"""

    def __init__(self, status, slug, taskdefs):
        """Constructor for the analyzer.
        :param status: The CollectorStatus object that tracks status state
        """
        self.status = status
        self.task_slug = slug
        self.taskdefs = taskdefs

    def default_path(self):
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        return os.path.join(self.status.tmp_folder_path(), "{}-{}.json".format(self.task_slug, timestamp))

    def save(self, path=None):
        """Write out JSON describing the candidate config to path"""

        if not path:
            path = self.default_path()
        races = []
        for race in self.status.races():
            race_dict = {"slug": race.slug, "candidates": []}
            races.append(race_dict)
            for candidate in race.candidates.all():
                terms = [urlquote(term.term) for term in candidate.search_terms.all()]
                candidate_dict = {"name": candidate.name, "terms": terms}
                race_dict["candidates"].append(candidate_dict)
        tasks = []
        for task in self.taskdefs:
            task_dict = {"raceslug": task.race_slug, "inpath": task.in_path, "outfolder": task.out_folder,
                         "outname": task.out_name}
            tasks.append(task_dict)

        with open(path, "w") as f:
            json.dump({"tasks": tasks, "races": races}, f)


class AnalysisTaskDef(object):
    """Definition of an analysis task"""

    def __init__(self, in_path, out_folder, out_name, race_slug):
        self.in_path = in_path
        self.out_folder = out_folder
        self.out_name = out_name
        self.race_slug = race_slug
