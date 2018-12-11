# -*- coding: utf-8 -*-

##
# Copyright 2018 Telefonica S.A.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
##

from collections import OrderedDict
from osm_common.dbbase import DbException

__author__ = "Alfonso Tierno"


class LcmException(Exception):
    pass


class LcmExceptionNoMgmtIP(LcmException):
    pass


def versiontuple(v):
    """utility for compare dot separate versions. Fills with zeros to proper number comparison
    package version will be something like 4.0.1.post11+gb3f024d.dirty-1. Where 4.0.1 is the git tag, postXX is the
    number of commits from this tag, and +XXXXXXX is the git commit short id. Total length is 16 with until 999 commits
    """
    filled = []
    for point in v.split("."):
        filled.append(point.zfill(16))
    return tuple(filled)


class TaskRegistry:
    """
    Implements a registry of task needed for later cancelation, look for related tasks that must be completed before
    etc. It stores a four level dict
    First level is the topic, ns, vim_account, sdn
    Second level is the _id
    Third level is the operation id
    Fourth level is a descriptive name, the value is the task class
    """

    def __init__(self):
        self.task_registry = {
            "ns": {},
            "nsi": {},
            "vim_account": {},
            "wim_account": {},
            "sdn": {},
        }

    def register(self, topic, _id, op_id, task_name, task):
        """
        Register a new task
        :param topic: Can be "ns", "nsi", "vim_account", "sdn"
        :param _id: _id of the related item
        :param op_id: id of the operation of the related item
        :param task_name: Task descriptive name, as create, instantiate, terminate. Must be unique in this op_id
        :param task: Task class
        :return: none
        """
        if _id not in self.task_registry[topic]:
            self.task_registry[topic][_id] = OrderedDict()
        if op_id not in self.task_registry[topic][_id]:
            self.task_registry[topic][_id][op_id] = {task_name: task}
        else:
            self.task_registry[topic][_id][op_id][task_name] = task
        # print("registering task", topic, _id, op_id, task_name, task)

    def remove(self, topic, _id, op_id, task_name=None):
        """
        When task is ended, it should removed. It ignores missing tasks
        :param topic: Can be "ns", "nsi", "vim_account", "sdn"
        :param _id: _id of the related item
        :param op_id: id of the operation of the related item
        :param task_name: Task descriptive name. If note it deletes all
        :return:
        """
        if not self.task_registry[topic].get(_id) or not self.task_registry[topic][_id].get(op_id):
            return
        if not task_name:
            # print("deleting tasks", topic, _id, op_id, self.task_registry[topic][_id][op_id])
            del self.task_registry[topic][_id][op_id]
        elif task_name in self.task_registry[topic][_id][op_id]:
            # print("deleting tasks", topic, _id, op_id, task_name, self.task_registry[topic][_id][op_id][task_name])
            del self.task_registry[topic][_id][op_id][task_name]
            if not self.task_registry[topic][_id][op_id]:
                del self.task_registry[topic][_id][op_id]
        if not self.task_registry[topic][_id]:
            del self.task_registry[topic][_id]

    def lookfor_related(self, topic, _id, my_op_id=None):
        task_list = []
        task_name_list = []
        if _id not in self.task_registry[topic]:
            return "", task_name_list
        for op_id in reversed(self.task_registry[topic][_id]):
            if my_op_id:
                if my_op_id == op_id:
                    my_op_id = None  # so that the next task is taken
                continue

            for task_name, task in self.task_registry[topic][_id][op_id].items():
                task_list.append(task)
                task_name_list.append(task_name)
            break
        return ", ".join(task_name_list), task_list

    def cancel(self, topic, _id, target_op_id=None, target_task_name=None):
        """
        Cancel all active tasks of a concrete ns, nsi, vim_account, sdn identified for _id. If op_id is supplied only 
        this is cancelled, and the same with task_name
        """
        if not self.task_registry[topic].get(_id):
            return
        for op_id in reversed(self.task_registry[topic][_id]):
            if target_op_id and target_op_id != op_id:
                continue
            for task_name, task in self.task_registry[topic][_id][op_id].items():
                if target_task_name and target_task_name != task_name:
                    continue
                # result =
                task.cancel()
                # if result:
                #     self.logger.debug("{} _id={} order_id={} task={} cancelled".format(topic, _id, op_id, task_name))


class LcmBase:

    def __init__(self, db, msg, fs, logger):
        """

        :param db: database connection
        """
        self.db = db
        self.msg = msg
        self.fs = fs
        self.logger = logger

    def update_db_2(self, item, _id, _desc):
        """
        Updates database with _desc information. Upon success _desc is cleared
        :param item:
        :param _id:
        :param _desc:
        :return:
        """
        if not _desc:
            return
        try:
            self.db.set_one(item, {"_id": _id}, _desc)
            _desc.clear()
        except DbException as e:
            self.logger.error("Updating {} _id={} with '{}'. Error: {}".format(item, _id, _desc, e))
