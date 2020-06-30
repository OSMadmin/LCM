# -*- coding: utf-8 -*-

##
# Copyright 2020 Telefonica S.A.
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

import asyncio
from time import time
import logging
import aiohttp
import yaml
import os
from osm_lcm.lcm_utils import LcmException
from osm_common.dbbase import DbException


__author__ = "Alfonso Tierno <alfonso.tiernosepulveda@telefonica.com>"

initial_prometheus_data = {
    "_id": "prometheus",
    "_admin": {
        "locked_at": 0,
        "locked_by": None,
        "modified": 1593445184,  # 2020-06-29
        "created": 1593445184,
        "version": "1.0"    # to allow future version updates
    },
    'scrape_configs': [{'static_configs': [{'targets': ['mon:8000']}], 'job_name': 'mon_exporter'}],
    'global': {'evaluation_interval': '15s', 'scrape_interval': '15s'},
    'rule_files': None,
    'alerting': {'alertmanagers': [{'static_configs': [{'targets': None}]}]}
}


class Prometheus():
    """
    Implements a class to update Prometheus
    """

    PROMETHEUS_LOCKED_TIME = 120

    def __init__(self, config, worker_id, db, loop, logger=None):
        self.worker_id = worker_id
        self.db = db
        self.loop = loop
        self.logger = logger or logging.get_legger("lcm.prometheus")
        self.server = config["uri"]
        self.path = config["path"]
        if not self.path.endswith("/"):
            self.path += "/"
        self.cfg_file = self.path + "prometheus.yml"
        self.cfg_file_backup = self.path + "prometheus.yml-backup"

    async def start(self):
        for retry in range(4):
            try:
                # read from database
                prometheus_data = self.db.get_one("admin", {"_id": "prometheus"}, fail_on_empty=True)
                if not prometheus_data:
                    self.logger.info("Init db.admin.prometheus content")
                    self.db.create("admin", initial_prometheus_data)
                # send database config file to prometheus. Ignore loading errors, as prometheus may be starting
                # but at least an initial configuration file is set
                await self.update()
            except DbException as e:
                if retry == 3:
                    raise LcmException("Max retries trying to init prometheus configuration: {}".format(e))
                await asyncio.sleep(5, loop=self.loop)

    async def update(self, add_jobs=None, remove_jobs=None):
        for retry in range(4):
            result = True
            if retry:  # first time do not wait
                await asyncio.sleep(self.PROMETHEUS_LOCKED_TIME / 2, loop=self.loop)
            # lock database
            now = time()
            if not self.db.set_one(
                    "admin",
                    q_filter={"_id": "prometheus", "_admin.locked_at.lt": now - self.PROMETHEUS_LOCKED_TIME},
                    update_dict={"_admin.locked_at": now, "_admin.locked_by": self.worker_id},
                    fail_on_empty=False):
                continue
            # read database
            prometheus_data = self.db.get_one("admin", {"_id": "prometheus"})

            # Make changes from prometheus_incremental
            push_list = pull_list = None
            if add_jobs or remove_jobs:
                update_dict = {"_admin.locked_at": 0,
                               "_admin.locked_by": None,
                               "_admin.modified_at": now}
                if add_jobs:
                    push_list = {"scrape_configs.static_configs": add_jobs}
                    prometheus_data["scrape_configs"]["static_configs"] += add_jobs
                elif remove_jobs:
                    pass    # TODO
                if not self.send_data(prometheus_data):
                    push_list = pull_list = None
                    result = False

            # unblock database
            if not self.db.set_one(
                    "admin", {"_id": "prometheus", "_admin.locked_at": now, "_admin.locked_by": self.worker_id},
                    update_dict=update_dict, pull_list=pull_list, push_list=push_list, fail_on_empty=False):
                continue
            return result
        raise LcmException("Cannot update prometheus database. Reached max retries")

    async def send_data(self, new_config):
        restore_backup = False
        try:
            if os.path.exists(self.cfg_file):
                os.rename(self.cfg_file, self.cfg_file_backup)
                restore_backup = True
            with open(self.cfg_file, "w+") as f:
                yaml.dump(new_config, f)
            async with aiohttp.ClientSession() as session:
                async with session.post(self.server + "/-/reload") as resp:
                    if resp.status > 204:
                        raise LcmException(resp.text)
                await asyncio.sleep(5, loop=self.loop)
                async with session.get(self.server + "/api/v1/status/config") as resp:
                    if resp.status > 204:
                        raise LcmException(resp.text)
                    current_config = resp.json()
                    if not self._check_configuration_equal(current_config, new_config):
                        return False
                    else:
                        restore_backup = False
            return True
        except Exception as e:
            self.logger.error("Error updating prometheus configuration {}".format(e))
            return False
        finally:
            if restore_backup:
                os.rename(self.cfg_file_backup, self.cfg_file)

    @staticmethod
    def _check_configuration_equal(current_config, new_config):
        # TODO compare and return True if equal
        return True
