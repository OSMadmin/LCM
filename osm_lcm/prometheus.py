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
from jinja2 import Template, TemplateError, TemplateNotFound, TemplateSyntaxError

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
    'scrape_configs': {   # Dictionary at database. Converted to list before sending to prometheus
        'mon_exporter': {'static_configs': [{'targets': ['mon:8000']}], 'job_name': 'mon_exporter'},
    },
    'global': {'evaluation_interval': '15s', 'scrape_interval': '15s'},
    'rule_files': None,
    'alerting': {'alertmanagers': [{'static_configs': [{'targets': None}]}]}
}


class Prometheus:
    """
    Implements a class to update Prometheus
    """

    PROMETHEUS_LOCKED_TIME = 120

    def __init__(self, config, worker_id, db, loop, logger=None):
        self.worker_id = worker_id
        self.db = db
        self.loop = loop
        self.logger = logger or logging.getLogger("lcm.prometheus")
        self.server = config["uri"]
        self.path = config["path"]
        if not self.path.endswith("/"):
            self.path += "/"
        self.cfg_file = self.path + "prometheus.yml"
        self.cfg_file_backup = self.path + "prometheus.yml-backup"

    @staticmethod
    def parse_job(job_data: str, variables: dict) -> dict:
        try:
            template = Template(job_data)
            job_parsed = template.render(variables or {})
            return yaml.safe_load(job_parsed)
        except (TemplateError, TemplateNotFound, TemplateSyntaxError) as e:
            # TODO yaml exceptions
            raise LcmException("Error parsing Jinja2 to prometheus job. job_data={}, variables={}. Error={}".format(
                job_data, variables, e))

    async def start(self):
        for retry in range(4):
            try:
                # self.logger("Starting prometheus ")
                # read from database
                prometheus_data = self.db.get_one("admin", {"_id": "prometheus"}, fail_on_empty=False)
                if not prometheus_data:
                    self.logger.info("Init db.admin.prometheus content")
                    self.db.create("admin", initial_prometheus_data)
                # send database config file to prometheus. Ignore loading errors, as prometheus may be starting
                # but at least an initial configuration file is set
                await self.update()
                return
            except DbException as e:
                if retry == 3:
                    raise LcmException("Max retries trying to init prometheus configuration: {}".format(e))
                await asyncio.sleep(5, loop=self.loop)

    async def update(self, add_jobs: dict = None, remove_jobs: list = None) -> bool:
        """

        :param add_jobs: dictionary with {job_id_1: job_content, job_id_2: job_content}
        :param remove_jobs: list with jobs to remove [job_id_1, job_id_2]
        :return: result. If false prometheus denies this configuration. Exception on error
        """
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
            update_dict = {"_admin.locked_at": 0,
                           "_admin.locked_by": None}

            # Make changes from prometheus_incremental
            push_dict = pull_dict = None
            if add_jobs or remove_jobs:
                log_text_list = []
                if add_jobs:
                    log_text_list.append("adding jobs: {}".format(list(add_jobs.keys())))
                    prometheus_data["scrape_configs"].update(add_jobs)
                    push_dict = {"scrape_configs." + job_id: job_data for job_id, job_data in add_jobs.items()}
                elif remove_jobs:
                    log_text_list.append("removing jobs: {}".format(list(remove_jobs)))
                    for job_id in remove_jobs:
                        prometheus_data["scrape_configs"].pop(job_id, None)
                    pull_dict = {"scrape_configs." + job_id: None for job_id in remove_jobs}
                self.logger.debug("Updating. " + ". ".join(log_text_list))

            if not await self.send_data(prometheus_data):
                self.logger.error("Cannot update add_jobs: {}. remove_jobs: {}".format(add_jobs, remove_jobs))
                push_dict = pull_dict = None
                result = False

            # unblock database
            if push_dict:
                update_dict.update(push_dict)
            if push_dict or pull_dict:
                update_dict["_admin.modified_at"] = now
            if not self.db.set_one(
                    "admin", {"_id": "prometheus", "_admin.locked_at": now, "_admin.locked_by": self.worker_id},
                    update_dict=update_dict, unset=pull_dict, fail_on_empty=False):
                continue
            return result
        raise LcmException("Cannot update prometheus database. Reached max retries")

    async def send_data(self, new_config):
        restore_backup = False
        del new_config["_id"]
        del new_config["_admin"]
        new_scrape_configs = []

        # generate a list with the values of scrape_configs
        for scrape_config in new_config["scrape_configs"].values():
            scrape_config = scrape_config.copy()
            # remove nsr_id metadata from scrape_configs
            scrape_config.pop("nsr_id", None)
            new_scrape_configs.append(scrape_config)
        new_config["scrape_configs"] = new_scrape_configs

        try:
            if os.path.exists(self.cfg_file):
                os.rename(self.cfg_file, self.cfg_file_backup)
                restore_backup = True
            with open(self.cfg_file, "w+") as f:
                yaml.safe_dump(new_config, f, indent=4, default_flow_style=False)
            # self.logger.debug("new configuration: {}".format(yaml.safe_dump(new_config, indent=4,
            #                                                                 default_flow_style=False)))
            async with aiohttp.ClientSession() as session:
                async with session.post(self.server + "-/reload") as resp:
                    if resp.status > 204:
                        raise LcmException(await resp.text())
                await asyncio.sleep(5, loop=self.loop)
                # If prometheus does not admit this configuration, remains with the old one
                # Then, to check if the configuration has been accepted, get the configuration from prometheus
                # and compares with the inserted one
                async with session.get(self.server + "api/v1/status/config") as resp:
                    if resp.status > 204:
                        raise LcmException(await resp.text())
                    current_config = await resp.json()
                    if not self._check_configuration_equal(current_config, new_config):
                        return False
                    else:
                        restore_backup = False
            return True
        except Exception as e:
            self.logger.error("Error updating configuration url={}: {}".format(self.server, e))
            return False
        finally:
            if restore_backup:
                try:
                    os.rename(self.cfg_file_backup, self.cfg_file)
                except Exception as e:
                    self.logger.critical("Exception while rolling back: {}".format(e))

    def _check_configuration_equal(self, current_config, expected_config):
        try:
            # self.logger.debug("Comparing current_config='{}' with expected_config='{}'".format(current_config,
            #                                                                                    expected_config))
            current_config_yaml = yaml.safe_load(current_config['data']['yaml'])
            current_jobs = [j["job_name"] for j in current_config_yaml["scrape_configs"]]
            expected_jobs = [j["job_name"] for j in expected_config["scrape_configs"]]
            if current_jobs == expected_jobs:
                return True
            else:
                self.logger.error("Not all jobs have been loaded. Target jobs: {} Loaded jobs: {}".format(
                    expected_jobs, current_jobs))
                return False
        except Exception as e:
            self.logger.error("Invalid obtained status from server. Error: '{}'. Obtained data: '{}'".format(
                e, current_config))
            # if format is not understood, cannot be compared, assume it is ok
            return True
