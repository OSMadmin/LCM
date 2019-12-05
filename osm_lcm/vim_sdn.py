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

import asyncio
import yaml
import logging
import logging.handlers
from osm_lcm import ROclient
from osm_lcm.lcm_utils import LcmException, LcmBase, deep_get
from n2vc.k8s_helm_conn import K8sHelmConnector
from n2vc.k8s_juju_conn import K8sJujuConnector
from n2vc.exceptions import K8sException, N2VCException
from osm_common.dbbase import DbException
from copy import deepcopy

__author__ = "Alfonso Tierno"


class VimLcm(LcmBase):
    # values that are encrypted at vim config because they are passwords
    vim_config_encrypted = {"1.1": ("admin_password", "nsx_password", "vcenter_password"),
                            "default": ("admin_password", "nsx_password", "vcenter_password", "vrops_password")}

    def __init__(self, db, msg, fs, lcm_tasks, ro_config, loop):
        """
        Init, Connect to database, filesystem storage, and messaging
        :param config: two level dictionary with configuration. Top level should contain 'database', 'storage',
        :return: None
        """

        self.logger = logging.getLogger('lcm.vim')
        self.loop = loop
        self.lcm_tasks = lcm_tasks
        self.ro_config = ro_config

        super().__init__(db, msg, fs, self.logger)

    async def create(self, vim_content, order_id):

        # HA tasks and backward compatibility:
        # If 'vim_content' does not include 'op_id', we a running a legacy NBI version.
        # In such a case, HA is not supported by NBI, 'op_id' is None, and lock_HA() will do nothing.
        # Register 'create' task here for related future HA operations
        op_id = vim_content.pop('op_id', None)
        if not self.lcm_tasks.lock_HA('vim', 'create', op_id):
            return

        vim_id = vim_content["_id"]
        vim_content.pop("op_id", None)
        logging_text = "Task vim_create={} ".format(vim_id)
        self.logger.debug(logging_text + "Enter")

        db_vim = None
        db_vim_update = {}
        exc = None
        RO_sdn_id = None
        operationState_HA = ''
        detailed_status_HA = ''
        try:
            step = "Getting vim-id='{}' from db".format(vim_id)
            db_vim = self.db.get_one("vim_accounts", {"_id": vim_id})
            if vim_content.get("config") and vim_content["config"].get("sdn-controller"):
                step = "Getting sdn-controller-id='{}' from db".format(vim_content["config"]["sdn-controller"])
                db_sdn = self.db.get_one("sdns", {"_id": vim_content["config"]["sdn-controller"]})

                # If the VIM account has an associated SDN account, also
                # wait for any previous tasks in process for the SDN
                await self.lcm_tasks.waitfor_related_HA('sdn', 'ANY', db_sdn["_id"])

                if db_sdn.get("_admin") and db_sdn["_admin"].get("deployed") and db_sdn["_admin"]["deployed"].get("RO"):
                    RO_sdn_id = db_sdn["_admin"]["deployed"]["RO"]
                else:
                    raise LcmException("sdn-controller={} is not available. Not deployed at RO".format(
                        vim_content["config"]["sdn-controller"]))

            step = "Creating vim at RO"
            db_vim_update["_admin.deployed.RO"] = None
            db_vim_update["_admin.detailed-status"] = step
            self.update_db_2("vim_accounts", vim_id, db_vim_update)
            RO = ROclient.ROClient(self.loop, **self.ro_config)
            vim_RO = deepcopy(vim_content)
            vim_RO.pop("_id", None)
            vim_RO.pop("_admin", None)
            schema_version = vim_RO.pop("schema_version", None)
            vim_RO.pop("schema_type", None)
            vim_RO.pop("vim_tenant_name", None)
            vim_RO["type"] = vim_RO.pop("vim_type")
            vim_RO.pop("vim_user", None)
            vim_RO.pop("vim_password", None)
            if RO_sdn_id:
                vim_RO["config"]["sdn-controller"] = RO_sdn_id
            desc = await RO.create("vim", descriptor=vim_RO)
            RO_vim_id = desc["uuid"]
            db_vim_update["_admin.deployed.RO"] = RO_vim_id
            self.logger.debug(logging_text + "VIM created at RO_vim_id={}".format(RO_vim_id))

            step = "Creating vim_account at RO"
            db_vim_update["_admin.detailed-status"] = step
            self.update_db_2("vim_accounts", vim_id, db_vim_update)

            if vim_content.get("vim_password"):
                vim_content["vim_password"] = self.db.decrypt(vim_content["vim_password"],
                                                              schema_version=schema_version,
                                                              salt=vim_id)
            vim_account_RO = {"vim_tenant_name": vim_content["vim_tenant_name"],
                              "vim_username": vim_content["vim_user"],
                              "vim_password": vim_content["vim_password"]
                              }
            if vim_RO.get("config"):
                vim_account_RO["config"] = vim_RO["config"]
                if "sdn-controller" in vim_account_RO["config"]:
                    del vim_account_RO["config"]["sdn-controller"]
                if "sdn-port-mapping" in vim_account_RO["config"]:
                    del vim_account_RO["config"]["sdn-port-mapping"]
                vim_config_encrypted_keys = self.vim_config_encrypted.get(schema_version) or \
                    self.vim_config_encrypted.get("default")
                for p in vim_config_encrypted_keys:
                    if vim_account_RO["config"].get(p):
                        vim_account_RO["config"][p] = self.db.decrypt(vim_account_RO["config"][p],
                                                                      schema_version=schema_version,
                                                                      salt=vim_id)

            desc = await RO.attach("vim_account", RO_vim_id, descriptor=vim_account_RO)
            db_vim_update["_admin.deployed.RO-account"] = desc["uuid"]
            db_vim_update["_admin.operationalState"] = "ENABLED"
            db_vim_update["_admin.detailed-status"] = "Done"
            # Mark the VIM 'create' HA task as successful
            operationState_HA = 'COMPLETED'
            detailed_status_HA = 'Done'

            # await asyncio.sleep(15)   # TODO remove. This is for test
            self.logger.debug(logging_text + "Exit Ok VIM account created at RO_vim_account_id={}".format(desc["uuid"]))
            return

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            if exc and db_vim:
                db_vim_update["_admin.operationalState"] = "ERROR"
                db_vim_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
                # Mark the VIM 'create' HA task as erroneous
                operationState_HA = 'FAILED'
                detailed_status_HA = "ERROR {}: {}".format(step, exc)
            try:
                if db_vim_update:
                    self.update_db_2("vim_accounts", vim_id, db_vim_update)
                # Register the VIM 'create' HA task either
                # succesful or erroneous, or do nothing (if legacy NBI)
                self.lcm_tasks.register_HA('vim', 'create', op_id,
                                           operationState=operationState_HA,
                                           detailed_status=detailed_status_HA)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))

            self.lcm_tasks.remove("vim_account", vim_id, order_id)

    async def edit(self, vim_content, order_id):

        # HA tasks and backward compatibility:
        # If 'vim_content' does not include 'op_id', we a running a legacy NBI version.
        # In such a case, HA is not supported by NBI, and the HA check always returns True
        op_id = vim_content.pop('op_id', None)
        if not self.lcm_tasks.lock_HA('vim', 'edit', op_id):
            return

        vim_id = vim_content["_id"]
        vim_content.pop("op_id", None)
        logging_text = "Task vim_edit={} ".format(vim_id)
        self.logger.debug(logging_text + "Enter")

        db_vim = None
        exc = None
        RO_sdn_id = None
        RO_vim_id = None
        db_vim_update = {}
        operationState_HA = ''
        detailed_status_HA = ''
        step = "Getting vim-id='{}' from db".format(vim_id)
        try:
            # wait for any previous tasks in process
            await self.lcm_tasks.waitfor_related_HA('vim', 'edit', op_id)

            db_vim = self.db.get_one("vim_accounts", {"_id": vim_id})

            if db_vim.get("_admin") and db_vim["_admin"].get("deployed") and db_vim["_admin"]["deployed"].get("RO"):
                if vim_content.get("config") and vim_content["config"].get("sdn-controller"):
                    step = "Getting sdn-controller-id='{}' from db".format(vim_content["config"]["sdn-controller"])
                    db_sdn = self.db.get_one("sdns", {"_id": vim_content["config"]["sdn-controller"]})

                    # If the VIM account has an associated SDN account, also
                    # wait for any previous tasks in process for the SDN
                    await self.lcm_tasks.waitfor_related_HA('sdn', 'ANY', db_sdn["_id"])

                    if db_sdn.get("_admin") and db_sdn["_admin"].get("deployed") and db_sdn["_admin"]["deployed"].get(
                            "RO"):
                        RO_sdn_id = db_sdn["_admin"]["deployed"]["RO"]
                    else:
                        raise LcmException("sdn-controller={} is not available. Not deployed at RO".format(
                            vim_content["config"]["sdn-controller"]))

                RO_vim_id = db_vim["_admin"]["deployed"]["RO"]
                step = "Editing vim at RO"
                RO = ROclient.ROClient(self.loop, **self.ro_config)
                vim_RO = deepcopy(vim_content)
                vim_RO.pop("_id", None)
                vim_RO.pop("_admin", None)
                schema_version = vim_RO.pop("schema_version", None)
                vim_RO.pop("schema_type", None)
                vim_RO.pop("vim_tenant_name", None)
                if "vim_type" in vim_RO:
                    vim_RO["type"] = vim_RO.pop("vim_type")
                vim_RO.pop("vim_user", None)
                vim_RO.pop("vim_password", None)
                if RO_sdn_id:
                    vim_RO["config"]["sdn-controller"] = RO_sdn_id
                # TODO make a deep update of sdn-port-mapping 
                if vim_RO:
                    await RO.edit("vim", RO_vim_id, descriptor=vim_RO)

                step = "Editing vim-account at RO tenant"
                vim_account_RO = {}
                if "config" in vim_content:
                    if "sdn-controller" in vim_content["config"]:
                        del vim_content["config"]["sdn-controller"]
                    if "sdn-port-mapping" in vim_content["config"]:
                        del vim_content["config"]["sdn-port-mapping"]
                    if not vim_content["config"]:
                        del vim_content["config"]
                if "vim_tenant_name" in vim_content:
                    vim_account_RO["vim_tenant_name"] = vim_content["vim_tenant_name"]
                if "vim_password" in vim_content:
                    vim_account_RO["vim_password"] = vim_content["vim_password"]
                if vim_content.get("vim_password"):
                    vim_account_RO["vim_password"] = self.db.decrypt(vim_content["vim_password"],
                                                                     schema_version=schema_version,
                                                                     salt=vim_id)
                if "config" in vim_content:
                    vim_account_RO["config"] = vim_content["config"]
                if vim_content.get("config"):
                    vim_config_encrypted_keys = self.vim_config_encrypted.get(schema_version) or \
                        self.vim_config_encrypted.get("default")
                    for p in vim_config_encrypted_keys:
                        if vim_content["config"].get(p):
                            vim_account_RO["config"][p] = self.db.decrypt(vim_content["config"][p],
                                                                          schema_version=schema_version,
                                                                          salt=vim_id)

                if "vim_user" in vim_content:
                    vim_content["vim_username"] = vim_content["vim_user"]
                # vim_account must be edited always even if empty in order to ensure changes are translated to RO
                # vim_thread. RO will remove and relaunch a new thread for this vim_account
                await RO.edit("vim_account", RO_vim_id, descriptor=vim_account_RO)
                db_vim_update["_admin.operationalState"] = "ENABLED"
                # Mark the VIM 'edit' HA task as successful
                operationState_HA = 'COMPLETED'
                detailed_status_HA = 'Done'

            self.logger.debug(logging_text + "Exit Ok RO_vim_id={}".format(RO_vim_id))
            return

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            if exc and db_vim:
                db_vim_update["_admin.operationalState"] = "ERROR"
                db_vim_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
                # Mark the VIM 'edit' HA task as erroneous
                operationState_HA = 'FAILED'
                detailed_status_HA = "ERROR {}: {}".format(step, exc)
            try:
                if db_vim_update:
                    self.update_db_2("vim_accounts", vim_id, db_vim_update)
                # Register the VIM 'edit' HA task either
                # succesful or erroneous, or do nothing (if legacy NBI)
                self.lcm_tasks.register_HA('vim', 'edit', op_id,
                                           operationState=operationState_HA,
                                           detailed_status=detailed_status_HA)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))

            self.lcm_tasks.remove("vim_account", vim_id, order_id)

    async def delete(self, vim_content, order_id):

        # HA tasks and backward compatibility:
        # If 'vim_content' does not include 'op_id', we a running a legacy NBI version.
        # In such a case, HA is not supported by NBI, and the HA check always returns True
        op_id = vim_content.pop('op_id', None)
        if not self.lcm_tasks.lock_HA('vim', 'delete', op_id):
            return

        vim_id = vim_content["_id"]
        logging_text = "Task vim_delete={} ".format(vim_id)
        self.logger.debug(logging_text + "Enter")

        db_vim = None
        db_vim_update = {}
        exc = None
        operationState_HA = ''
        detailed_status_HA = ''
        step = "Getting vim from db"
        try:
            # wait for any previous tasks in process
            await self.lcm_tasks.waitfor_related_HA('vim', 'delete', op_id)

            db_vim = self.db.get_one("vim_accounts", {"_id": vim_id})
            if db_vim.get("_admin") and db_vim["_admin"].get("deployed") and db_vim["_admin"]["deployed"].get("RO"):
                RO_vim_id = db_vim["_admin"]["deployed"]["RO"]
                RO = ROclient.ROClient(self.loop, **self.ro_config)
                step = "Detaching vim from RO tenant"
                try:
                    await RO.detach("vim_account", RO_vim_id)
                except ROclient.ROClientException as e:
                    if e.http_code == 404:  # not found
                        self.logger.debug(logging_text + "RO_vim_id={} already detached".format(RO_vim_id))
                    else:
                        raise

                step = "Deleting vim from RO"
                try:
                    await RO.delete("vim", RO_vim_id)
                except ROclient.ROClientException as e:
                    if e.http_code == 404:  # not found
                        self.logger.debug(logging_text + "RO_vim_id={} already deleted".format(RO_vim_id))
                    else:
                        raise
            else:
                # nothing to delete
                self.logger.error(logging_text + "Nothing to remove at RO")
            self.db.del_one("vim_accounts", {"_id": vim_id})
            db_vim = None
            self.logger.debug(logging_text + "Exit Ok")
            return

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            self.lcm_tasks.remove("vim_account", vim_id, order_id)
            if exc and db_vim:
                db_vim_update["_admin.operationalState"] = "ERROR"
                db_vim_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
                # Mark the VIM 'delete' HA task as erroneous
                operationState_HA = 'FAILED'
                detailed_status_HA = "ERROR {}: {}".format(step, exc)
                self.lcm_tasks.register_HA('vim', 'delete', op_id,
                                           operationState=operationState_HA,
                                           detailed_status=detailed_status_HA)
            try:
                if db_vim and db_vim_update:
                    self.update_db_2("vim_accounts", vim_id, db_vim_update)
                # If the VIM 'delete' HA task was succesful, the DB entry has been deleted,
                # which means that there is nowhere to register this task, so do nothing here.
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.lcm_tasks.remove("vim_account", vim_id, order_id)


class WimLcm(LcmBase):
    # values that are encrypted at wim config because they are passwords
    wim_config_encrypted = ()

    def __init__(self, db, msg, fs, lcm_tasks, ro_config, loop):
        """
        Init, Connect to database, filesystem storage, and messaging
        :param config: two level dictionary with configuration. Top level should contain 'database', 'storage',
        :return: None
        """

        self.logger = logging.getLogger('lcm.vim')
        self.loop = loop
        self.lcm_tasks = lcm_tasks
        self.ro_config = ro_config

        super().__init__(db, msg, fs, self.logger)

    async def create(self, wim_content, order_id):

        # HA tasks and backward compatibility:
        # If 'wim_content' does not include 'op_id', we a running a legacy NBI version.
        # In such a case, HA is not supported by NBI, 'op_id' is None, and lock_HA() will do nothing.
        # Register 'create' task here for related future HA operations
        op_id = wim_content.pop('op_id', None)
        self.lcm_tasks.lock_HA('wim', 'create', op_id)

        wim_id = wim_content["_id"]
        wim_content.pop("op_id", None)
        logging_text = "Task wim_create={} ".format(wim_id)
        self.logger.debug(logging_text + "Enter")

        db_wim = None
        db_wim_update = {}
        exc = None
        operationState_HA = ''
        detailed_status_HA = ''
        try:
            step = "Getting wim-id='{}' from db".format(wim_id)
            db_wim = self.db.get_one("wim_accounts", {"_id": wim_id})
            db_wim_update["_admin.deployed.RO"] = None

            step = "Creating wim at RO"
            db_wim_update["_admin.detailed-status"] = step
            self.update_db_2("wim_accounts", wim_id, db_wim_update)
            RO = ROclient.ROClient(self.loop, **self.ro_config)
            wim_RO = deepcopy(wim_content)
            wim_RO.pop("_id", None)
            wim_RO.pop("_admin", None)
            schema_version = wim_RO.pop("schema_version", None)
            wim_RO.pop("schema_type", None)
            wim_RO.pop("wim_tenant_name", None)
            wim_RO["type"] = wim_RO.pop("wim_type")
            wim_RO.pop("wim_user", None)
            wim_RO.pop("wim_password", None)
            desc = await RO.create("wim", descriptor=wim_RO)
            RO_wim_id = desc["uuid"]
            db_wim_update["_admin.deployed.RO"] = RO_wim_id
            self.logger.debug(logging_text + "WIM created at RO_wim_id={}".format(RO_wim_id))

            step = "Creating wim_account at RO"
            db_wim_update["_admin.detailed-status"] = step
            self.update_db_2("wim_accounts", wim_id, db_wim_update)

            if wim_content.get("wim_password"):
                wim_content["wim_password"] = self.db.decrypt(wim_content["wim_password"],
                                                              schema_version=schema_version,
                                                              salt=wim_id)
            wim_account_RO = {"name": wim_content["name"],
                              "user": wim_content["user"],
                              "password": wim_content["password"]
                              }
            if wim_RO.get("config"):
                wim_account_RO["config"] = wim_RO["config"]
                if "wim_port_mapping" in wim_account_RO["config"]:
                    del wim_account_RO["config"]["wim_port_mapping"]
                for p in self.wim_config_encrypted:
                    if wim_account_RO["config"].get(p):
                        wim_account_RO["config"][p] = self.db.decrypt(wim_account_RO["config"][p],
                                                                      schema_version=schema_version,
                                                                      salt=wim_id)

            desc = await RO.attach("wim_account", RO_wim_id, descriptor=wim_account_RO)
            db_wim_update["_admin.deployed.RO-account"] = desc["uuid"]
            db_wim_update["_admin.operationalState"] = "ENABLED"
            db_wim_update["_admin.detailed-status"] = "Done"
            # Mark the WIM 'create' HA task as successful
            operationState_HA = 'COMPLETED'
            detailed_status_HA = 'Done'

            self.logger.debug(logging_text + "Exit Ok WIM account created at RO_wim_account_id={}".format(desc["uuid"]))
            return

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            if exc and db_wim:
                db_wim_update["_admin.operationalState"] = "ERROR"
                db_wim_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
                # Mark the WIM 'create' HA task as erroneous
                operationState_HA = 'FAILED'
                detailed_status_HA = "ERROR {}: {}".format(step, exc)
            try:
                if db_wim_update:
                    self.update_db_2("wim_accounts", wim_id, db_wim_update)
                # Register the WIM 'create' HA task either
                # succesful or erroneous, or do nothing (if legacy NBI)
                self.lcm_tasks.register_HA('wim', 'create', op_id,
                                           operationState=operationState_HA,
                                           detailed_status=detailed_status_HA)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.lcm_tasks.remove("wim_account", wim_id, order_id)

    async def edit(self, wim_content, order_id):

        # HA tasks and backward compatibility:
        # If 'wim_content' does not include 'op_id', we a running a legacy NBI version.
        # In such a case, HA is not supported by NBI, and the HA check always returns True
        op_id = wim_content.pop('op_id', None)
        if not self.lcm_tasks.lock_HA('wim', 'edit', op_id):
            return

        wim_id = wim_content["_id"]
        wim_content.pop("op_id", None)
        logging_text = "Task wim_edit={} ".format(wim_id)
        self.logger.debug(logging_text + "Enter")

        db_wim = None
        exc = None
        RO_wim_id = None
        db_wim_update = {}
        step = "Getting wim-id='{}' from db".format(wim_id)
        operationState_HA = ''
        detailed_status_HA = ''
        try:
            # wait for any previous tasks in process
            await self.lcm_tasks.waitfor_related_HA('wim', 'edit', op_id)

            db_wim = self.db.get_one("wim_accounts", {"_id": wim_id})

            if db_wim.get("_admin") and db_wim["_admin"].get("deployed") and db_wim["_admin"]["deployed"].get("RO"):

                RO_wim_id = db_wim["_admin"]["deployed"]["RO"]
                step = "Editing wim at RO"
                RO = ROclient.ROClient(self.loop, **self.ro_config)
                wim_RO = deepcopy(wim_content)
                wim_RO.pop("_id", None)
                wim_RO.pop("_admin", None)
                schema_version = wim_RO.pop("schema_version", None)
                wim_RO.pop("schema_type", None)
                wim_RO.pop("wim_tenant_name", None)
                if "wim_type" in wim_RO:
                    wim_RO["type"] = wim_RO.pop("wim_type")
                wim_RO.pop("wim_user", None)
                wim_RO.pop("wim_password", None)
                # TODO make a deep update of wim_port_mapping
                if wim_RO:
                    await RO.edit("wim", RO_wim_id, descriptor=wim_RO)

                step = "Editing wim-account at RO tenant"
                wim_account_RO = {}
                if "config" in wim_content:
                    if "wim_port_mapping" in wim_content["config"]:
                        del wim_content["config"]["wim_port_mapping"]
                    if not wim_content["config"]:
                        del wim_content["config"]
                if "wim_tenant_name" in wim_content:
                    wim_account_RO["wim_tenant_name"] = wim_content["wim_tenant_name"]
                if "wim_password" in wim_content:
                    wim_account_RO["wim_password"] = wim_content["wim_password"]
                if wim_content.get("wim_password"):
                    wim_account_RO["wim_password"] = self.db.decrypt(wim_content["wim_password"],
                                                                     schema_version=schema_version,
                                                                     salt=wim_id)
                if "config" in wim_content:
                    wim_account_RO["config"] = wim_content["config"]
                if wim_content.get("config"):
                    for p in self.wim_config_encrypted:
                        if wim_content["config"].get(p):
                            wim_account_RO["config"][p] = self.db.decrypt(wim_content["config"][p],
                                                                          schema_version=schema_version,
                                                                          salt=wim_id)

                if "wim_user" in wim_content:
                    wim_content["wim_username"] = wim_content["wim_user"]
                # wim_account must be edited always even if empty in order to ensure changes are translated to RO
                # wim_thread. RO will remove and relaunch a new thread for this wim_account
                await RO.edit("wim_account", RO_wim_id, descriptor=wim_account_RO)
                db_wim_update["_admin.operationalState"] = "ENABLED"
                # Mark the WIM 'edit' HA task as successful
                operationState_HA = 'COMPLETED'
                detailed_status_HA = 'Done'

            self.logger.debug(logging_text + "Exit Ok RO_wim_id={}".format(RO_wim_id))
            return

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            if exc and db_wim:
                db_wim_update["_admin.operationalState"] = "ERROR"
                db_wim_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
                # Mark the WIM 'edit' HA task as erroneous
                operationState_HA = 'FAILED'
                detailed_status_HA = "ERROR {}: {}".format(step, exc)
            try:
                if db_wim_update:
                    self.update_db_2("wim_accounts", wim_id, db_wim_update)
                # Register the WIM 'edit' HA task either
                # succesful or erroneous, or do nothing (if legacy NBI)
                self.lcm_tasks.register_HA('wim', 'edit', op_id,
                                           operationState=operationState_HA,
                                           detailed_status=detailed_status_HA)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.lcm_tasks.remove("wim_account", wim_id, order_id)

    async def delete(self, wim_content, order_id):

        # HA tasks and backward compatibility:
        # If 'vim_content' does not include 'op_id', we a running a legacy NBI version.
        # In such a case, HA is not supported by NBI, and the HA check always returns True
        op_id = wim_content.pop('op_id', None)
        if not self.lcm_tasks.lock_HA('wim', 'delete', op_id):
            return

        wim_id = wim_content["_id"]
        logging_text = "Task wim_delete={} ".format(wim_id)
        self.logger.debug(logging_text + "Enter")

        db_wim = None
        db_wim_update = {}
        exc = None
        step = "Getting wim from db"
        operationState_HA = ''
        detailed_status_HA = ''
        try:
            # wait for any previous tasks in process
            await self.lcm_tasks.waitfor_related_HA('wim', 'delete', op_id)

            db_wim = self.db.get_one("wim_accounts", {"_id": wim_id})
            if db_wim.get("_admin") and db_wim["_admin"].get("deployed") and db_wim["_admin"]["deployed"].get("RO"):
                RO_wim_id = db_wim["_admin"]["deployed"]["RO"]
                RO = ROclient.ROClient(self.loop, **self.ro_config)
                step = "Detaching wim from RO tenant"
                try:
                    await RO.detach("wim_account", RO_wim_id)
                except ROclient.ROClientException as e:
                    if e.http_code == 404:  # not found
                        self.logger.debug(logging_text + "RO_wim_id={} already detached".format(RO_wim_id))
                    else:
                        raise

                step = "Deleting wim from RO"
                try:
                    await RO.delete("wim", RO_wim_id)
                except ROclient.ROClientException as e:
                    if e.http_code == 404:  # not found
                        self.logger.debug(logging_text + "RO_wim_id={} already deleted".format(RO_wim_id))
                    else:
                        raise
            else:
                # nothing to delete
                self.logger.error(logging_text + "Nohing to remove at RO")
            self.db.del_one("wim_accounts", {"_id": wim_id})
            db_wim = None
            self.logger.debug(logging_text + "Exit Ok")
            return

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            self.lcm_tasks.remove("wim_account", wim_id, order_id)
            if exc and db_wim:
                db_wim_update["_admin.operationalState"] = "ERROR"
                db_wim_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
                # Mark the WIM 'delete' HA task as erroneous
                operationState_HA = 'FAILED'
                detailed_status_HA = "ERROR {}: {}".format(step, exc)
                self.lcm_tasks.register_HA('wim', 'delete', op_id,
                                           operationState=operationState_HA,
                                           detailed_status=detailed_status_HA)
            try:
                if db_wim and db_wim_update:
                    self.update_db_2("wim_accounts", wim_id, db_wim_update)
                # If the WIM 'delete' HA task was succesful, the DB entry has been deleted,
                # which means that there is nowhere to register this task, so do nothing here.
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.lcm_tasks.remove("wim_account", wim_id, order_id)


class SdnLcm(LcmBase):

    def __init__(self, db, msg, fs, lcm_tasks, ro_config, loop):
        """
        Init, Connect to database, filesystem storage, and messaging
        :param config: two level dictionary with configuration. Top level should contain 'database', 'storage',
        :return: None
        """

        self.logger = logging.getLogger('lcm.sdn')
        self.loop = loop
        self.lcm_tasks = lcm_tasks
        self.ro_config = ro_config

        super().__init__(db, msg, fs, self.logger)

    async def create(self, sdn_content, order_id):

        # HA tasks and backward compatibility:
        # If 'sdn_content' does not include 'op_id', we a running a legacy NBI version.
        # In such a case, HA is not supported by NBI, 'op_id' is None, and lock_HA() will do nothing.
        # Register 'create' task here for related future HA operations
        op_id = sdn_content.pop('op_id', None)
        self.lcm_tasks.lock_HA('sdn', 'create', op_id)

        sdn_id = sdn_content["_id"]
        sdn_content.pop("op_id", None)
        logging_text = "Task sdn_create={} ".format(sdn_id)
        self.logger.debug(logging_text + "Enter")

        db_sdn = None
        db_sdn_update = {}
        RO_sdn_id = None
        exc = None
        operationState_HA = ''
        detailed_status_HA = ''
        try:
            step = "Getting sdn from db"
            db_sdn = self.db.get_one("sdns", {"_id": sdn_id})
            db_sdn_update["_admin.deployed.RO"] = None

            step = "Creating sdn at RO"
            db_sdn_update["_admin.detailed-status"] = step
            self.update_db_2("sdns", sdn_id, db_sdn_update)

            RO = ROclient.ROClient(self.loop, **self.ro_config)
            sdn_RO = deepcopy(sdn_content)
            sdn_RO.pop("_id", None)
            sdn_RO.pop("_admin", None)
            schema_version = sdn_RO.pop("schema_version", None)
            sdn_RO.pop("schema_type", None)
            sdn_RO.pop("description", None)
            if sdn_RO.get("password"):
                sdn_RO["password"] = self.db.decrypt(sdn_RO["password"], schema_version=schema_version, salt=sdn_id)

            desc = await RO.create("sdn", descriptor=sdn_RO)
            RO_sdn_id = desc["uuid"]
            db_sdn_update["_admin.deployed.RO"] = RO_sdn_id
            db_sdn_update["_admin.operationalState"] = "ENABLED"
            self.logger.debug(logging_text + "Exit Ok RO_sdn_id={}".format(RO_sdn_id))
            # Mark the SDN 'create' HA task as successful
            operationState_HA = 'COMPLETED'
            detailed_status_HA = 'Done'
            return

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            if exc and db_sdn:
                db_sdn_update["_admin.operationalState"] = "ERROR"
                db_sdn_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
                # Mark the SDN 'create' HA task as erroneous
                operationState_HA = 'FAILED'
                detailed_status_HA = "ERROR {}: {}".format(step, exc)
            try:
                if db_sdn and db_sdn_update:
                    self.update_db_2("sdns", sdn_id, db_sdn_update)
                # Register the SDN 'create' HA task either
                # succesful or erroneous, or do nothing (if legacy NBI)
                self.lcm_tasks.register_HA('sdn', 'create', op_id,
                                           operationState=operationState_HA,
                                           detailed_status=detailed_status_HA)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.lcm_tasks.remove("sdn", sdn_id, order_id)

    async def edit(self, sdn_content, order_id):

        # HA tasks and backward compatibility:
        # If 'sdn_content' does not include 'op_id', we a running a legacy NBI version.
        # In such a case, HA is not supported by NBI, and the HA check always returns True
        op_id = sdn_content.pop('op_id', None)
        if not self.lcm_tasks.lock_HA('sdn', 'edit', op_id):
            return

        sdn_id = sdn_content["_id"]
        sdn_content.pop("op_id", None)
        logging_text = "Task sdn_edit={} ".format(sdn_id)
        self.logger.debug(logging_text + "Enter")

        db_sdn = None
        db_sdn_update = {}
        exc = None
        operationState_HA = ''
        detailed_status_HA = ''
        step = "Getting sdn from db"
        try:
            # wait for any previous tasks in process
            await self.lcm_tasks.waitfor_related_HA('sdn', 'edit', op_id)

            db_sdn = self.db.get_one("sdns", {"_id": sdn_id})
            RO_sdn_id = None
            if db_sdn.get("_admin") and db_sdn["_admin"].get("deployed") and db_sdn["_admin"]["deployed"].get("RO"):
                RO_sdn_id = db_sdn["_admin"]["deployed"]["RO"]
                RO = ROclient.ROClient(self.loop, **self.ro_config)
                step = "Editing sdn at RO"
                sdn_RO = deepcopy(sdn_content)
                sdn_RO.pop("_id", None)
                sdn_RO.pop("_admin", None)
                schema_version = sdn_RO.pop("schema_version", None)
                sdn_RO.pop("schema_type", None)
                sdn_RO.pop("description", None)
                if sdn_RO.get("password"):
                    sdn_RO["password"] = self.db.decrypt(sdn_RO["password"], schema_version=schema_version, salt=sdn_id)
                if sdn_RO:
                    await RO.edit("sdn", RO_sdn_id, descriptor=sdn_RO)
                db_sdn_update["_admin.operationalState"] = "ENABLED"
                # Mark the SDN 'edit' HA task as successful
                operationState_HA = 'COMPLETED'
                detailed_status_HA = 'Done'

            self.logger.debug(logging_text + "Exit Ok RO_sdn_id={}".format(RO_sdn_id))
            return

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            if exc and db_sdn:
                db_sdn["_admin.operationalState"] = "ERROR"
                db_sdn["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
                # Mark the SDN 'edit' HA task as erroneous
                operationState_HA = 'FAILED'
                detailed_status_HA = "ERROR {}: {}".format(step, exc)
            try:
                if db_sdn_update:
                    self.update_db_2("sdns", sdn_id, db_sdn_update)
                # Register the SDN 'edit' HA task either
                # succesful or erroneous, or do nothing (if legacy NBI)
                self.lcm_tasks.register_HA('sdn', 'edit', op_id,
                                           operationState=operationState_HA,
                                           detailed_status=detailed_status_HA)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.lcm_tasks.remove("sdn", sdn_id, order_id)

    async def delete(self, sdn_content, order_id):

        # HA tasks and backward compatibility:
        # If 'vim_content' does not include 'op_id', we a running a legacy NBI version.
        # In such a case, HA is not supported by NBI, and the HA check always returns True
        op_id = sdn_content.pop('op_id', None)
        if not self.lcm_tasks.lock_HA('sdn', 'delete', op_id):
            return

        sdn_id = sdn_content["_id"]
        logging_text = "Task sdn_delete={} ".format(sdn_id)
        self.logger.debug(logging_text + "Enter")

        db_sdn = None
        db_sdn_update = {}
        exc = None
        operationState_HA = ''
        detailed_status_HA = ''
        step = "Getting sdn from db"
        try:
            # wait for any previous tasks in process
            await self.lcm_tasks.waitfor_related_HA('sdn', 'delete', op_id)

            db_sdn = self.db.get_one("sdns", {"_id": sdn_id})
            if db_sdn.get("_admin") and db_sdn["_admin"].get("deployed") and db_sdn["_admin"]["deployed"].get("RO"):
                RO_sdn_id = db_sdn["_admin"]["deployed"]["RO"]
                RO = ROclient.ROClient(self.loop, **self.ro_config)
                step = "Deleting sdn from RO"
                try:
                    await RO.delete("sdn", RO_sdn_id)
                except ROclient.ROClientException as e:
                    if e.http_code == 404:  # not found
                        self.logger.debug(logging_text + "RO_sdn_id={} already deleted".format(RO_sdn_id))
                    else:
                        raise
            else:
                # nothing to delete
                self.logger.error(logging_text + "Skipping. There is not RO information at database")
            self.db.del_one("sdns", {"_id": sdn_id})
            db_sdn = None
            self.logger.debug("sdn_delete task sdn_id={} Exit Ok".format(sdn_id))
            return

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            if exc and db_sdn:
                db_sdn["_admin.operationalState"] = "ERROR"
                db_sdn["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
                # Mark the SDN 'delete' HA task as erroneous
                operationState_HA = 'FAILED'
                detailed_status_HA = "ERROR {}: {}".format(step, exc)
                self.lcm_tasks.register_HA('sdn', 'delete', op_id,
                                           operationState=operationState_HA,
                                           detailed_status=detailed_status_HA)
            try:
                if db_sdn and db_sdn_update:
                    self.update_db_2("sdns", sdn_id, db_sdn_update)
                # If the SDN 'delete' HA task was succesful, the DB entry has been deleted,
                # which means that there is nowhere to register this task, so do nothing here.
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.lcm_tasks.remove("sdn", sdn_id, order_id)


class K8sClusterLcm(LcmBase):

    def __init__(self, db, msg, fs, lcm_tasks, vca_config, loop):
        """
        Init, Connect to database, filesystem storage, and messaging
        :param config: two level dictionary with configuration. Top level should contain 'database', 'storage',
        :return: None
        """

        self.logger = logging.getLogger('lcm.k8scluster')
        self.loop = loop
        self.lcm_tasks = lcm_tasks
        self.vca_config = vca_config
        self.fs = fs
        self.db = db

        self.helm_k8scluster = K8sHelmConnector(
            kubectl_command=self.vca_config.get("kubectlpath"),
            helm_command=self.vca_config.get("helmpath"),
            fs=self.fs,
            log=self.logger,
            db=self.db,
            on_update_db=None
        )

        self.juju_k8scluster = K8sJujuConnector(
            kubectl_command=self.vca_config.get("kubectlpath"),
            juju_command=self.vca_config.get("jujupath"),
            fs=self.fs,
            log=self.logger,
            db=self.db,
            on_update_db=None
        )

        super().__init__(db, msg, fs, self.logger)

    async def create(self, k8scluster_content, order_id):

        # HA tasks and backward compatibility:
        # If 'vim_content' does not include 'op_id', we a running a legacy NBI version.
        # In such a case, HA is not supported by NBI, 'op_id' is None, and lock_HA() will do nothing.
        # Register 'create' task here for related future HA operations
        op_id = k8scluster_content.pop('op_id', None)
        if not self.lcm_tasks.lock_HA('k8scluster', 'create', op_id):
            return

        k8scluster_id = k8scluster_content["_id"]
        k8scluster_content.pop("op_id", None)
        logging_text = "Task k8scluster_create={} ".format(k8scluster_id)
        self.logger.debug(logging_text + "Enter")

        db_k8scluster = None
        db_k8scluster_update = {}

        exc = None
        operationState_HA = ''
        detailed_status_HA = ''
        try:
            step = "Getting k8scluster-id='{}' from db".format(k8scluster_id)
            self.logger.debug(logging_text + step)
            db_k8scluster = self.db.get_one("k8sclusters", {"_id": k8scluster_id})
            self.db.encrypt_decrypt_fields(db_k8scluster.get("credentials"), 'decrypt', ['password', 'secret'],
                                           schema_version=db_k8scluster["schema_version"], salt=db_k8scluster["_id"])
            k8s_credentials = yaml.safe_dump(db_k8scluster.get("credentials"))
            error_text_list = []
            # helm-chart
            k8s_hc_id = None
            try:
                k8s_hc_id, uninstall_sw = await self.helm_k8scluster.init_env(k8s_credentials)
                db_k8scluster_update["_admin.helm-chart.id"] = k8s_hc_id
                db_k8scluster_update["_admin.helm-chart.created"] = uninstall_sw
            except Exception as e:
                error_text_list.append("Failing init helm-chart: {}".format(e))
                db_k8scluster_update["_admin.helm-chart.error_msg"] = str(e)
                if isinstance(e, K8sException):
                    self.logger.error(logging_text + "Failing init helm-chart: {}".format(e))
                else:
                    self.logger.error(logging_text + "Failing init helm-chart: {}".format(e), exc_info=True)

            # Juju/k8s cluster
            k8s_jb_id = None
            try:
                k8s_jb_id, uninstall_sw = await self.juju_k8scluster.init_env(k8s_credentials)
                db_k8scluster_update["_admin.juju-bundle.id"] = k8s_jb_id
                db_k8scluster_update["_admin.juju-bundle.created"] = uninstall_sw
            except Exception as e:
                error_text_list.append("Failing init juju-bundle: {}".format(e))
                db_k8scluster_update["_admin.juju-bundle.error_msg"] = str(e)
                if isinstance(e, N2VCException):
                    self.logger.error(logging_text + "Failing init juju-bundle: {}".format(e))
                else:
                    self.logger.error(logging_text + "Failing init juju-bundle: {}".format(e), exc_info=True)

            step = "Getting the list of repos"
            if k8s_hc_id:
                self.logger.debug(logging_text + step)
                task_list = []
                db_k8srepo_list = self.db.get_list("k8srepos", {"type": "helm-chart"})
                for repo in db_k8srepo_list:
                    step = "Adding repo {} to cluster: {}".format(repo["name"], k8s_hc_id)
                    self.logger.debug(logging_text + step)
                    task = asyncio.ensure_future(self.helm_k8scluster.repo_add(cluster_uuid=k8s_hc_id,
                                                 name=repo["name"], url=repo["url"],
                                                 repo_type="chart"))
                    task_list.append(task)
                    repo_k8scluster_list = deep_get(repo, ("_admin", "cluster-inserted")) or []
                    repo_k8scluster_list.append(k8s_hc_id)
                    self.update_db_2("k8srepos", repo["_id"], {"_admin.cluster-inserted": repo_k8scluster_list})

                if task_list:
                    self.logger.debug(logging_text + 'Waiting for terminate tasks of repo_add')
                    done, pending = await asyncio.wait(task_list, timeout=3600)
                    if pending:
                        self.logger.error(logging_text + 'There are pending tasks: {}'.format(pending))

            # mark as an error if both helm-chart and juju-bundle have been failed
            if k8s_hc_id or k8s_jb_id:
                db_k8scluster_update["_admin.operationalState"] = "ENABLED"
            else:
                db_k8scluster_update["_admin.operationalState"] = "ERROR"
                db_k8scluster_update["_admin.detailed-status"] = ";".join(error_text_list)

        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            if exc and db_k8scluster:
                db_k8scluster_update["_admin.operationalState"] = "ERROR"
                db_k8scluster_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)

                # Mark the k8scluster 'create' HA task as erroneous
                operationState_HA = 'FAILED'
                detailed_status_HA = "ERROR {}: {}".format(step, exc)
            try:
                if db_k8scluster_update:
                    self.update_db_2("k8sclusters", k8scluster_id, db_k8scluster_update)

                # Register the K8scluster 'create' HA task either
                # succesful or erroneous, or do nothing (if legacy NBI)
                self.lcm_tasks.register_HA('k8scluster', 'create', op_id,
                                           operationState=operationState_HA,
                                           detailed_status=detailed_status_HA)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.lcm_tasks.remove("k8sclusters", k8scluster_id, order_id)

    async def delete(self, k8scluster_content, order_id):

        # HA tasks and backward compatibility:
        # If 'vim_content' does not include 'op_id', we a running a legacy NBI version.
        # In such a case, HA is not supported by NBI, 'op_id' is None, and lock_HA() will do nothing.
        # Register 'delete' task here for related future HA operations
        op_id = k8scluster_content.pop('op_id', None)
        if not self.lcm_tasks.lock_HA('k8scluster', 'delete', op_id):
            return

        k8scluster_id = k8scluster_content["_id"]
        k8scluster_content.pop("op_id", None)
        logging_text = "Task k8scluster_delete={} ".format(k8scluster_id)
        self.logger.debug(logging_text + "Enter")

        db_k8scluster = None
        db_k8scluster_update = {}
        exc = None
        operationState_HA = ''
        detailed_status_HA = ''
        try:
            step = "Getting k8scluster='{}' from db".format(k8scluster_id)
            self.logger.debug(logging_text + step)
            db_k8scluster = self.db.get_one("k8sclusters", {"_id": k8scluster_id})
            k8s_hc_id = deep_get(db_k8scluster, ("_admin", "helm-chart", "id"))
            k8s_jb_id = deep_get(db_k8scluster, ("_admin", "juju-bundle", "id"))

            uninstall_sw = deep_get(db_k8scluster, ("_admin", "helm-chart", "created"))
            cluster_removed = True
            if k8s_hc_id:
                uninstall_sw = uninstall_sw or False
                cluster_removed = await self.helm_k8scluster.reset(cluster_uuid=k8s_hc_id, uninstall_sw=uninstall_sw)

            if k8s_jb_id:
                uninstall_sw = uninstall_sw or False
                cluster_removed = await self.juju_k8scluster.reset(cluster_uuid=k8s_jb_id, uninstall_sw=uninstall_sw)

            if k8s_hc_id and cluster_removed:
                step = "Removing k8scluster='{}' from k8srepos".format(k8scluster_id)
                self.logger.debug(logging_text + step)
                db_k8srepo_list = self.db.get_list("k8srepos", {"_admin.cluster-inserted": k8s_hc_id})
                for k8srepo in db_k8srepo_list:
                    try:
                        cluster_list = k8srepo["_admin"]["cluster-inserted"]
                        cluster_list.remove(k8s_hc_id)
                        self.update_db_2("k8srepos", k8srepo["_id"], {"_admin.cluster-inserted": cluster_list})
                    except Exception as e:
                        self.logger.error("{}: {}".format(step, e))
                self.db.del_one("k8sclusters", {"_id": k8scluster_id})
            else:
                raise LcmException("An error happened during the reset of the k8s cluster '{}'".format(k8scluster_id))
            # if not cluster_removed:
            #     raise Exception("K8scluster was not properly removed")

        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            if exc and db_k8scluster:
                db_k8scluster_update["_admin.operationalState"] = "ERROR"
                db_k8scluster_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
                # Mark the WIM 'create' HA task as erroneous
                operationState_HA = 'FAILED'
                detailed_status_HA = "ERROR {}: {}".format(step, exc)
            try:
                if db_k8scluster_update:
                    self.update_db_2("k8sclusters", k8scluster_id, db_k8scluster_update)
                # Register the K8scluster 'delete' HA task either
                # succesful or erroneous, or do nothing (if legacy NBI)
                self.lcm_tasks.register_HA('k8scluster', 'delete', op_id,
                                           operationState=operationState_HA,
                                           detailed_status=detailed_status_HA)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.lcm_tasks.remove("k8sclusters", k8scluster_id, order_id)


class K8sRepoLcm(LcmBase):

    def __init__(self, db, msg, fs, lcm_tasks, vca_config, loop):
        """
        Init, Connect to database, filesystem storage, and messaging
        :param config: two level dictionary with configuration. Top level should contain 'database', 'storage',
        :return: None
        """

        self.logger = logging.getLogger('lcm.k8srepo')
        self.loop = loop
        self.lcm_tasks = lcm_tasks
        self.vca_config = vca_config
        self.fs = fs
        self.db = db

        self.k8srepo = K8sHelmConnector(
            kubectl_command=self.vca_config.get("kubectlpath"),
            helm_command=self.vca_config.get("helmpath"),
            fs=self.fs,
            log=self.logger,
            db=self.db,
            on_update_db=None
        )

        super().__init__(db, msg, fs, self.logger)

    async def create(self, k8srepo_content, order_id):

        # HA tasks and backward compatibility:
        # If 'vim_content' does not include 'op_id', we a running a legacy NBI version.
        # In such a case, HA is not supported by NBI, 'op_id' is None, and lock_HA() will do nothing.
        # Register 'create' task here for related future HA operations

        op_id = k8srepo_content.pop('op_id', None)
        if not self.lcm_tasks.lock_HA('k8srepo', 'create', op_id):
            return

        k8srepo_id = k8srepo_content.get("_id")
        logging_text = "Task k8srepo_create={} ".format(k8srepo_id)
        self.logger.debug(logging_text + "Enter")

        db_k8srepo = None
        db_k8srepo_update = {}
        exc = None
        operationState_HA = ''
        detailed_status_HA = ''
        try:
            step = "Getting k8srepo-id='{}' from db".format(k8srepo_id)
            self.logger.debug(logging_text + step)
            db_k8srepo = self.db.get_one("k8srepos", {"_id": k8srepo_id})
            step = "Getting k8scluster_list from db"
            self.logger.debug(logging_text + step)
            db_k8scluster_list = self.db.get_list("k8sclusters", {})
            db_k8srepo_update["_admin.cluster-inserted"] = []
            task_list = []
            for k8scluster in db_k8scluster_list:
                hc_id = deep_get(k8scluster, ("_admin", "helm-chart", "id"))
                if hc_id:
                    step = "Adding repo to cluster: {}".format(hc_id)
                    self.logger.debug(logging_text + step)
                    task = asyncio.ensure_future(self.k8srepo.repo_add(cluster_uuid=hc_id,
                                                                       name=db_k8srepo["name"], url=db_k8srepo["url"],
                                                                       repo_type="chart"))
                    task_list.append(task)
                    db_k8srepo_update["_admin.cluster-inserted"].append(hc_id)

            done = None
            pending = None
            if len(task_list) > 0:
                self.logger.debug('Waiting for terminate pending tasks...')
                done, pending = await asyncio.wait(task_list, timeout=3600)
                if not pending:
                    self.logger.debug('All tasks finished...')
                else:
                    self.logger.info('There are pending tasks: {}'.format(pending))
            db_k8srepo_update["_admin.operationalState"] = "ENABLED"
        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            if exc and db_k8srepo:
                db_k8srepo_update["_admin.operationalState"] = "ERROR"
                db_k8srepo_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
                # Mark the WIM 'create' HA task as erroneous
                operationState_HA = 'FAILED'
                detailed_status_HA = "ERROR {}: {}".format(step, exc)
            try:
                if db_k8srepo_update:
                    self.update_db_2("k8srepos", k8srepo_id, db_k8srepo_update)
                # Register the K8srepo 'create' HA task either
                # succesful or erroneous, or do nothing (if legacy NBI)
                self.lcm_tasks.register_HA('k8srepo', 'create', op_id,
                                           operationState=operationState_HA,
                                           detailed_status=detailed_status_HA)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.lcm_tasks.remove("k8srepo", k8srepo_id, order_id)

    async def delete(self, k8srepo_content, order_id):

        # HA tasks and backward compatibility:
        # If 'vim_content' does not include 'op_id', we a running a legacy NBI version.
        # In such a case, HA is not supported by NBI, 'op_id' is None, and lock_HA() will do nothing.
        # Register 'delete' task here for related future HA operations
        op_id = k8srepo_content.pop('op_id', None)
        if not self.lcm_tasks.lock_HA('k8srepo', 'delete', op_id):
            return

        k8srepo_id = k8srepo_content.get("_id")
        logging_text = "Task k8srepo_delete={} ".format(k8srepo_id)
        self.logger.debug(logging_text + "Enter")

        db_k8srepo = None
        db_k8srepo_update = {}

        operationState_HA = ''
        detailed_status_HA = ''
        try:
            step = "Getting k8srepo-id='{}' from db".format(k8srepo_id)
            self.logger.debug(logging_text + step)
            db_k8srepo = self.db.get_one("k8srepos", {"_id": k8srepo_id})
            step = "Getting k8scluster_list from db"
            self.logger.debug(logging_text + step)
            db_k8scluster_list = self.db.get_list("k8sclusters", {})

            task_list = []
            for k8scluster in db_k8scluster_list:
                hc_id = deep_get(k8scluster, ("_admin", "helm-chart", "id"))
                if hc_id:
                    task = asyncio.ensure_future(self.k8srepo.repo_remove(cluster_uuid=hc_id,
                                                                          name=db_k8srepo["name"]))
                task_list.append(task)
            done = None
            pending = None
            if len(task_list) > 0:
                self.logger.debug('Waiting for terminate pending tasks...')
                done, pending = await asyncio.wait(task_list, timeout=3600)
                if not pending:
                    self.logger.debug('All tasks finished...')
                else:
                    self.logger.info('There are pending tasks: {}'.format(pending))
            self.db.del_one("k8srepos", {"_id": k8srepo_id})

        except Exception as e:
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
            exc = e
        finally:
            if exc and db_k8srepo:
                db_k8srepo_update["_admin.operationalState"] = "ERROR"
                db_k8srepo_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
                # Mark the WIM 'create' HA task as erroneous
                operationState_HA = 'FAILED'
                detailed_status_HA = "ERROR {}: {}".format(step, exc)
            try:
                if db_k8srepo_update:
                    self.update_db_2("k8srepos", k8srepo_id, db_k8srepo_update)
                # Register the K8srepo 'delete' HA task either
                # succesful or erroneous, or do nothing (if legacy NBI)
                self.lcm_tasks.register_HA('k8srepo', 'delete', op_id,
                                           operationState=operationState_HA,
                                           detailed_status=detailed_status_HA)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            self.lcm_tasks.remove("k8srepo", k8srepo_id, order_id)
