#!/usr/bin/python3
# -*- coding: utf-8 -*-

import asyncio
import yaml
import logging
import logging.handlers
import getopt
import functools
import sys
import traceback
import ROclient
# from osm_lcm import version as lcm_version, version_date as lcm_version_date, ROclient
from osm_common import dbmemory
from osm_common import dbmongo
from osm_common import fslocal
from osm_common import msglocal
from osm_common import msgkafka
from osm_common.dbbase import DbException
from osm_common.fsbase import FsException
from osm_common.msgbase import MsgException
from os import environ, path
from n2vc.vnf import N2VC
from n2vc import version as N2VC_version

from collections import OrderedDict
from copy import deepcopy
from http import HTTPStatus
from time import time


__author__ = "Alfonso Tierno"
min_RO_version = [0, 5, 72]
# uncomment if LCM is installed as library and installed, and get them from __init__.py
lcm_version = '0.1.13'
lcm_version_date = '2018-08-28'


class LcmException(Exception):
    pass


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
            "vim_account": {},
            "sdn": {},
        }

    def register(self, topic, _id, op_id, task_name, task):
        """
        Register a new task
        :param topic: Can be "ns", "vim_account", "sdn"
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
        :param topic: Can be "ns", "vim_account", "sdn"
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
        Cancel all active tasks of a concrete ns, vim_account, sdn identified for _id. If op_id is supplied only this is
        cancelled, and the same with task_name
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


class Lcm:

    def __init__(self, config_file):
        """
        Init, Connect to database, filesystem storage, and messaging
        :param config: two level dictionary with configuration. Top level should contain 'database', 'storage',
        :return: None
        """

        self.db = None
        self.msg = None
        self.fs = None
        self.pings_not_received = 1

        # contains created tasks/futures to be able to cancel
        self.lcm_tasks = TaskRegistry()
        # logging
        self.logger = logging.getLogger('lcm')
        # load configuration
        config = self.read_config_file(config_file)
        self.config = config
        self.ro_config = {
            "endpoint_url": "http://{}:{}/openmano".format(config["RO"]["host"], config["RO"]["port"]),
            "tenant": config.get("tenant", "osm"),
            "logger_name": "lcm.ROclient",
            "loglevel": "ERROR",
        }

        self.vca = config["VCA"]  # TODO VCA
        self.loop = None

        # logging
        log_format_simple = "%(asctime)s %(levelname)s %(name)s %(filename)s:%(lineno)s %(message)s"
        log_formatter_simple = logging.Formatter(log_format_simple, datefmt='%Y-%m-%dT%H:%M:%S')
        config["database"]["logger_name"] = "lcm.db"
        config["storage"]["logger_name"] = "lcm.fs"
        config["message"]["logger_name"] = "lcm.msg"
        if config["global"].get("logfile"):
            file_handler = logging.handlers.RotatingFileHandler(config["global"]["logfile"],
                                                                maxBytes=100e6, backupCount=9, delay=0)
            file_handler.setFormatter(log_formatter_simple)
            self.logger.addHandler(file_handler)
        if not config["global"].get("nologging"):
            str_handler = logging.StreamHandler()
            str_handler.setFormatter(log_formatter_simple)
            self.logger.addHandler(str_handler)

        if config["global"].get("loglevel"):
            self.logger.setLevel(config["global"]["loglevel"])

        # logging other modules
        for k1, logname in {"message": "lcm.msg", "database": "lcm.db", "storage": "lcm.fs"}.items():
            config[k1]["logger_name"] = logname
            logger_module = logging.getLogger(logname)
            if config[k1].get("logfile"):
                file_handler = logging.handlers.RotatingFileHandler(config[k1]["logfile"],
                                                                    maxBytes=100e6, backupCount=9, delay=0)
                file_handler.setFormatter(log_formatter_simple)
                logger_module.addHandler(file_handler)
            if config[k1].get("loglevel"):
                logger_module.setLevel(config[k1]["loglevel"])
        self.logger.critical("starting osm/lcm version {} {}".format(lcm_version, lcm_version_date))
        self.n2vc = N2VC(
            log=self.logger,
            server=config['VCA']['host'],
            port=config['VCA']['port'],
            user=config['VCA']['user'],
            secret=config['VCA']['secret'],
            # TODO: This should point to the base folder where charms are stored,
            # if there is a common one (like object storage). Otherwise, leave
            # it unset and pass it via DeployCharms
            # artifacts=config['VCA'][''],
            artifacts=None,
        )
        # check version of N2VC
        # TODO enhance with int conversion or from distutils.version import LooseVersion
        # or with list(map(int, version.split(".")))
        if N2VC_version < "0.0.2":
            raise LcmException("Not compatible osm/N2VC version '{}'. Needed '0.0.2' or higher".format(N2VC_version))

        try:
            # TODO check database version
            if config["database"]["driver"] == "mongo":
                self.db = dbmongo.DbMongo()
                self.db.db_connect(config["database"])
            elif config["database"]["driver"] == "memory":
                self.db = dbmemory.DbMemory()
                self.db.db_connect(config["database"])
            else:
                raise LcmException("Invalid configuration param '{}' at '[database]':'driver'".format(
                    config["database"]["driver"]))

            if config["storage"]["driver"] == "local":
                self.fs = fslocal.FsLocal()
                self.fs.fs_connect(config["storage"])
            else:
                raise LcmException("Invalid configuration param '{}' at '[storage]':'driver'".format(
                    config["storage"]["driver"]))

            if config["message"]["driver"] == "local":
                self.msg = msglocal.MsgLocal()
                self.msg.connect(config["message"])
            elif config["message"]["driver"] == "kafka":
                self.msg = msgkafka.MsgKafka()
                self.msg.connect(config["message"])
            else:
                raise LcmException("Invalid configuration param '{}' at '[message]':'driver'".format(
                    config["storage"]["driver"]))
        except (DbException, FsException, MsgException) as e:
            self.logger.critical(str(e), exc_info=True)
            raise LcmException(str(e))

    async def check_RO_version(self):
        try:
            RO = ROclient.ROClient(self.loop, **self.ro_config)
            RO_version = await RO.get_version()
            if RO_version < min_RO_version:
                raise LcmException("Not compatible osm/RO version '{}.{}.{}'. Needed '{}.{}.{}' or higher".format(
                    *RO_version, *min_RO_version
                ))
        except ROclient.ROClientException as e:
            self.logger.critical("Error while conneting to osm/RO " + str(e), exc_info=True)
            raise LcmException(str(e))

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

    async def vim_create(self, vim_content, order_id):
        vim_id = vim_content["_id"]
        logging_text = "Task vim_create={} ".format(vim_id)
        self.logger.debug(logging_text + "Enter")
        db_vim = None
        db_vim_update = {}
        exc = None
        RO_sdn_id = None
        try:
            step = "Getting vim-id='{}' from db".format(vim_id)
            db_vim = self.db.get_one("vim_accounts", {"_id": vim_id})
            db_vim_update["_admin.deployed.RO"] = None
            if vim_content.get("config") and vim_content["config"].get("sdn-controller"):
                step = "Getting sdn-controller-id='{}' from db".format(vim_content["config"]["sdn-controller"])
                db_sdn = self.db.get_one("sdns", {"_id": vim_content["config"]["sdn-controller"]})
                if db_sdn.get("_admin") and db_sdn["_admin"].get("deployed") and db_sdn["_admin"]["deployed"].get("RO"):
                    RO_sdn_id = db_sdn["_admin"]["deployed"]["RO"]
                else:
                    raise LcmException("sdn-controller={} is not available. Not deployed at RO".format(
                        vim_content["config"]["sdn-controller"]))

            step = "Creating vim at RO"
            db_vim_update["_admin.detailed-status"] = step
            self.update_db_2("vim_accounts", vim_id, db_vim_update)
            RO = ROclient.ROClient(self.loop, **self.ro_config)
            vim_RO = deepcopy(vim_content)
            vim_RO.pop("_id", None)
            vim_RO.pop("_admin", None)
            vim_RO.pop("schema_version", None)
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

            step = "Creating vim_account at RO"
            db_vim_update["_admin.detailed-status"] = step
            self.update_db_2("vim_accounts", vim_id, db_vim_update)

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
            desc = await RO.attach_datacenter(RO_vim_id, descriptor=vim_account_RO)
            db_vim_update["_admin.deployed.RO-account"] = desc["uuid"]
            db_vim_update["_admin.operationalState"] = "ENABLED"
            db_vim_update["_admin.detailed-status"] = "Done"

            # await asyncio.sleep(15)   # TODO remove. This is for test
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
            if db_vim_update:
                self.update_db_2("vim_accounts", vim_id, db_vim_update)
            self.lcm_tasks.remove("vim_account", vim_id, order_id)

    async def vim_edit(self, vim_content, order_id):
        vim_id = vim_content["_id"]
        logging_text = "Task vim_edit={} ".format(vim_id)
        self.logger.debug(logging_text + "Enter")
        db_vim = None
        exc = None
        RO_sdn_id = None
        RO_vim_id = None
        db_vim_update = {}
        step = "Getting vim-id='{}' from db".format(vim_id)
        try:
            db_vim = self.db.get_one("vim_accounts", {"_id": vim_id})

            # look if previous tasks in process
            task_name, task_dependency = self.lcm_tasks.lookfor_related("vim_account", vim_id, order_id)
            if task_dependency:
                step = "Waiting for related tasks to be completed: {}".format(task_name)
                self.logger.debug(logging_text + step)
                # TODO write this to database
                await asyncio.wait(task_dependency, timeout=3600)

            if db_vim.get("_admin") and db_vim["_admin"].get("deployed") and db_vim["_admin"]["deployed"].get("RO"):
                if vim_content.get("config") and vim_content["config"].get("sdn-controller"):
                    step = "Getting sdn-controller-id='{}' from db".format(vim_content["config"]["sdn-controller"])
                    db_sdn = self.db.get_one("sdns", {"_id": vim_content["config"]["sdn-controller"]})

                    # look if previous tasks in process
                    task_name, task_dependency = self.lcm_tasks.lookfor_related("sdn", db_sdn["_id"])
                    if task_dependency:
                        step = "Waiting for related tasks to be completed: {}".format(task_name)
                        self.logger.debug(logging_text + step)
                        # TODO write this to database
                        await asyncio.wait(task_dependency, timeout=3600)

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
                vim_RO.pop("schema_version", None)
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
                for k in ("vim_tenant_name", "vim_password", "config"):
                    if k in vim_content:
                        vim_account_RO[k] = vim_content[k]
                if "vim_user" in vim_content:
                    vim_content["vim_username"] = vim_content["vim_user"]
                # vim_account must be edited always even if empty in order to ensure changes are translated to RO
                # vim_thread. RO will remove and relaunch a new thread for this vim_account
                await RO.edit("vim_account", RO_vim_id, descriptor=vim_account_RO)
                db_vim_update["_admin.operationalState"] = "ENABLED"

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
            if db_vim_update:
                self.update_db_2("vim_accounts", vim_id, db_vim_update)
            self.lcm_tasks.remove("vim_account", vim_id, order_id)

    async def vim_delete(self, vim_id, order_id):
        logging_text = "Task vim_delete={} ".format(vim_id)
        self.logger.debug(logging_text + "Enter")
        db_vim = None
        db_vim_update = {}
        exc = None
        step = "Getting vim from db"
        try:
            db_vim = self.db.get_one("vim_accounts", {"_id": vim_id})
            if db_vim.get("_admin") and db_vim["_admin"].get("deployed") and db_vim["_admin"]["deployed"].get("RO"):
                RO_vim_id = db_vim["_admin"]["deployed"]["RO"]
                RO = ROclient.ROClient(self.loop, **self.ro_config)
                step = "Detaching vim from RO tenant"
                try:
                    await RO.detach_datacenter(RO_vim_id)
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
                self.logger.error(logging_text + "Nohing to remove at RO")
            self.db.del_one("vim_accounts", {"_id": vim_id})
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
            if db_vim_update:
                self.update_db_2("vim_accounts", vim_id, db_vim_update)
            self.lcm_tasks.remove("vim_account", vim_id, order_id)

    async def sdn_create(self, sdn_content, order_id):
        sdn_id = sdn_content["_id"]
        logging_text = "Task sdn_create={} ".format(sdn_id)
        self.logger.debug(logging_text + "Enter")
        db_sdn = None
        db_sdn_update = {}
        RO_sdn_id = None
        exc = None
        try:
            step = "Getting sdn from db"
            db_sdn = self.db.get_one("sdns", {"_id": sdn_id})
            db_sdn_update["_admin.deployed.RO"] = None

            step = "Creating sdn at RO"
            RO = ROclient.ROClient(self.loop, **self.ro_config)
            sdn_RO = deepcopy(sdn_content)
            sdn_RO.pop("_id", None)
            sdn_RO.pop("_admin", None)
            sdn_RO.pop("schema_version", None)
            sdn_RO.pop("schema_type", None)
            sdn_RO.pop("description", None)
            desc = await RO.create("sdn", descriptor=sdn_RO)
            RO_sdn_id = desc["uuid"]
            db_sdn_update["_admin.deployed.RO"] = RO_sdn_id
            db_sdn_update["_admin.operationalState"] = "ENABLED"
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
                db_sdn_update["_admin.operationalState"] = "ERROR"
                db_sdn_update["_admin.detailed-status"] = "ERROR {}: {}".format(step, exc)
            if db_sdn_update:
                self.update_db_2("sdns", sdn_id, db_sdn_update)
            self.lcm_tasks.remove("sdn", sdn_id, order_id)

    async def sdn_edit(self, sdn_content, order_id):
        sdn_id = sdn_content["_id"]
        logging_text = "Task sdn_edit={} ".format(sdn_id)
        self.logger.debug(logging_text + "Enter")
        db_sdn = None
        db_sdn_update = {}
        exc = None
        step = "Getting sdn from db"
        try:
            db_sdn = self.db.get_one("sdns", {"_id": sdn_id})
            if db_sdn.get("_admin") and db_sdn["_admin"].get("deployed") and db_sdn["_admin"]["deployed"].get("RO"):
                RO_sdn_id = db_sdn["_admin"]["deployed"]["RO"]
                RO = ROclient.ROClient(self.loop, **self.ro_config)
                step = "Editing sdn at RO"
                sdn_RO = deepcopy(sdn_content)
                sdn_RO.pop("_id", None)
                sdn_RO.pop("_admin", None)
                sdn_RO.pop("schema_version", None)
                sdn_RO.pop("schema_type", None)
                sdn_RO.pop("description", None)
                if sdn_RO:
                    await RO.edit("sdn", RO_sdn_id, descriptor=sdn_RO)
                db_sdn_update["_admin.operationalState"] = "ENABLED"

            self.logger.debug(logging_text + "Exit Ok RO_sdn_id".format(RO_sdn_id))
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
            if db_sdn_update:
                self.update_db_2("sdns", sdn_id, db_sdn_update)
            self.lcm_tasks.remove("sdn", sdn_id, order_id)

    async def sdn_delete(self, sdn_id, order_id):
        logging_text = "Task sdn_delete={} ".format(sdn_id)
        self.logger.debug(logging_text + "Enter")
        db_sdn = None
        db_sdn_update = {}
        exc = None
        step = "Getting sdn from db"
        try:
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
            if db_sdn_update:
                self.update_db_2("sdns", sdn_id, db_sdn_update)
            self.lcm_tasks.remove("sdn", sdn_id, order_id)

    def vnfd2RO(self, vnfd, new_id=None):
        """
        Converts creates a new vnfd descriptor for RO base on input OSM IM vnfd
        :param vnfd: input vnfd
        :param new_id: overrides vnf id if provided
        :return: copy of vnfd
        """
        ci_file = None
        try:
            vnfd_RO = deepcopy(vnfd)
            vnfd_RO.pop("_id", None)
            vnfd_RO.pop("_admin", None)
            if new_id:
                vnfd_RO["id"] = new_id
            for vdu in vnfd_RO["vdu"]:
                if "cloud-init-file" in vdu:
                    base_folder = vnfd["_admin"]["storage"]
                    clout_init_file = "{}/{}/cloud_init/{}".format(
                        base_folder["folder"],
                        base_folder["pkg-dir"],
                        vdu["cloud-init-file"]
                    )
                    ci_file = self.fs.file_open(clout_init_file, "r")
                    # TODO: detect if binary or text. Propose to read as binary and try to decode to utf8. If fails
                    #  convert to base 64 or similar
                    clout_init_content = ci_file.read()
                    ci_file.close()
                    ci_file = None
                    vdu.pop("cloud-init-file", None)
                    vdu["cloud-init"] = clout_init_content
            # remnove unused by RO configuration, monitoring, scaling
            vnfd_RO.pop("vnf-configuration", None)
            vnfd_RO.pop("monitoring-param", None)
            vnfd_RO.pop("scaling-group-descriptor", None)
            return vnfd_RO
        except FsException as e:
            raise LcmException("Error reading file at vnfd {}: {} ".format(vnfd["_id"], e))
        finally:
            if ci_file:
                ci_file.close()

    def n2vc_callback(self, model_name, application_name, status, message, db_nsr, db_nslcmop, task=None):
        """
        Callback both for charm status change and task completion
        :param model_name: Charm model name
        :param application_name: Charm application name
        :param status: Can be
            - blocked: The unit needs manual intervention
            - maintenance: The unit is actively deploying/configuring
            - waiting: The unit is waiting for another charm to be ready
            - active: The unit is deployed, configured, and ready
            - error: The charm has failed and needs attention.
            - terminated: The charm has been destroyed
            - removing,
            - removed
        :param message: detailed message error
        :param db_nsr: nsr database content
        :param db_nslcmop: nslcmop database content
        :param task: None for charm status change, or task for completion task callback
        :return:
        """
        nsr_id = None
        nslcmop_id = None
        db_nsr_update = {}
        db_nslcmop_update = {}
        try:
            nsr_id = db_nsr["_id"]
            nsr_lcm = db_nsr["_admin"]["deployed"]
            nslcmop_id = db_nslcmop["_id"]
            ns_operation = db_nslcmop["lcmOperationType"]
            logging_text = "Task ns={} {}={} [n2vc_callback] application={}".format(nsr_id, ns_operation, nslcmop_id,
                                                                                    application_name)
            vca_deployed = nsr_lcm["VCA"].get(application_name)
            if not vca_deployed:
                self.logger.error(logging_text + " Not present at nsr._admin.deployed.VCA")
                return

            if task:
                if task.cancelled():
                    self.logger.debug(logging_text + " task Cancelled")
                    # TODO update db_nslcmop
                    return

                if task.done():
                    exc = task.exception()
                    if exc:
                        self.logger.error(logging_text + " task Exception={}".format(exc))
                        if ns_operation in ("instantiate", "terminate"):
                            vca_deployed['operational-status'] = "error"
                            db_nsr_update["_admin.deployed.VCA.{}.operational-status".format(application_name)] = \
                                "error"
                            vca_deployed['detailed-status'] = str(exc)
                            db_nsr_update[
                                "_admin.deployed.VCA.{}.detailed-status".format(application_name)] = str(exc)
                        elif ns_operation == "action":
                            db_nslcmop_update["operationState"] = "FAILED"
                            db_nslcmop_update["detailed-status"] = str(exc)
                            db_nslcmop_update["statusEnteredTime"] = time()
                            return

                    else:
                        self.logger.debug(logging_text + " task Done")
                        # TODO revise with Adam if action is finished and ok when task is done
                        if ns_operation == "action":
                            db_nslcmop_update["operationState"] = "COMPLETED"
                            db_nslcmop_update["detailed-status"] = "Done"
                            db_nslcmop_update["statusEnteredTime"] = time()
                        # task is Done, but callback is still ongoing. So ignore
                        return
            elif status:
                self.logger.debug(logging_text + " Enter status={}".format(status))
                if vca_deployed['operational-status'] == status:
                    return  # same status, ignore
                vca_deployed['operational-status'] = status
                db_nsr_update["_admin.deployed.VCA.{}.operational-status".format(application_name)] = status
                vca_deployed['detailed-status'] = str(message)
                db_nsr_update["_admin.deployed.VCA.{}.detailed-status".format(application_name)] = str(message)
            else:
                self.logger.critical(logging_text + " Enter with bad parameters", exc_info=True)
                return

            all_active = True
            status_map = {}
            n2vc_error_text = []   # contain text error list. If empty no one is in error status
            for _, vca_info in nsr_lcm["VCA"].items():
                vca_status = vca_info["operational-status"]
                if vca_status not in status_map:
                    # Initialize it
                    status_map[vca_status] = 0
                status_map[vca_status] += 1

                if vca_status != "active":
                    all_active = False
                if vca_status in ("error", "blocked"):
                    n2vc_error_text.append("member_vnf_index={} vdu_id={} {}: {}".format(vca_info["member-vnf-index"],
                                                                                         vca_info["vdu_id"], vca_status,
                                                                                         vca_info["detailed-status"]))

            if all_active:
                self.logger.debug(logging_text + " All active")
                db_nsr_update["config-status"] = "configured"
                db_nsr_update["detailed-status"] = "done"
                db_nslcmop_update["operationState"] = "COMPLETED"
                db_nslcmop_update["detailed-status"] = "Done"
                db_nslcmop_update["statusEnteredTime"] = time()
            elif n2vc_error_text:
                db_nsr_update["config-status"] = "failed"
                error_text = "fail configuring " + ";".join(n2vc_error_text)
                db_nsr_update["detailed-status"] = error_text
                db_nslcmop_update["operationState"] = "FAILED_TEMP"
                db_nslcmop_update["detailed-status"] = error_text
                db_nslcmop_update["statusEnteredTime"] = time()
            else:
                cs = "configuring: "
                separator = ""
                for status, num in status_map.items():
                    cs += separator + "{}: {}".format(status, num)
                    separator = ", "
                db_nsr_update["config-status"] = cs
                db_nsr_update["detailed-status"] = cs
                db_nslcmop_update["detailed-status"] = cs

        except Exception as e:
            self.logger.critical(logging_text + " Exception {}".format(e), exc_info=True)
        finally:
            try:
                if db_nslcmop_update:
                    self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)
                if db_nsr_update:
                    self.update_db_2("nsrs", nsr_id, db_nsr_update)
            except Exception as e:
                self.logger.critical(logging_text + " Update database Exception {}".format(e), exc_info=True)

    def ns_params_2_RO(self, ns_params, nsd, vnfd_dict):
        """
        Creates a RO ns descriptor from OSM ns_instantite params
        :param ns_params: OSM instantiate params
        :return: The RO ns descriptor
        """
        vim_2_RO = {}

        def vim_account_2_RO(vim_account):
            if vim_account in vim_2_RO:
                return vim_2_RO[vim_account]

            db_vim = self.db.get_one("vim_accounts", {"_id": vim_account})
            if db_vim["_admin"]["operationalState"] != "ENABLED":
                raise LcmException("VIM={} is not available. operationalState={}".format(
                    vim_account, db_vim["_admin"]["operationalState"]))
            RO_vim_id = db_vim["_admin"]["deployed"]["RO"]
            vim_2_RO[vim_account] = RO_vim_id
            return RO_vim_id

        def ip_profile_2_RO(ip_profile):
            RO_ip_profile = deepcopy((ip_profile))
            if "dns-server" in RO_ip_profile:
                RO_ip_profile["dns-address"] = RO_ip_profile.pop("dns-server")
            if RO_ip_profile.get("ip-version") == "ipv4":
                RO_ip_profile["ip-version"] = "IPv4"
            if RO_ip_profile.get("ip-version") == "ipv6":
                RO_ip_profile["ip-version"] = "IPv6"
            if "dhcp-params" in RO_ip_profile:
                RO_ip_profile["dhcp"] = RO_ip_profile.pop("dhcp-params")
            return RO_ip_profile

        if not ns_params:
            return None
        RO_ns_params = {
            # "name": ns_params["nsName"],
            # "description": ns_params.get("nsDescription"),
            "datacenter": vim_account_2_RO(ns_params["vimAccountId"]),
            # "scenario": ns_params["nsdId"],
            "vnfs": {},
            "networks": {},
        }
        if ns_params.get("ssh-authorized-key"):
            RO_ns_params["cloud-config"] = {"key-pairs": ns_params["ssh-authorized-key"]}
        if ns_params.get("vnf"):
            for vnf_params in ns_params["vnf"]:
                for constituent_vnfd in nsd["constituent-vnfd"]:
                    if constituent_vnfd["member-vnf-index"] == vnf_params["member-vnf-index"]:
                        vnf_descriptor = vnfd_dict[constituent_vnfd["vnfd-id-ref"]]
                        break
                else:
                    raise LcmException("Invalid instantiate parameter vnf:member-vnf-index={} is not present at nsd:"
                                       "constituent-vnfd".format(vnf_params["member-vnf-index"]))
                RO_vnf = {"vdus": {}, "networks": {}}
                if vnf_params.get("vimAccountId"):
                    RO_vnf["datacenter"] = vim_account_2_RO(vnf_params["vimAccountId"])
                if vnf_params.get("vdu"):
                    for vdu_params in vnf_params["vdu"]:
                        RO_vnf["vdus"][vdu_params["id"]] = {}
                        if vdu_params.get("volume"):
                            RO_vnf["vdus"][vdu_params["id"]]["devices"] = {}
                            for volume_params in vdu_params["volume"]:
                                RO_vnf["vdus"][vdu_params["id"]]["devices"][volume_params["name"]] = {}
                                if volume_params.get("vim-volume-id"):
                                    RO_vnf["vdus"][vdu_params["id"]]["devices"][volume_params["name"]]["vim_id"] = \
                                        volume_params["vim-volume-id"]
                        if vdu_params.get("interface"):
                            RO_vnf["vdus"][vdu_params["id"]]["interfaces"] = {}
                            for interface_params in vdu_params["interface"]:
                                RO_interface = {}
                                RO_vnf["vdus"][vdu_params["id"]]["interfaces"][interface_params["name"]] = RO_interface
                                if interface_params.get("ip-address"):
                                    RO_interface["ip_address"] = interface_params["ip-address"]
                                if interface_params.get("mac-address"):
                                    RO_interface["mac_address"] = interface_params["mac-address"]
                                if interface_params.get("floating-ip-required"):
                                    RO_interface["floating-ip"] = interface_params["floating-ip-required"]
                if vnf_params.get("internal-vld"):
                    for internal_vld_params in vnf_params["internal-vld"]:
                        RO_vnf["networks"][internal_vld_params["name"]] = {}
                        if internal_vld_params.get("vim-network-name"):
                            RO_vnf["networks"][internal_vld_params["name"]]["vim-network-name"] = \
                                internal_vld_params["vim-network-name"]
                        if internal_vld_params.get("ip-profile"):
                            RO_vnf["networks"][internal_vld_params["name"]]["ip-profile"] = \
                                ip_profile_2_RO(internal_vld_params["ip-profile"])
                        if internal_vld_params.get("internal-connection-point"):
                            for icp_params in internal_vld_params["internal-connection-point"]:
                                # look for interface
                                iface_found = False
                                for vdu_descriptor in vnf_descriptor["vdu"]:
                                    for vdu_interface in vdu_descriptor["interface"]:
                                        if vdu_interface.get("internal-connection-point-ref") == icp_params["id-ref"]:
                                            if vdu_descriptor["id"] not in RO_vnf["vdus"]:
                                                RO_vnf["vdus"][vdu_descriptor["id"]] = {}
                                            if "interfaces" not in RO_vnf["vdus"][vdu_descriptor["id"]]:
                                                RO_vnf["vdus"][vdu_descriptor["id"]]["interfaces"] = {}
                                            RO_ifaces = RO_vnf["vdus"][vdu_descriptor["id"]]["interfaces"]
                                            if vdu_interface["name"] not in RO_ifaces:
                                                RO_ifaces[vdu_interface["name"]] = {}

                                            RO_ifaces[vdu_interface["name"]]["ip_address"] = icp_params["ip-address"]
                                            iface_found = True
                                            break
                                    if iface_found:
                                        break
                                else:
                                    raise LcmException("Invalid instantiate parameter vnf:member-vnf-index[{}]:"
                                                       "internal-vld:id-ref={} is not present at vnfd:internal-"
                                                       "connection-point".format(vnf_params["member-vnf-index"],
                                                                                 icp_params["id-ref"]))

                if not RO_vnf["vdus"]:
                    del RO_vnf["vdus"]
                if not RO_vnf["networks"]:
                    del RO_vnf["networks"]
                if RO_vnf:
                    RO_ns_params["vnfs"][vnf_params["member-vnf-index"]] = RO_vnf
        if ns_params.get("vld"):
            for vld_params in ns_params["vld"]:
                RO_vld = {}
                if "ip-profile" in vld_params:
                    RO_vld["ip-profile"] = ip_profile_2_RO(vld_params["ip-profile"])
                if "vim-network-name" in vld_params:
                    RO_vld["sites"] = []
                    if isinstance(vld_params["vim-network-name"], dict):
                        for vim_account, vim_net in vld_params["vim-network-name"].items():
                            RO_vld["sites"].append({
                                "netmap-use": vim_net,
                                "datacenter": vim_account_2_RO(vim_account)
                            })
                    else:  # isinstance str
                        RO_vld["sites"].append({"netmap-use": vld_params["vim-network-name"]})
                if RO_vld:
                    RO_ns_params["networks"][vld_params["name"]] = RO_vld
        return RO_ns_params

    def ns_update_vnfr(self, db_vnfrs, nsr_desc_RO):
        """
        Updates database vnfr with the RO info, e.g. ip_address, vim_id... Descriptor db_vnfrs is also updated
        :param db_vnfrs:
        :param nsr_desc_RO:
        :return:
        """
        for vnf_index, db_vnfr in db_vnfrs.items():
            for vnf_RO in nsr_desc_RO["vnfs"]:
                if vnf_RO["member_vnf_index"] == vnf_index:
                    vnfr_update = {}
                    db_vnfr["ip-address"] = vnfr_update["ip-address"] = vnf_RO.get("ip_address")
                    vdur_list = []
                    for vdur_RO in vnf_RO.get("vms", ()):
                        vdur = {
                            "vim-id": vdur_RO.get("vim_vm_id"),
                            "ip-address": vdur_RO.get("ip_address"),
                            "vdu-id-ref": vdur_RO.get("vdu_osm_id"),
                            "name": vdur_RO.get("vim_name"),
                            "status": vdur_RO.get("status"),
                            "status-detailed": vdur_RO.get("error_msg"),
                            "interfaces": []
                        }

                        for interface_RO in vdur_RO.get("interfaces", ()):
                            vdur["interfaces"].append({
                                "ip-address": interface_RO.get("ip_address"),
                                "mac-address": interface_RO.get("mac_address"),
                                "name": interface_RO.get("external_name"),
                            })
                        vdur_list.append(vdur)
                    db_vnfr["vdur"] = vnfr_update["vdur"] = vdur_list
                    self.update_db_2("vnfrs", db_vnfr["_id"], vnfr_update)
                    break

            else:
                raise LcmException("ns_update_vnfr: Not found member_vnf_index={} at RO info".format(vnf_index))

    async def create_monitoring(self, nsr_id, vnf_member_index, vnfd_desc):
        if not vnfd_desc.get("scaling-group-descriptor"):
            return
        for scaling_group in vnfd_desc["scaling-group-descriptor"]:
            scaling_policy_desc = {}
            scaling_desc = {
                "ns_id": nsr_id,
                "scaling_group_descriptor": {
                    "name": scaling_group["name"],
                    "scaling_policy": scaling_policy_desc
                }
            }
            for scaling_policy in scaling_group.get("scaling-policy"):
                scaling_policy_desc["scale_in_operation_type"] = scaling_policy_desc["scale_out_operation_type"] = \
                    scaling_policy["scaling-type"]
                scaling_policy_desc["threshold_time"] = scaling_policy["threshold-time"]
                scaling_policy_desc["cooldown_time"] = scaling_policy["cooldown-time"]
                scaling_policy_desc["scaling_criteria"] = []
                for scaling_criteria in scaling_policy.get("scaling-criteria"):
                    scaling_criteria_desc = {"scale_in_threshold": scaling_criteria.get("scale-in-threshold"),
                                             "scale_out_threshold": scaling_criteria.get("scale-out-threshold"),
                                             }
                    if not scaling_criteria.get("vnf-monitoring-param-ref"):
                        continue
                    for monitoring_param in vnfd_desc.get("monitoring-param", ()):
                        if monitoring_param["id"] == scaling_criteria["vnf-monitoring-param-ref"]:
                            scaling_criteria_desc["monitoring_param"] = {
                                "id": monitoring_param["id"],
                                "name": monitoring_param["name"],
                                "aggregation_type": monitoring_param.get("aggregation-type"),
                                "vdu_name": monitoring_param.get("vdu-ref"),
                                "vnf_member_index": vnf_member_index,
                            }

                            scaling_policy_desc["scaling_criteria"].append(scaling_criteria_desc)
                            break
                    else:
                        self.logger.error(
                            "Task ns={} member_vnf_index={} Invalid vnfd vnf-monitoring-param-ref={} not in "
                            "monitoring-param list".format(nsr_id, vnf_member_index,
                                                           scaling_criteria["vnf-monitoring-param-ref"]))

            await self.msg.aiowrite("lcm_pm", "configure_scaling", scaling_desc, self.loop)

    async def ns_instantiate(self, nsr_id, nslcmop_id):
        logging_text = "Task ns={} instantiate={} ".format(nsr_id, nslcmop_id)
        self.logger.debug(logging_text + "Enter")
        # get all needed from database
        db_nsr = None
        db_nslcmop = None
        db_nsr_update = {}
        db_nslcmop_update = {}
        db_vnfrs = {}
        RO_descriptor_number = 0   # number of descriptors created at RO
        descriptor_id_2_RO = {}    # map between vnfd/nsd id to the id used at RO
        exc = None
        try:
            step = "Getting nslcmop={} from db".format(nslcmop_id)
            db_nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_id})
            step = "Getting nsr={} from db".format(nsr_id)
            db_nsr = self.db.get_one("nsrs", {"_id": nsr_id})
            ns_params = db_nsr.get("instantiate_params")
            nsd = db_nsr["nsd"]
            nsr_name = db_nsr["name"]   # TODO short-name??
            needed_vnfd = {}
            vnfr_filter = {"nsr-id-ref": nsr_id, "member-vnf-index-ref": None}
            for c_vnf in nsd["constituent-vnfd"]:
                vnfd_id = c_vnf["vnfd-id-ref"]
                vnfr_filter["member-vnf-index-ref"] = c_vnf["member-vnf-index"]
                step = "Getting vnfr={} of nsr={} from db".format(c_vnf["member-vnf-index"], nsr_id)
                db_vnfrs[c_vnf["member-vnf-index"]] = self.db.get_one("vnfrs", vnfr_filter)
                if vnfd_id not in needed_vnfd:
                    step = "Getting vnfd={} from db".format(vnfd_id)
                    needed_vnfd[vnfd_id] = self.db.get_one("vnfds", {"id": vnfd_id})

            nsr_lcm = db_nsr["_admin"].get("deployed")
            if not nsr_lcm:
                nsr_lcm = db_nsr["_admin"]["deployed"] = {
                    "id": nsr_id,
                    "RO": {"vnfd_id": {}, "nsd_id": None, "nsr_id": None, "nsr_status": "SCHEDULED"},
                    "nsr_ip": {},
                    "VCA": {},
                }
            db_nsr_update["detailed-status"] = "creating"
            db_nsr_update["operational-status"] = "init"

            RO = ROclient.ROClient(self.loop, **self.ro_config)

            # get vnfds, instantiate at RO
            for vnfd_id, vnfd in needed_vnfd.items():
                step = db_nsr_update["detailed-status"] = "Creating vnfd={} at RO".format(vnfd_id)
                # self.logger.debug(logging_text + step)
                vnfd_id_RO = "{}.{}.{}".format(nsr_id, RO_descriptor_number, vnfd_id[:23])
                descriptor_id_2_RO[vnfd_id] = vnfd_id_RO
                RO_descriptor_number += 1

                # look if present
                vnfd_list = await RO.get_list("vnfd", filter_by={"osm_id": vnfd_id_RO})
                if vnfd_list:
                    db_nsr_update["_admin.deployed.RO.vnfd_id.{}".format(vnfd_id)] = vnfd_list[0]["uuid"]
                    self.logger.debug(logging_text + "vnfd={} exists at RO. Using RO_id={}".format(
                        vnfd_id, vnfd_list[0]["uuid"]))
                else:
                    vnfd_RO = self.vnfd2RO(vnfd, vnfd_id_RO)
                    desc = await RO.create("vnfd", descriptor=vnfd_RO)
                    db_nsr_update["_admin.deployed.RO.vnfd_id.{}".format(vnfd_id)] = desc["uuid"]
                    db_nsr_update["_admin.nsState"] = "INSTANTIATED"
                    self.logger.debug(logging_text + "vnfd={} created at RO. RO_id={}".format(
                        vnfd_id, desc["uuid"]))
                self.update_db_2("nsrs", nsr_id, db_nsr_update)

            # create nsd at RO
            nsd_id = nsd["id"]
            step = db_nsr_update["detailed-status"] = "Creating nsd={} at RO".format(nsd_id)
            # self.logger.debug(logging_text + step)

            RO_osm_nsd_id = "{}.{}.{}".format(nsr_id, RO_descriptor_number, nsd_id[:23])
            descriptor_id_2_RO[nsd_id] = RO_osm_nsd_id
            RO_descriptor_number += 1
            nsd_list = await RO.get_list("nsd", filter_by={"osm_id": RO_osm_nsd_id})
            if nsd_list:
                db_nsr_update["_admin.deployed.RO.nsd_id"] = RO_nsd_uuid = nsd_list[0]["uuid"]
                self.logger.debug(logging_text + "nsd={} exists at RO. Using RO_id={}".format(
                    nsd_id, RO_nsd_uuid))
            else:
                nsd_RO = deepcopy(nsd)
                nsd_RO["id"] = RO_osm_nsd_id
                nsd_RO.pop("_id", None)
                nsd_RO.pop("_admin", None)
                for c_vnf in nsd_RO["constituent-vnfd"]:
                    vnfd_id = c_vnf["vnfd-id-ref"]
                    c_vnf["vnfd-id-ref"] = descriptor_id_2_RO[vnfd_id]
                desc = await RO.create("nsd", descriptor=nsd_RO)
                db_nsr_update["_admin.nsState"] = "INSTANTIATED"
                db_nsr_update["_admin.deployed.RO.nsd_id"] = RO_nsd_uuid = desc["uuid"]
                self.logger.debug(logging_text + "nsd={} created at RO. RO_id={}".format(nsd_id, RO_nsd_uuid))
            self.update_db_2("nsrs", nsr_id, db_nsr_update)

            # Crate ns at RO
            # if present use it unless in error status
            RO_nsr_id = db_nsr["_admin"].get("deployed", {}).get("RO", {}).get("nsr_id")
            if RO_nsr_id:
                try:
                    step = db_nsr_update["detailed-status"] = "Looking for existing ns at RO"
                    # self.logger.debug(logging_text + step + " RO_ns_id={}".format(RO_nsr_id))
                    desc = await RO.show("ns", RO_nsr_id)
                except ROclient.ROClientException as e:
                    if e.http_code != HTTPStatus.NOT_FOUND:
                        raise
                    RO_nsr_id = db_nsr_update["_admin.deployed.RO.nsr_id"] = None
                if RO_nsr_id:
                    ns_status, ns_status_info = RO.check_ns_status(desc)
                    db_nsr_update["_admin.deployed.RO.nsr_status"] = ns_status
                    if ns_status == "ERROR":
                        step = db_nsr_update["detailed-status"] = "Deleting ns at RO. RO_ns_id={}".format(RO_nsr_id)
                        self.logger.debug(logging_text + step)
                        await RO.delete("ns", RO_nsr_id)
                        RO_nsr_id = db_nsr_update["_admin.deployed.RO.nsr_id"] = None
            if not RO_nsr_id:
                step = db_nsr_update["detailed-status"] = "Creating ns at RO"
                # self.logger.debug(logging_text + step)

                # check if VIM is creating and wait  look if previous tasks in process
                task_name, task_dependency = self.lcm_tasks.lookfor_related("vim_account", ns_params["vimAccountId"])
                if task_dependency:
                    step = "Waiting for related tasks to be completed: {}".format(task_name)
                    self.logger.debug(logging_text + step)
                    await asyncio.wait(task_dependency, timeout=3600)
                if ns_params.get("vnf"):
                    for vnf in ns_params["vnf"]:
                        if "vimAccountId" in vnf:
                            task_name, task_dependency = self.lcm_tasks.lookfor_related("vim_account",
                                                                                        vnf["vimAccountId"])
                        if task_dependency:
                            step = "Waiting for related tasks to be completed: {}".format(task_name)
                            self.logger.debug(logging_text + step)
                            await asyncio.wait(task_dependency, timeout=3600)

                RO_ns_params = self.ns_params_2_RO(ns_params, nsd, needed_vnfd)
                desc = await RO.create("ns", descriptor=RO_ns_params,
                                       name=db_nsr["name"],
                                       scenario=RO_nsd_uuid)
                RO_nsr_id = db_nsr_update["_admin.deployed.RO.nsr_id"] = desc["uuid"]
                db_nsr_update["_admin.nsState"] = "INSTANTIATED"
                db_nsr_update["_admin.deployed.RO.nsr_status"] = "BUILD"
                self.logger.debug(logging_text + "ns created at RO. RO_id={}".format(desc["uuid"]))
            self.update_db_2("nsrs", nsr_id, db_nsr_update)

            # update VNFR vimAccount
            step = "Updating VNFR vimAcccount"
            for vnf_index, vnfr in db_vnfrs.items():
                if vnfr.get("vim-account-id"):
                    continue
                vnfr_update = {"vim-account-id": db_nsr["instantiate_params"]["vimAccountId"]}
                if db_nsr["instantiate_params"].get("vnf"):
                    for vnf_params in db_nsr["instantiate_params"]["vnf"]:
                        if vnf_params.get("member-vnf-index") == vnf_index:
                            if vnf_params.get("vimAccountId"):
                                vnfr_update["vim-account-id"] = vnf_params.get("vimAccountId")
                            break
                self.update_db_2("vnfrs", vnfr["_id"], vnfr_update)

            # wait until NS is ready
            step = ns_status_detailed = detailed_status = "Waiting ns ready at RO. RO_id={}".format(RO_nsr_id)
            detailed_status_old = None
            self.logger.debug(logging_text + step)

            deployment_timeout = 2 * 3600   # Two hours
            while deployment_timeout > 0:
                desc = await RO.show("ns", RO_nsr_id)
                ns_status, ns_status_info = RO.check_ns_status(desc)
                db_nsr_update["admin.deployed.RO.nsr_status"] = ns_status
                if ns_status == "ERROR":
                    raise ROclient.ROClientException(ns_status_info)
                elif ns_status == "BUILD":
                    detailed_status = ns_status_detailed + "; {}".format(ns_status_info)
                elif ns_status == "ACTIVE":
                    step = detailed_status = "Waiting for management IP address reported by the VIM"
                    try:
                        nsr_lcm["nsr_ip"] = RO.get_ns_vnf_info(desc)
                        break
                    except ROclient.ROClientException as e:
                        if e.http_code != 409:  # IP address is not ready return code is 409 CONFLICT
                            raise e
                else:
                    assert False, "ROclient.check_ns_status returns unknown {}".format(ns_status)
                if detailed_status != detailed_status_old:
                    detailed_status_old = db_nsr_update["detailed-status"] = detailed_status
                    self.update_db_2("nsrs", nsr_id, db_nsr_update)
                await asyncio.sleep(5, loop=self.loop)
                deployment_timeout -= 5
            if deployment_timeout <= 0:
                raise ROclient.ROClientException("Timeout waiting ns to be ready")

            step = "Updating VNFRs"
            self.ns_update_vnfr(db_vnfrs, desc)

            db_nsr["detailed-status"] = "Configuring vnfr"
            self.update_db_2("nsrs", nsr_id, db_nsr_update)

            # The parameters we'll need to deploy a charm
            number_to_configure = 0

            def deploy(vnf_index, vdu_id, mgmt_ip_address, config_primitive=None):
                """An inner function to deploy the charm from either vnf or vdu
                vnf_index is mandatory. vdu_id can be None for a vnf configuration or the id for vdu configuration
                """
                if not mgmt_ip_address:
                    raise LcmException("vnfd/vdu has not management ip address to configure it")
                # Login to the VCA.
                # if number_to_configure == 0:
                #     self.logger.debug("Logging into N2VC...")
                #     task = asyncio.ensure_future(self.n2vc.login())
                #     yield from asyncio.wait_for(task, 30.0)
                #     self.logger.debug("Logged into N2VC!")

                # # await self.n2vc.login()

                # Note: The charm needs to exist on disk at the location
                # specified by charm_path.
                base_folder = vnfd["_admin"]["storage"]
                storage_params = self.fs.get_params()
                charm_path = "{}{}/{}/charms/{}".format(
                    storage_params["path"],
                    base_folder["folder"],
                    base_folder["pkg-dir"],
                    proxy_charm
                )

                # Setup the runtime parameters for this VNF
                params = {'rw_mgmt_ip': mgmt_ip_address}
                if config_primitive:
                    params["initial-config-primitive"] = config_primitive

                # ns_name will be ignored in the current version of N2VC
                # but will be implemented for the next point release.
                model_name = 'default'
                vdu_id_text = "vnfd"
                if vdu_id:
                    vdu_id_text = vdu_id
                application_name = self.n2vc.FormatApplicationName(
                    nsr_name,
                    vnf_index,
                    vdu_id_text
                )
                if not nsr_lcm.get("VCA"):
                    nsr_lcm["VCA"] = {}
                nsr_lcm["VCA"][application_name] = db_nsr_update["_admin.deployed.VCA.{}".format(application_name)] = {
                    "member-vnf-index": vnf_index,
                    "vdu_id": vdu_id,
                    "model": model_name,
                    "application": application_name,
                    "operational-status": "init",
                    "detailed-status": "",
                    "vnfd_id": vnfd_id,
                }
                self.update_db_2("nsrs", nsr_id, db_nsr_update)

                self.logger.debug("Task create_ns={} Passing artifacts path '{}' for {}".format(nsr_id, charm_path,
                                                                                                proxy_charm))
                task = asyncio.ensure_future(
                    self.n2vc.DeployCharms(
                        model_name,          # The network service name
                        application_name,    # The application name
                        vnfd,                # The vnf descriptor
                        charm_path,          # Path to charm
                        params,              # Runtime params, like mgmt ip
                        {},                  # for native charms only
                        self.n2vc_callback,  # Callback for status changes
                        db_nsr,              # Callback parameter
                        db_nslcmop,
                        None,                # Callback parameter (task)
                    )
                )
                task.add_done_callback(functools.partial(self.n2vc_callback, model_name, application_name, None, None,
                                                         db_nsr, db_nslcmop))
                self.lcm_tasks.register("ns", nsr_id, nslcmop_id, "create_charm:" + application_name, task)

            step = "Looking for needed vnfd to configure"
            self.logger.debug(logging_text + step)

            for c_vnf in nsd["constituent-vnfd"]:
                vnfd_id = c_vnf["vnfd-id-ref"]
                vnf_index = str(c_vnf["member-vnf-index"])
                vnfd = needed_vnfd[vnfd_id]

                # Check if this VNF has a charm configuration
                vnf_config = vnfd.get("vnf-configuration")

                if vnf_config and vnf_config.get("juju"):
                    proxy_charm = vnf_config["juju"]["charm"]
                    config_primitive = None

                    if proxy_charm:
                        if 'initial-config-primitive' in vnf_config:
                            config_primitive = vnf_config['initial-config-primitive']

                        # Login to the VCA. If there are multiple calls to login(),
                        # subsequent calls will be a nop and return immediately.
                        step = "connecting to N2VC to configure vnf {}".format(vnf_index)
                        await self.n2vc.login()
                        deploy(vnf_index, None, db_vnfrs[vnf_index]["ip-address"], config_primitive)
                        number_to_configure += 1

                # Deploy charms for each VDU that supports one.
                vdu_index = 0
                for vdu in vnfd['vdu']:
                    vdu_config = vdu.get('vdu-configuration')
                    proxy_charm = None
                    config_primitive = None

                    if vdu_config and vdu_config.get("juju"):
                        proxy_charm = vdu_config["juju"]["charm"]

                        if 'initial-config-primitive' in vdu_config:
                            config_primitive = vdu_config['initial-config-primitive']

                        if proxy_charm:
                            step = "connecting to N2VC to configure vdu {} from vnf {}".format(vdu["id"], vnf_index)
                            await self.n2vc.login()
                            deploy(vnf_index, vdu["id"], db_vnfrs[vnf_index]["vdur"][vdu_index]["ip-address"],
                                   config_primitive)
                            number_to_configure += 1
                    vdu_index += 1

            if number_to_configure:
                db_nsr_update["config-status"] = "configuring"
                db_nsr_update["operational-status"] = "running"
                db_nsr_update["detailed-status"] = "configuring: init: {}".format(number_to_configure)
                db_nslcmop_update["detailed-status"] = "configuring: init: {}".format(number_to_configure)
            else:
                db_nslcmop_update["operationState"] = "COMPLETED"
                db_nslcmop_update["statusEnteredTime"] = time()
                db_nslcmop_update["detailed-status"] = "done"
                db_nsr_update["config-status"] = "configured"
                db_nsr_update["detailed-status"] = "done"
                db_nsr_update["operational-status"] = "running"
            step = "Sending monitoring parameters to PM"
            # for c_vnf in nsd["constituent-vnfd"]:
            #     await self.create_monitoring(nsr_id, c_vnf["member-vnf-index"], needed_vnfd[c_vnf["vnfd-id-ref"]])
            try:
                await self.msg.aiowrite("ns", "instantiated", {"nsr_id": nsr_id, "nslcmop_id": nslcmop_id})
            except Exception as e:
                self.logger.error(logging_text + "kafka_write notification Exception {}".format(e))

            self.logger.debug(logging_text + "Exit")
            return

        except (ROclient.ROClientException, DbException, LcmException) as e:
            self.logger.error(logging_text + "Exit Exception while '{}': {}".format(step, e))
            exc = e
        except asyncio.CancelledError:
            self.logger.error(logging_text + "Cancelled Exception while '{}'".format(step))
            exc = "Operation was cancelled"
        except Exception as e:
            exc = traceback.format_exc()
            self.logger.critical(logging_text + "Exit Exception {} while '{}': {}".format(type(e).__name__, step, e),
                                 exc_info=True)
        finally:
            if exc:
                if db_nsr:
                    db_nsr_update["detailed-status"] = "ERROR {}: {}".format(step, exc)
                    db_nsr_update["operational-status"] = "failed"
                if db_nslcmop:
                    db_nslcmop_update["detailed-status"] = "FAILED {}: {}".format(step, exc)
                    db_nslcmop_update["operationState"] = "FAILED"
                    db_nslcmop_update["statusEnteredTime"] = time()
            if db_nsr_update:
                self.update_db_2("nsrs", nsr_id, db_nsr_update)
            if db_nslcmop_update:
                self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)
            self.lcm_tasks.remove("ns", nsr_id, nslcmop_id, "ns_instantiate")

    async def ns_terminate(self, nsr_id, nslcmop_id):
        logging_text = "Task ns={} terminate={} ".format(nsr_id, nslcmop_id)
        self.logger.debug(logging_text + "Enter")
        db_nsr = None
        db_nslcmop = None
        exc = None
        failed_detail = []   # annotates all failed error messages
        vca_task_list = []
        vca_task_dict = {}
        db_nsr_update = {}
        db_nslcmop_update = {}
        try:
            step = "Getting nslcmop={} from db".format(nslcmop_id)
            db_nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_id})
            step = "Getting nsr={} from db".format(nsr_id)
            db_nsr = self.db.get_one("nsrs", {"_id": nsr_id})
            # nsd = db_nsr["nsd"]
            nsr_lcm = deepcopy(db_nsr["_admin"].get("deployed"))
            if db_nsr["_admin"]["nsState"] == "NOT_INSTANTIATED":
                return
            # TODO ALF remove
            # db_vim = self.db.get_one("vim_accounts", {"_id":  db_nsr["datacenter"]})
            # #TODO check if VIM is creating and wait
            # RO_vim_id = db_vim["_admin"]["deployed"]["RO"]

            db_nsr_update["operational-status"] = "terminating"
            db_nsr_update["config-status"] = "terminating"

            if nsr_lcm and nsr_lcm.get("VCA"):
                try:
                    step = "Scheduling configuration charms removing"
                    db_nsr_update["detailed-status"] = "Deleting charms"
                    self.logger.debug(logging_text + step)
                    self.update_db_2("nsrs", nsr_id, db_nsr_update)
                    for application_name, deploy_info in nsr_lcm["VCA"].items():
                        if deploy_info:  # TODO it would be desirable having a and deploy_info.get("deployed"):
                            task = asyncio.ensure_future(
                                self.n2vc.RemoveCharms(
                                    deploy_info['model'],
                                    application_name,
                                    # self.n2vc_callback,
                                    # db_nsr,
                                    # db_nslcmop,
                                )
                            )
                            vca_task_list.append(task)
                            vca_task_dict[application_name] = task
                            # task.add_done_callback(functools.partial(self.n2vc_callback, deploy_info['model'],
                            #                                          deploy_info['application'], None, db_nsr,
                            #                                          db_nslcmop, vnf_index))
                            self.lcm_ns_tasks[nsr_id][nslcmop_id]["delete_charm:" + application_name] = task
                except Exception as e:
                    self.logger.debug(logging_text + "Failed while deleting charms: {}".format(e))

            # remove from RO
            RO_fail = False
            RO = ROclient.ROClient(self.loop, **self.ro_config)

            # Delete ns
            RO_nsr_id = RO_delete_action = None
            if nsr_lcm and nsr_lcm.get("RO"):
                RO_nsr_id = nsr_lcm["RO"].get("nsr_id")
                RO_delete_action = nsr_lcm["RO"].get("nsr_delete_action_id")
            try:
                if RO_nsr_id:
                    step = db_nsr_update["detailed-status"] = db_nslcmop_update["detailed-status"] = "Deleting ns at RO"
                    self.logger.debug(logging_text + step)
                    desc = await RO.delete("ns", RO_nsr_id)
                    RO_delete_action = desc["action_id"]
                    db_nsr_update["_admin.deployed.RO.nsr_delete_action_id"] = RO_delete_action
                    db_nsr_update["_admin.deployed.RO.nsr_id"] = None
                    db_nsr_update["_admin.deployed.RO.nsr_status"] = "DELETED"
                if RO_delete_action:
                    # wait until NS is deleted from VIM
                    step = detailed_status = "Waiting ns deleted from VIM. RO_id={}".format(RO_nsr_id)
                    detailed_status_old = None
                    self.logger.debug(logging_text + step)

                    delete_timeout = 20 * 60   # 20 minutes
                    while delete_timeout > 0:
                        desc = await RO.show("ns", item_id_name=RO_nsr_id, extra_item="action",
                                             extra_item_id=RO_delete_action)
                        ns_status, ns_status_info = RO.check_action_status(desc)
                        if ns_status == "ERROR":
                            raise ROclient.ROClientException(ns_status_info)
                        elif ns_status == "BUILD":
                            detailed_status = step + "; {}".format(ns_status_info)
                        elif ns_status == "ACTIVE":
                            break
                        else:
                            assert False, "ROclient.check_action_status returns unknown {}".format(ns_status)
                        await asyncio.sleep(5, loop=self.loop)
                        delete_timeout -= 5
                        if detailed_status != detailed_status_old:
                            detailed_status_old = db_nslcmop_update["detailed-status"] = detailed_status
                            self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)
                    else:  # delete_timeout <= 0:
                        raise ROclient.ROClientException("Timeout waiting ns deleted from VIM")

            except ROclient.ROClientException as e:
                if e.http_code == 404:  # not found
                    db_nsr_update["_admin.deployed.RO.nsr_id"] = None
                    db_nsr_update["_admin.deployed.RO.nsr_status"] = "DELETED"
                    self.logger.debug(logging_text + "RO_ns_id={} already deleted".format(RO_nsr_id))
                elif e.http_code == 409:   # conflict
                    failed_detail.append("RO_ns_id={} delete conflict: {}".format(RO_nsr_id, e))
                    self.logger.debug(logging_text + failed_detail[-1])
                    RO_fail = True
                else:
                    failed_detail.append("RO_ns_id={} delete error: {}".format(RO_nsr_id, e))
                    self.logger.error(logging_text + failed_detail[-1])
                    RO_fail = True

            # Delete nsd
            if not RO_fail and nsr_lcm and nsr_lcm.get("RO") and nsr_lcm["RO"].get("nsd_id"):
                RO_nsd_id = nsr_lcm["RO"]["nsd_id"]
                try:
                    step = db_nsr_update["detailed-status"] = db_nslcmop_update["detailed-status"] =\
                        "Deleting nsd at RO"
                    await RO.delete("nsd", RO_nsd_id)
                    self.logger.debug(logging_text + "RO_nsd_id={} deleted".format(RO_nsd_id))
                    db_nsr_update["_admin.deployed.RO.nsd_id"] = None
                except ROclient.ROClientException as e:
                    if e.http_code == 404:  # not found
                        db_nsr_update["_admin.deployed.RO.nsd_id"] = None
                        self.logger.debug(logging_text + "RO_nsd_id={} already deleted".format(RO_nsd_id))
                    elif e.http_code == 409:   # conflict
                        failed_detail.append("RO_nsd_id={} delete conflict: {}".format(RO_nsd_id, e))
                        self.logger.debug(logging_text + failed_detail[-1])
                        RO_fail = True
                    else:
                        failed_detail.append("RO_nsd_id={} delete error: {}".format(RO_nsd_id, e))
                        self.logger.error(logging_text + failed_detail[-1])
                        RO_fail = True

            if not RO_fail and nsr_lcm and nsr_lcm.get("RO") and nsr_lcm["RO"].get("vnfd_id"):
                for vnf_id, RO_vnfd_id in nsr_lcm["RO"]["vnfd_id"].items():
                    if not RO_vnfd_id:
                        continue
                    try:
                        step = db_nsr_update["detailed-status"] = db_nslcmop_update["detailed-status"] =\
                            "Deleting vnfd={} at RO".format(vnf_id)
                        await RO.delete("vnfd", RO_vnfd_id)
                        self.logger.debug(logging_text + "RO_vnfd_id={} deleted".format(RO_vnfd_id))
                        db_nsr_update["_admin.deployed.RO.vnfd_id.{}".format(vnf_id)] = None
                    except ROclient.ROClientException as e:
                        if e.http_code == 404:  # not found
                            db_nsr_update["_admin.deployed.RO.vnfd_id.{}".format(vnf_id)] = None
                            self.logger.debug(logging_text + "RO_vnfd_id={} already deleted ".format(RO_vnfd_id))
                        elif e.http_code == 409:   # conflict
                            failed_detail.append("RO_vnfd_id={} delete conflict: {}".format(RO_vnfd_id, e))
                            self.logger.debug(logging_text + failed_detail[-1])
                        else:
                            failed_detail.append("RO_vnfd_id={} delete error: {}".format(RO_vnfd_id, e))
                            self.logger.error(logging_text + failed_detail[-1])

            if vca_task_list:
                db_nsr_update["detailed-status"] = db_nslcmop_update["detailed-status"] =\
                    "Waiting for deletion of configuration charms"
                self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)
                self.update_db_2("nsrs", nsr_id, db_nsr_update)
                await asyncio.wait(vca_task_list, timeout=300)
            for application_name, task in vca_task_dict.items():
                if task.cancelled():
                    failed_detail.append("VCA[{}] Deletion has been cancelled".format(application_name))
                elif task.done():
                    exc = task.exception()
                    if exc:
                        failed_detail.append("VCA[{}] Deletion exception: {}".format(application_name, exc))
                    else:
                        db_nsr_update["_admin.deployed.VCA.{}".format(application_name)] = None
                else:  # timeout
                    # TODO Should it be cancelled?!!
                    task.cancel()
                    failed_detail.append("VCA[{}] Deletion timeout".format(application_name))

            if failed_detail:
                self.logger.error(logging_text + " ;".join(failed_detail))
                db_nsr_update["operational-status"] = "failed"
                db_nsr_update["detailed-status"] = "Deletion errors " + "; ".join(failed_detail)
                db_nslcmop_update["detailed-status"] = "; ".join(failed_detail)
                db_nslcmop_update["operationState"] = "FAILED"
                db_nslcmop_update["statusEnteredTime"] = time()
            elif db_nslcmop["operationParams"].get("autoremove"):
                self.db.del_one("nsrs", {"_id": nsr_id})
                db_nsr_update.clear()
                self.db.del_list("nslcmops", {"nsInstanceId": nsr_id})
                db_nslcmop_update.clear()
                self.db.del_list("vnfrs", {"nsr-id-ref": nsr_id})
                self.logger.debug(logging_text + "Delete from database")
            else:
                db_nsr_update["operational-status"] = "terminated"
                db_nsr_update["detailed-status"] = "Done"
                db_nsr_update["_admin.nsState"] = "NOT_INSTANTIATED"
                db_nslcmop_update["detailed-status"] = "Done"
                db_nslcmop_update["operationState"] = "COMPLETED"
                db_nslcmop_update["statusEnteredTime"] = time()
            try:
                await self.msg.aiowrite("ns", "terminated", {"nsr_id": nsr_id, "nslcmop_id": nslcmop_id})
            except Exception as e:
                self.logger.error(logging_text + "kafka_write notification Exception {}".format(e))
            self.logger.debug(logging_text + "Exit")

        except (ROclient.ROClientException, DbException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except asyncio.CancelledError:
            self.logger.error(logging_text + "Cancelled Exception while '{}'".format(step))
            exc = "Operation was cancelled"
        except Exception as e:
            exc = traceback.format_exc()
            self.logger.critical(logging_text + "Exit Exception {}".format(e), exc_info=True)
        finally:
            if exc and db_nslcmop:
                db_nslcmop_update = {
                    "detailed-status": "FAILED {}: {}".format(step, exc),
                    "operationState": "FAILED",
                    "statusEnteredTime": time(),
                }
            if db_nslcmop_update:
                self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)
            if db_nsr_update:
                self.update_db_2("nsrs", nsr_id, db_nsr_update)
            self.lcm_tasks.remove("ns", nsr_id, nslcmop_id, "ns_terminate")

    async def _ns_execute_primitive(self, db_deployed, nsr_name, member_vnf_index, vdu_id, primitive, primitive_params):

        vdu_id_text = "vnfd"
        if vdu_id:
            vdu_id_text = vdu_id
        application_name = self.n2vc.FormatApplicationName(
            nsr_name,
            member_vnf_index,
            vdu_id_text
        )
        vca_deployed = db_deployed["VCA"].get(application_name)
        if not vca_deployed:
            raise LcmException("charm for member_vnf_index={} vdu_id={} is not deployed".format(member_vnf_index,
                                                                                                vdu_id))
        model_name = vca_deployed.get("model")
        application_name = vca_deployed.get("application")
        if not model_name or not application_name:
            raise LcmException("charm for member_vnf_index={} is not properly deployed".format(member_vnf_index))
        if vca_deployed["operational-status"] != "active":
            raise LcmException("charm for member_vnf_index={} operational_status={} not 'active'".format(
                member_vnf_index, vca_deployed["operational-status"]))
        callback = None  # self.n2vc_callback
        callback_args = ()  # [db_nsr, db_nslcmop, member_vnf_index, None]
        await self.n2vc.login()
        task = asyncio.ensure_future(
            self.n2vc.ExecutePrimitive(
                model_name,
                application_name,
                primitive, callback,
                *callback_args,
                **primitive_params
            )
        )
        # task.add_done_callback(functools.partial(self.n2vc_callback, model_name, application_name, None,
        #                                          db_nsr, db_nslcmop, member_vnf_index))
        # self.lcm_ns_tasks[nsr_id][nslcmop_id]["action: " + primitive] = task
        # wait until completed with timeout
        await asyncio.wait((task,), timeout=600)

        result = "FAILED"  # by default
        result_detail = ""
        if task.cancelled():
            result_detail = "Task has been cancelled"
        elif task.done():
            exc = task.exception()
            if exc:
                result_detail = str(exc)
            else:
                # TODO revise with Adam if action is finished and ok when task is done or callback is needed
                result = "COMPLETED"
                result_detail = "Done"
        else:  # timeout
            # TODO Should it be cancelled?!!
            task.cancel()
            result_detail = "timeout"
        return result, result_detail

    async def ns_action(self, nsr_id, nslcmop_id):
        logging_text = "Task ns={} action={} ".format(nsr_id, nslcmop_id)
        self.logger.debug(logging_text + "Enter")
        # get all needed from database
        db_nsr = None
        db_nslcmop = None
        db_nslcmop_update = None
        exc = None
        try:
            step = "Getting information from database"
            db_nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_id})
            db_nsr = self.db.get_one("nsrs", {"_id": nsr_id})
            nsr_lcm = db_nsr["_admin"].get("deployed")
            nsr_name = db_nsr["name"]
            vnf_index = db_nslcmop["operationParams"]["member_vnf_index"]
            vdu_id = db_nslcmop["operationParams"].get("vdu_id")

            # TODO check if ns is in a proper status
            primitive = db_nslcmop["operationParams"]["primitive"]
            primitive_params = db_nslcmop["operationParams"]["primitive_params"]
            result, result_detail = await self._ns_execute_primitive(nsr_lcm, nsr_name, vnf_index, vdu_id, primitive,
                                                                     primitive_params)
            db_nslcmop_update = {
                "detailed-status": result_detail,
                "operationState": result,
                "statusEnteredTime": time()
            }
            self.logger.debug(logging_text + " task Done with result {} {}".format(result, result_detail))
            return  # database update is called inside finally

        except (DbException, LcmException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except asyncio.CancelledError:
            self.logger.error(logging_text + "Cancelled Exception while '{}'".format(step))
            exc = "Operation was cancelled"
        except Exception as e:
            exc = traceback.format_exc()
            self.logger.critical(logging_text + "Exit Exception {} {}".format(type(e).__name__, e), exc_info=True)
        finally:
            if exc and db_nslcmop:
                db_nslcmop_update = {
                    "detailed-status": "FAILED {}: {}".format(step, exc),
                    "operationState": "FAILED",
                    "statusEnteredTime": time(),
                }
            if db_nslcmop_update:
                self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)
            self.lcm_tasks.remove("ns", nsr_id, nslcmop_id, "ns_action")

    async def ns_scale(self, nsr_id, nslcmop_id):
        logging_text = "Task ns={} scale={} ".format(nsr_id, nslcmop_id)
        self.logger.debug(logging_text + "Enter")
        # get all needed from database
        db_nsr = None
        db_nslcmop = None
        db_nslcmop_update = {}
        db_nsr_update = {}
        exc = None
        try:
            step = "Getting nslcmop from database"
            db_nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_id})
            step = "Getting nsr from database"
            db_nsr = self.db.get_one("nsrs", {"_id": nsr_id})
            step = "Parsing scaling parameters"
            db_nsr_update["operational-status"] = "scaling"
            self.update_db_2("nsrs", nsr_id, db_nsr_update)
            nsr_lcm = db_nsr["_admin"].get("deployed")
            RO_nsr_id = nsr_lcm["RO"]["nsr_id"]
            vnf_index = db_nslcmop["operationParams"]["scaleVnfData"]["scaleByStepData"]["member-vnf-index"]
            scaling_group = db_nslcmop["operationParams"]["scaleVnfData"]["scaleByStepData"]["scaling-group-descriptor"]
            scaling_type = db_nslcmop["operationParams"]["scaleVnfData"]["scaleVnfType"]
            # scaling_policy = db_nslcmop["operationParams"]["scaleVnfData"]["scaleByStepData"].get("scaling-policy")

            step = "Getting vnfr from database"
            db_vnfr = self.db.get_one("vnfrs", {"member-vnf-index-ref": vnf_index, "nsr-id-ref": nsr_id})
            step = "Getting vnfd from database"
            db_vnfd = self.db.get_one("vnfds", {"_id": db_vnfr["vnfd-id"]})
            step = "Getting scaling-group-descriptor"
            for scaling_descriptor in db_vnfd["scaling-group-descriptor"]:
                if scaling_descriptor["name"] == scaling_group:
                    break
            else:
                raise LcmException("input parameter 'scaleByStepData':'scaling-group-descriptor':'{}' is not present "
                                   "at vnfd:scaling-group-descriptor".format(scaling_group))
            # cooldown_time = 0
            # for scaling_policy_descriptor in scaling_descriptor.get("scaling-policy", ()):
            #     cooldown_time = scaling_policy_descriptor.get("cooldown-time", 0)
            #     if scaling_policy and scaling_policy == scaling_policy_descriptor.get("name"):
            #         break

            # TODO check if ns is in a proper status
            step = "Sending scale order to RO"
            nb_scale_op = 0
            if not db_nsr["_admin"].get("scaling-group"):
                self.update_db_2("nsrs", nsr_id, {"_admin.scaling-group": [{"name": scaling_group, "nb-scale-op": 0}]})
                admin_scale_index = 0
            else:
                for admin_scale_index, admin_scale_info in enumerate(db_nsr["_admin"]["scaling-group"]):
                    if admin_scale_info["name"] == scaling_group:
                        nb_scale_op = admin_scale_info.get("nb-scale-op", 0)
                        break
            RO_scaling_info = []
            vdu_scaling_info = {"scaling_group_name": scaling_group, "vdu": []}
            if scaling_type == "SCALE_OUT":
                # count if max-instance-count is reached
                if "max-instance-count" in scaling_descriptor and scaling_descriptor["max-instance-count"] is not None:
                    max_instance_count = int(scaling_descriptor["max-instance-count"])
                    if nb_scale_op >= max_instance_count:
                        raise LcmException("reached the limit of {} (max-instance-count) scaling-out operations for the"
                                           " scaling-group-descriptor '{}'".format(nb_scale_op, scaling_group))
                nb_scale_op = nb_scale_op + 1
                vdu_scaling_info["scaling_direction"] = "OUT"
                vdu_scaling_info["vdu-create"] = {}
                for vdu_scale_info in scaling_descriptor["vdu"]:
                    RO_scaling_info.append({"osm_vdu_id": vdu_scale_info["vdu-id-ref"], "member-vnf-index": vnf_index,
                                            "type": "create", "count": vdu_scale_info.get("count", 1)})
                    vdu_scaling_info["vdu-create"][vdu_scale_info["vdu-id-ref"]] = vdu_scale_info.get("count", 1)
            elif scaling_type == "SCALE_IN":
                # count if min-instance-count is reached
                if "min-instance-count" in scaling_descriptor and scaling_descriptor["min-instance-count"] is not None:
                    min_instance_count = int(scaling_descriptor["min-instance-count"])
                    if nb_scale_op <= min_instance_count:
                        raise LcmException("reached the limit of {} (min-instance-count) scaling-in operations for the "
                                           "scaling-group-descriptor '{}'".format(nb_scale_op, scaling_group))
                nb_scale_op = nb_scale_op - 1
                vdu_scaling_info["scaling_direction"] = "IN"
                vdu_scaling_info["vdu-delete"] = {}
                for vdu_scale_info in scaling_descriptor["vdu"]:
                    RO_scaling_info.append({"osm_vdu_id": vdu_scale_info["vdu-id-ref"], "member-vnf-index": vnf_index,
                                            "type": "delete", "count": vdu_scale_info.get("count", 1)})
                    vdu_scaling_info["vdu-delete"][vdu_scale_info["vdu-id-ref"]] = vdu_scale_info.get("count", 1)

            # update VDU_SCALING_INFO with the VDUs to delete ip_addresses
            if vdu_scaling_info["scaling_direction"] == "IN":
                for vdur in reversed(db_vnfr["vdur"]):
                    if vdu_scaling_info["vdu-delete"].get(vdur["vdu-id-ref"]):
                        vdu_scaling_info["vdu-delete"][vdur["vdu-id-ref"]] -= 1
                        vdu_scaling_info["vdu"].append({
                            "name": vdur["name"],
                            "vdu_id": vdur["vdu-id-ref"],
                            "interface": []
                        })
                        for interface in vdur["interfaces"]:
                            vdu_scaling_info["vdu"][-1]["interface"].append({
                                "name": interface["name"],
                                "ip_address": interface["ip-address"],
                                "mac_address": interface.get("mac-address"),
                            })
                del vdu_scaling_info["vdu-delete"]

            # execute primitive service PRE-SCALING
            step = "Executing pre-scale vnf-config-primitive"
            if scaling_descriptor.get("scaling-config-action"):
                for scaling_config_action in scaling_descriptor["scaling-config-action"]:
                    if scaling_config_action.get("trigger") and scaling_config_action["trigger"] == "pre-scale-in" \
                            and scaling_type == "SCALE_IN":
                        vnf_config_primitive = scaling_config_action["vnf-config-primitive-name-ref"]
                        step = db_nslcmop_update["detailed-status"] = \
                            "executing pre-scale scaling-config-action '{}'".format(vnf_config_primitive)
                        # look for primitive
                        primitive_params = {}
                        for config_primitive in db_vnfd.get("vnf-configuration", {}).get("config-primitive", ()):
                            if config_primitive["name"] == vnf_config_primitive:
                                for parameter in config_primitive.get("parameter", ()):
                                    if 'default-value' in parameter and \
                                            parameter['default-value'] == "<VDU_SCALE_INFO>":
                                        primitive_params[parameter["name"]] = yaml.safe_dump(vdu_scaling_info,
                                                                                             default_flow_style=True,
                                                                                             width=256)
                                break
                        else:
                            raise LcmException(
                                "Invalid vnfd descriptor at scaling-group-descriptor[name='{}']:scaling-config-action"
                                "[vnf-config-primitive-name-ref='{}'] does not match any vnf-cnfiguration:config-"
                                "primitive".format(scaling_group, config_primitive))
                        result, result_detail = await self._ns_execute_primitive(nsr_lcm, vnf_index,
                                                                                 vnf_config_primitive, primitive_params)
                        self.logger.debug(logging_text + "vnf_config_primitive={} Done with result {} {}".format(
                            vnf_config_primitive, result, result_detail))
                        if result == "FAILED":
                            raise LcmException(result_detail)

            if RO_scaling_info:
                RO = ROclient.ROClient(self.loop, **self.ro_config)
                RO_desc = await RO.create_action("ns", RO_nsr_id, {"vdu-scaling": RO_scaling_info})
                db_nsr_update["_admin.scaling-group.{}.nb-scale-op".format(admin_scale_index)] = nb_scale_op
                db_nsr_update["_admin.scaling-group.{}.time".format(admin_scale_index)] = time()
                # TODO mark db_nsr_update as scaling
                # wait until ready
                RO_nslcmop_id = RO_desc["instance_action_id"]
                db_nslcmop_update["_admin.deploy.RO"] = RO_nslcmop_id

                RO_task_done = False
                step = detailed_status = "Waiting RO_task_id={} to complete the scale action.".format(RO_nslcmop_id)
                detailed_status_old = None
                self.logger.debug(logging_text + step)

                deployment_timeout = 1 * 3600   # One hours
                while deployment_timeout > 0:
                    if not RO_task_done:
                        desc = await RO.show("ns", item_id_name=RO_nsr_id, extra_item="action",
                                             extra_item_id=RO_nslcmop_id)
                        ns_status, ns_status_info = RO.check_action_status(desc)
                        if ns_status == "ERROR":
                            raise ROclient.ROClientException(ns_status_info)
                        elif ns_status == "BUILD":
                            detailed_status = step + "; {}".format(ns_status_info)
                        elif ns_status == "ACTIVE":
                            RO_task_done = True
                            step = detailed_status = "Waiting ns ready at RO. RO_id={}".format(RO_nsr_id)
                            self.logger.debug(logging_text + step)
                        else:
                            assert False, "ROclient.check_action_status returns unknown {}".format(ns_status)
                    else:
                        desc = await RO.show("ns", RO_nsr_id)
                        ns_status, ns_status_info = RO.check_ns_status(desc)
                        if ns_status == "ERROR":
                            raise ROclient.ROClientException(ns_status_info)
                        elif ns_status == "BUILD":
                            detailed_status = step + "; {}".format(ns_status_info)
                        elif ns_status == "ACTIVE":
                            step = detailed_status = "Waiting for management IP address reported by the VIM"
                            try:
                                desc = await RO.show("ns", RO_nsr_id)
                                nsr_lcm["nsr_ip"] = RO.get_ns_vnf_info(desc)
                                break
                            except ROclient.ROClientException as e:
                                if e.http_code != 409:  # IP address is not ready return code is 409 CONFLICT
                                    raise e
                        else:
                            assert False, "ROclient.check_ns_status returns unknown {}".format(ns_status)
                    if detailed_status != detailed_status_old:
                        detailed_status_old = db_nslcmop_update["detailed-status"] = detailed_status
                        self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)

                    await asyncio.sleep(5, loop=self.loop)
                    deployment_timeout -= 5
                if deployment_timeout <= 0:
                    raise ROclient.ROClientException("Timeout waiting ns to be ready")

                step = "Updating VNFRs"
                self.ns_update_vnfr({db_vnfr["member-vnf-index-ref"]: db_vnfr}, desc)

                # update VDU_SCALING_INFO with the obtained ip_addresses
                if vdu_scaling_info["scaling_direction"] == "OUT":
                    for vdur in reversed(db_vnfr["vdur"]):
                        if vdu_scaling_info["vdu-create"].get(vdur["vdu-id-ref"]):
                            vdu_scaling_info["vdu-create"][vdur["vdu-id-ref"]] -= 1
                            vdu_scaling_info["vdu"].append({
                                "name": vdur["name"],
                                "vdu_id": vdur["vdu-id-ref"],
                                "interface": []
                            })
                            for interface in vdur["interfaces"]:
                                vdu_scaling_info["vdu"][-1]["interface"].append({
                                    "name": interface["name"],
                                    "ip_address": interface["ip-address"],
                                    "mac_address": interface.get("mac-address"),
                                })
                    del vdu_scaling_info["vdu-create"]

            if db_nsr_update:
                self.update_db_2("nsrs", nsr_id, db_nsr_update)

            # execute primitive service POST-SCALING
            step = "Executing post-scale vnf-config-primitive"
            if scaling_descriptor.get("scaling-config-action"):
                for scaling_config_action in scaling_descriptor["scaling-config-action"]:
                    if scaling_config_action.get("trigger") and scaling_config_action["trigger"] == "post-scale-out" \
                            and scaling_type == "SCALE_OUT":
                        vnf_config_primitive = scaling_config_action["vnf-config-primitive-name-ref"]
                        step = db_nslcmop_update["detailed-status"] = \
                            "executing post-scale scaling-config-action '{}'".format(vnf_config_primitive)
                        # look for primitive
                        primitive_params = {}
                        for config_primitive in db_vnfd.get("vnf-configuration", {}).get("config-primitive", ()):
                            if config_primitive["name"] == vnf_config_primitive:
                                for parameter in config_primitive.get("parameter", ()):
                                    if 'default-value' in parameter and \
                                            parameter['default-value'] == "<VDU_SCALE_INFO>":
                                        primitive_params[parameter["name"]] = yaml.safe_dump(vdu_scaling_info,
                                                                                             default_flow_style=True,
                                                                                             width=256)
                                break
                        else:
                            raise LcmException("Invalid vnfd descriptor at scaling-group-descriptor[name='{}']:"
                                               "scaling-config-action[vnf-config-primitive-name-ref='{}'] does not "
                                               "match any vnf-cnfiguration:config-primitive".format(scaling_group,
                                                                                                    config_primitive))
                        result, result_detail = await self._ns_execute_primitive(nsr_lcm, vnf_index,
                                                                                 vnf_config_primitive, primitive_params)
                        self.logger.debug(logging_text + "vnf_config_primitive={} Done with result {} {}".format(
                            vnf_config_primitive, result, result_detail))
                        if result == "FAILED":
                            raise LcmException(result_detail)

            db_nslcmop_update["operationState"] = "COMPLETED"
            db_nslcmop_update["statusEnteredTime"] = time()
            db_nslcmop_update["detailed-status"] = "done"
            db_nsr_update["detailed-status"] = "done"
            db_nsr_update["operational-status"] = "running"
            try:
                await self.msg.aiowrite("ns", "scaled", {"nsr_id": nsr_id, "nslcmop_id": nslcmop_id})
                # if cooldown_time:
                #     await asyncio.sleep(cooldown_time)
                # await self.msg.aiowrite("ns", "scaled-cooldown-time", {"nsr_id": nsr_id, "nslcmop_id": nslcmop_id})
            except Exception as e:
                self.logger.error(logging_text + "kafka_write notification Exception {}".format(e))
            self.logger.debug(logging_text + "Exit Ok")
            return
        except (ROclient.ROClientException, DbException, LcmException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except asyncio.CancelledError:
            self.logger.error(logging_text + "Cancelled Exception while '{}'".format(step))
            exc = "Operation was cancelled"
        except Exception as e:
            exc = traceback.format_exc()
            self.logger.critical(logging_text + "Exit Exception {} {}".format(type(e).__name__, e), exc_info=True)
        finally:
            if exc:
                if db_nslcmop:
                    db_nslcmop_update = {
                        "detailed-status": "FAILED {}: {}".format(step, exc),
                        "operationState": "FAILED",
                        "statusEnteredTime": time(),
                    }
                if db_nsr:
                    db_nsr_update["operational-status"] = "FAILED {}: {}".format(step, exc),
                    db_nsr_update["detailed-status"] = "failed"
            if db_nslcmop_update:
                self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)
            if db_nsr_update:
                self.update_db_2("nsrs", nsr_id, db_nsr_update)
            self.lcm_tasks.remove("ns", nsr_id, nslcmop_id, "ns_scale")

    async def test(self, param=None):
        self.logger.debug("Starting/Ending test task: {}".format(param))

    async def kafka_ping(self):
        self.logger.debug("Task kafka_ping Enter")
        consecutive_errors = 0
        first_start = True
        kafka_has_received = False
        self.pings_not_received = 1
        while True:
            try:
                await self.msg.aiowrite("admin", "ping", {"from": "lcm", "to": "lcm"}, self.loop)
                # time between pings are low when it is not received and at starting
                wait_time = 5 if not kafka_has_received else 120
                if not self.pings_not_received:
                    kafka_has_received = True
                self.pings_not_received += 1
                await asyncio.sleep(wait_time, loop=self.loop)
                if self.pings_not_received > 10:
                    raise LcmException("It is not receiving pings from Kafka bus")
                consecutive_errors = 0
                first_start = False
            except LcmException:
                raise
            except Exception as e:
                # if not first_start is the first time after starting. So leave more time and wait
                # to allow kafka starts
                if consecutive_errors == 8 if not first_start else 30:
                    self.logger.error("Task kafka_read task exit error too many errors. Exception: {}".format(e))
                    raise
                consecutive_errors += 1
                self.logger.error("Task kafka_read retrying after Exception {}".format(e))
                wait_time = 1 if not first_start else 5
                await asyncio.sleep(wait_time, loop=self.loop)

    async def kafka_read(self):
        self.logger.debug("Task kafka_read Enter")
        order_id = 1
        # future = asyncio.Future()
        consecutive_errors = 0
        first_start = True
        while consecutive_errors < 10:
            try:
                topics = ("admin", "ns", "vim_account", "sdn")
                topic, command, params = await self.msg.aioread(topics, self.loop)
                if topic != "admin" and command != "ping":
                    self.logger.debug("Task kafka_read receives {} {}: {}".format(topic, command, params))
                consecutive_errors = 0
                first_start = False
                order_id += 1
                if command == "exit":
                    print("Bye!")
                    break
                elif command.startswith("#"):
                    continue
                elif command == "echo":
                    # just for test
                    print(params)
                    sys.stdout.flush()
                    continue
                elif command == "test":
                    asyncio.Task(self.test(params), loop=self.loop)
                    continue

                if topic == "admin":
                    if command == "ping" and params["to"] == "lcm" and params["from"] == "lcm":
                        self.pings_not_received = 0
                    continue
                elif topic == "ns":
                    if command == "instantiate":
                        # self.logger.debug("Deploying NS {}".format(nsr_id))
                        nslcmop = params
                        nslcmop_id = nslcmop["_id"]
                        nsr_id = nslcmop["nsInstanceId"]
                        task = asyncio.ensure_future(self.ns_instantiate(nsr_id, nslcmop_id))
                        self.lcm_tasks.register("ns", nsr_id, nslcmop_id, "ns_instantiate", task)
                        continue
                    elif command == "terminate":
                        # self.logger.debug("Deleting NS {}".format(nsr_id))
                        nslcmop = params
                        nslcmop_id = nslcmop["_id"]
                        nsr_id = nslcmop["nsInstanceId"]
                        self.lcm_tasks.cancel(topic, nsr_id)
                        task = asyncio.ensure_future(self.ns_terminate(nsr_id, nslcmop_id))
                        self.lcm_tasks.register("ns", nsr_id, nslcmop_id, "ns_terminate", task)
                        continue
                    elif command == "action":
                        # self.logger.debug("Update NS {}".format(nsr_id))
                        nslcmop = params
                        nslcmop_id = nslcmop["_id"]
                        nsr_id = nslcmop["nsInstanceId"]
                        task = asyncio.ensure_future(self.ns_action(nsr_id, nslcmop_id))
                        self.lcm_tasks.register("ns", nsr_id, nslcmop_id, "ns_action", task)
                        continue
                    elif command == "scale":
                        # self.logger.debug("Update NS {}".format(nsr_id))
                        nslcmop = params
                        nslcmop_id = nslcmop["_id"]
                        nsr_id = nslcmop["nsInstanceId"]
                        task = asyncio.ensure_future(self.ns_scale(nsr_id, nslcmop_id))
                        self.lcm_tasks.register("ns", nsr_id, nslcmop_id, "ns_scale", task)
                        continue
                    elif command == "show":
                        try:
                            db_nsr = self.db.get_one("nsrs", {"_id": nsr_id})
                            print("nsr:\n    _id={}\n    operational-status: {}\n    config-status: {}"
                                  "\n    detailed-status: {}\n    deploy: {}\n    tasks: {}"
                                  "".format(nsr_id, db_nsr["operational-status"], db_nsr["config-status"],
                                            db_nsr["detailed-status"],
                                            db_nsr["_admin"]["deployed"], self.lcm_ns_tasks.get(nsr_id)))
                        except Exception as e:
                            print("nsr {} not found: {}".format(nsr_id, e))
                        sys.stdout.flush()
                        continue
                    elif command == "deleted":
                        continue  # TODO cleaning of task just in case should be done
                    elif command in ("terminated", "instantiated", "scaled", "actioned"):  # "scaled-cooldown-time"
                        continue
                elif topic == "vim_account":
                    vim_id = params["_id"]
                    if command == "create":
                        task = asyncio.ensure_future(self.vim_create(params, order_id))
                        self.lcm_tasks.register("vim_account", vim_id, order_id, "vim_create", task)
                        continue
                    elif command == "delete":
                        self.lcm_tasks.cancel(topic, vim_id)
                        task = asyncio.ensure_future(self.vim_delete(vim_id, order_id))
                        self.lcm_tasks.register("vim_account", vim_id, order_id, "vim_delete", task)
                        continue
                    elif command == "show":
                        print("not implemented show with vim_account")
                        sys.stdout.flush()
                        continue
                    elif command == "edit":
                        task = asyncio.ensure_future(self.vim_edit(params, order_id))
                        self.lcm_tasks.register("vim_account", vim_id, order_id, "vim_edit", task)
                        continue
                elif topic == "sdn":
                    _sdn_id = params["_id"]
                    if command == "create":
                        task = asyncio.ensure_future(self.sdn_create(params, order_id))
                        self.lcm_tasks.register("sdn", _sdn_id, order_id, "sdn_create", task)
                        continue
                    elif command == "delete":
                        self.lcm_tasks.cancel(topic, _sdn_id)
                        task = asyncio.ensure_future(self.sdn_delete(_sdn_id, order_id))
                        self.lcm_tasks.register("sdn", _sdn_id, order_id, "sdn_delete", task)
                        continue
                    elif command == "edit":
                        task = asyncio.ensure_future(self.sdn_edit(params, order_id))
                        self.lcm_tasks.register("sdn", _sdn_id, order_id, "sdn_edit", task)
                        continue
                self.logger.critical("unknown topic {} and command '{}'".format(topic, command))
            except Exception as e:
                # if not first_start is the first time after starting. So leave more time and wait
                # to allow kafka starts
                if consecutive_errors == 8 if not first_start else 30:
                    self.logger.error("Task kafka_read task exit error too many errors. Exception: {}".format(e))
                    raise
                consecutive_errors += 1
                self.logger.error("Task kafka_read retrying after Exception {}".format(e))
                wait_time = 2 if not first_start else 5
                await asyncio.sleep(wait_time, loop=self.loop)

        # self.logger.debug("Task kafka_read terminating")
        self.logger.debug("Task kafka_read exit")

    def start(self):
        self.loop = asyncio.get_event_loop()

        # check RO version
        self.loop.run_until_complete(self.check_RO_version())

        self.loop.run_until_complete(asyncio.gather(
            self.kafka_read(),
            self.kafka_ping()
        ))
        # TODO
        # self.logger.debug("Terminating cancelling creation tasks")
        # self.lcm_tasks.cancel("ALL", "create")
        # timeout = 200
        # while self.is_pending_tasks():
        #     self.logger.debug("Task kafka_read terminating. Waiting for tasks termination")
        #     await asyncio.sleep(2, loop=self.loop)
        #     timeout -= 2
        #     if not timeout:
        #         self.lcm_tasks.cancel("ALL", "ALL")
        self.loop.close()
        self.loop = None
        if self.db:
            self.db.db_disconnect()
        if self.msg:
            self.msg.disconnect()
        if self.fs:
            self.fs.fs_disconnect()

    def read_config_file(self, config_file):
        # TODO make a [ini] + yaml inside parser
        # the configparser library is not suitable, because it does not admit comments at the end of line,
        # and not parse integer or boolean
        try:
            with open(config_file) as f:
                conf = yaml.load(f)
            for k, v in environ.items():
                if not k.startswith("OSMLCM_"):
                    continue
                k_items = k.lower().split("_")
                c = conf
                try:
                    for k_item in k_items[1:-1]:
                        if k_item in ("ro", "vca"):
                            # put in capital letter
                            k_item = k_item.upper()
                        c = c[k_item]
                    if k_items[-1] == "port":
                        c[k_items[-1]] = int(v)
                    else:
                        c[k_items[-1]] = v
                except Exception as e:
                    self.logger.warn("skipping environ '{}' on exception '{}'".format(k, e))

            return conf
        except Exception as e:
            self.logger.critical("At config file '{}': {}".format(config_file, e))
            exit(1)


def usage():
    print("""Usage: {} [options]
        -c|--config [configuration_file]: loads the configuration file (default: ./nbi.cfg)
        -h|--help: shows this help
        """.format(sys.argv[0]))
    # --log-socket-host HOST: send logs to this host")
    # --log-socket-port PORT: send logs using this port (default: 9022)")


if __name__ == '__main__':
    try:
        # load parameters and configuration
        opts, args = getopt.getopt(sys.argv[1:], "hc:", ["config=", "help"])
        # TODO add  "log-socket-host=", "log-socket-port=", "log-file="
        config_file = None
        for o, a in opts:
            if o in ("-h", "--help"):
                usage()
                sys.exit()
            elif o in ("-c", "--config"):
                config_file = a
            # elif o == "--log-socket-port":
            #     log_socket_port = a
            # elif o == "--log-socket-host":
            #     log_socket_host = a
            # elif o == "--log-file":
            #     log_file = a
            else:
                assert False, "Unhandled option"
        if config_file:
            if not path.isfile(config_file):
                print("configuration file '{}' that not exist".format(config_file), file=sys.stderr)
                exit(1)
        else:
            for config_file in (__file__[:__file__.rfind(".")] + ".cfg", "./lcm.cfg", "/etc/osm/lcm.cfg"):
                if path.isfile(config_file):
                    break
            else:
                print("No configuration file 'nbi.cfg' found neither at local folder nor at /etc/osm/", file=sys.stderr)
                exit(1)
        lcm = Lcm(config_file)
        lcm.start()
    except (LcmException, getopt.GetoptError) as e:
        print(str(e), file=sys.stderr)
        # usage()
        exit(1)
