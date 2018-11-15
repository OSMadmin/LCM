#!/usr/bin/python3
# -*- coding: utf-8 -*-

import asyncio
import logging
import logging.handlers
import traceback
import ns
from lcm_utils import LcmException, LcmBase
from osm_common.dbbase import DbException
from time import time

__author__ = "Felipe Vicens, Pol Alemany, Alfonso Tierno"


class NetsliceLcm(LcmBase):

    def __init__(self, db, msg, fs, lcm_tasks, ro_config, vca_config, loop):
        """
        Init, Connect to database, filesystem storage, and messaging
        :param config: two level dictionary with configuration. Top level should contain 'database', 'storage',
        :return: None
        """
        # logging
        self.logger = logging.getLogger('lcm.netslice')
        self.loop = loop
        self.lcm_tasks = lcm_tasks
        self.ns = ns.NsLcm(db, msg, fs, lcm_tasks, ro_config, vca_config, loop)

        super().__init__(db, msg, fs, self.logger)

    # TODO: check logging_text within the self.logger.info/debug
    async def instantiate(self, nsir_id, nsilcmop_id):
        logging_text = "Task netslice={} instantiate={} ".format(nsir_id, nsilcmop_id)
        self.logger.debug(logging_text + "Enter")
        # get all needed from database
        exc = None 
        db_nsir = None
        db_nsilcmop = None
        db_nsir_update = {"_admin.nsilcmop": nsilcmop_id}
        db_nsilcmop_update = {}
        nsilcmop_operation_state = None

        try:
            step = "Getting nsir={} from db".format(nsir_id)
            db_nsir = self.db.get_one("nsis", {"_id": nsir_id})
            step = "Getting nsilcmop={} from db".format(nsilcmop_id)
            db_nsilcmop = self.db.get_one("nsilcmops", {"_id": nsilcmop_id})
            
            # look if previous tasks is in process
            task_name, task_dependency = self.lcm_tasks.lookfor_related("nsi", nsir_id, nsilcmop_id)
            if task_dependency:
                step = db_nsilcmop_update["detailed-status"] = \
                    "Waiting for related tasks to be completed: {}".format(task_name)
                self.logger.debug(logging_text + step)
                self.update_db_2("nsilcmops", nsilcmop_id, db_nsilcmop_update)
                _, pending = await asyncio.wait(task_dependency, timeout=3600)
                if pending:
                    raise LcmException("Timeout waiting related tasks to be completed")
                       
            # Empty list to keep track of network service records status in the netslice
            nsir_admin = db_nsir["_admin"]

            nsir_admin["nsrs-detailed-list"] = []

            # Slice status Creating
            db_nsir_update["detailed-status"] = "creating"
            db_nsir_update["operational-status"] = "init"
            self.update_db_2("nsis", nsir_id, db_nsir_update)            

            # Iterate over the network services operation ids to instantiate NSs
            # TODO: (future improvement) look another way check the tasks instead of keep asking 
            # -> https://docs.python.org/3/library/asyncio-task.html#waiting-primitives
            # steps: declare ns_tasks, add task when terminate is called, await asyncio.wait(vca_task_list, timeout=300)
            # ns_tasks = []
            nslcmop_ids = db_nsilcmop["operationParams"].get("nslcmops_ids")
            for nslcmop_id in nslcmop_ids:
                nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_id})
                nsr_id = nslcmop.get("nsInstanceId")
                step = "Launching ns={} instantiate={} task".format(nsr_id, nslcmop)
                task = asyncio.ensure_future(self.ns.instantiate(nsr_id, nslcmop_id))
                self.lcm_tasks.register("ns", nsr_id, nslcmop_id, "ns_instantiate", task)

            # Wait until Network Slice is ready
            step = nsir_status_detailed = " Waiting nsi ready. nsi_id={}".format(nsir_id)
            nsrs_detailed_list_old = None
            self.logger.debug(logging_text + step)
            
            # TODO: substitute while for await (all task to be done or not)
            deployment_timeout = 2 * 3600   # Two hours
            while deployment_timeout > 0:
                # Check ns instantiation status
                nsi_ready = True
                nsrs_detailed_list = []
                for nslcmop_item in nslcmop_ids:
                    nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_item})
                    status = nslcmop.get("operationState")
                    # TODO: (future improvement) other possible status: ROLLING_BACK,ROLLED_BACK
                    nsrs_detailed_list.append({"nsrId": nslcmop["nsInstanceId"], "status": nslcmop["operationState"],
                                               "detailed-status": 
                                               nsir_status_detailed + "; {}".format(nslcmop.get("detailed-status"))})
                    if status not in ["COMPLETED", "PARTIALLY_COMPLETED", "FAILED", "FAILED_TEMP"]:  
                        nsi_ready = False

                if nsrs_detailed_list != nsrs_detailed_list_old:
                    nsir_admin["nsrs-detailed-list"] = nsrs_detailed_list
                    nsrs_detailed_list_old = nsrs_detailed_list
                    db_nsir_update["_admin"] = nsir_admin
                    self.update_db_2("nsis", nsir_id, db_nsir_update)

                if nsi_ready:
                    step = "Network Slice Instance is ready. nsi_id={}".format(nsir_id)
                    for items in nsrs_detailed_list:
                        if "FAILED" in items.values():
                            raise LcmException("Error deploying NSI: {}".format(nsir_id))
                    break
 
                # TODO: future improvement due to synchronism -> await asyncio.wait(vca_task_list, timeout=300)
                await asyncio.sleep(5, loop=self.loop)
                deployment_timeout -= 5
            
            if deployment_timeout <= 0:
                raise LcmException("Timeout waiting nsi to be ready. nsi_id={}".format(nsir_id))

            db_nsir_update["operational-status"] = "running"
            db_nsir_update["detailed-status"] = "done"
            db_nsir_update["config-status"] = "configured"
            db_nsilcmop_update["operationState"] = nsilcmop_operation_state = "COMPLETED"
            db_nsilcmop_update["statusEnteredTime"] = time()
            db_nsilcmop_update["detailed-status"] = "done"
            return

        except (LcmException, DbException) as e:
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
                if db_nsir:
                    db_nsir_update["detailed-status"] = "ERROR {}: {}".format(step, exc)
                    db_nsir_update["operational-status"] = "failed"
                if db_nsilcmop:
                    db_nsilcmop_update["detailed-status"] = "FAILED {}: {}".format(step, exc)
                    db_nsilcmop_update["operationState"] = nsilcmop_operation_state = "FAILED"
                    db_nsilcmop_update["statusEnteredTime"] = time()
            if db_nsir:
                db_nsir_update["_admin.nsiState"] = "INSTANTIATED"
                db_nsir_update["_admin.nsilcmop"] = None
                self.update_db_2("nsis", nsir_id, db_nsir_update)
            if db_nsilcmop:

                self.update_db_2("nsilcmops", nsilcmop_id, db_nsilcmop_update)
            if nsilcmop_operation_state:
                try:
                    await self.msg.aiowrite("nsi", "instantiated", {"nsir_id": nsir_id, "nsilcmop_id": nsilcmop_id,
                                                                    "operationState": nsilcmop_operation_state})
                except Exception as e:
                    self.logger.error(logging_text + "kafka_write notification Exception {}".format(e))
            self.logger.debug(logging_text + "Exit")
            self.lcm_tasks.remove("nsi", nsir_id, nsilcmop_id, "nsi_instantiate")

    async def terminate(self, nsir_id, nsilcmop_id):
        logging_text = "Task nsi={} terminate={} ".format(nsir_id, nsilcmop_id)
        self.logger.debug(logging_text + "Enter")
        exc = None 
        db_nsir = None
        db_nsilcmop = None
        db_nsir_update = {"_admin.nsilcmop": nsilcmop_id}
        db_nsilcmop_update = {}
        nsilcmop_operation_state = None
        try:
            step = "Getting nsir={} from db".format(nsir_id)
            db_nsir = self.db.get_one("nsis", {"_id": nsir_id})
            step = "Getting nsilcmop={} from db".format(nsilcmop_id)
            db_nsilcmop = self.db.get_one("nsilcmops", {"_id": nsilcmop_id})
            
            # TODO: Check if makes sense check the nsiState=NOT_INSTANTIATED when terminate
            # CASE: Instance was terminated but there is a second request to terminate the instance
            if db_nsir["_admin"]["nsiState"] == "NOT_INSTANTIATED":
                return

            # Slice status Terminating
            db_nsir_update["operational-status"] = "terminating"
            db_nsir_update["config-status"] = "terminating"
            self.update_db_2("nsis", nsir_id, db_nsir_update)

            # look if previous tasks is in process
            task_name, task_dependency = self.lcm_tasks.lookfor_related("nsi", nsir_id, nsilcmop_id)
            if task_dependency:
                step = db_nsilcmop_update["detailed-status"] = \
                    "Waiting for related tasks to be completed: {}".format(task_name)
                self.logger.debug(logging_text + step)
                self.update_db_2("nsilcmops", nsilcmop_id, db_nsilcmop_update)
                _, pending = await asyncio.wait(task_dependency, timeout=3600)
                if pending:
                    raise LcmException("Timeout waiting related tasks to be completed")
                       
            # Gets the list to keep track of network service records status in the netslice
            nsir_admin = db_nsir["_admin"]
            nsrs_detailed_list = []       

            # Iterate over the network services operation ids to terminate NSs
            # TODO: (future improvement) look another way check the tasks instead of keep asking 
            # -> https://docs.python.org/3/library/asyncio-task.html#waiting-primitives
            # steps: declare ns_tasks, add task when terminate is called, await asyncio.wait(vca_task_list, timeout=300)
            # ns_tasks = []
            nslcmop_ids = db_nsilcmop["operationParams"].get("nslcmops_ids")
            for nslcmop_id in nslcmop_ids:
                nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_id})
                nsr_id = nslcmop["operationParams"].get("nsInstanceId")
                task = asyncio.ensure_future(self.ns.terminate(nsr_id, nslcmop_id))
                self.lcm_tasks.register("ns", nsr_id, nslcmop_id, "ns_instantiate", task)

            # Wait until Network Slice is terminated
            step = nsir_status_detailed = " Waiting nsi terminated. nsi_id={}".format(nsir_id)
            nsrs_detailed_list_old = None
            self.logger.debug(logging_text + step)
            
            termination_timeout = 2 * 3600   # Two hours
            while termination_timeout > 0:
                # Check ns termination status
                nsi_ready = True
                nsrs_detailed_list = []
                for nslcmop_item in nslcmop_ids:
                    nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_item})
                    status = nslcmop["operationState"]
                    # TODO: (future improvement) other possible status: ROLLING_BACK,ROLLED_BACK 
                    nsrs_detailed_list.append({"nsrId": nslcmop["nsInstanceId"], "status": nslcmop["operationState"],
                                               "detailed-status": 
                                               nsir_status_detailed + "; {}".format(nslcmop.get("detailed-status"))})
                    if status not in ["COMPLETED", "PARTIALLY_COMPLETED", "FAILED", "FAILED_TEMP"]:
                        nsi_ready = False

                if nsrs_detailed_list != nsrs_detailed_list_old:
                    nsir_admin["nsrs-detailed-list"] = nsrs_detailed_list
                    nsrs_detailed_list_old = nsrs_detailed_list
                    db_nsir_update["_admin"] = nsir_admin
                    self.update_db_2("nsis", nsir_id, db_nsir_update)
                    
                if nsi_ready:
                    step = "Network Slice Instance is terminated. nsi_id={}".format(nsir_id)
                    for items in nsrs_detailed_list:
                        if "FAILED" in items.values():
                            raise LcmException("Error terminating NSI: {}".format(nsir_id))
                    break

                await asyncio.sleep(5, loop=self.loop)
                termination_timeout -= 5

            if termination_timeout <= 0:
                raise LcmException("Timeout waiting nsi to be terminated. nsi_id={}".format(nsir_id))

            db_nsir_update["operational-status"] = "terminated"
            db_nsir_update["config-status"] = "configured"            
            db_nsir_update["detailed-status"] = "done" 
            db_nsilcmop_update["operationState"] = nsilcmop_operation_state = "COMPLETED"
            db_nsilcmop_update["statusEnteredTime"] = time()
            db_nsilcmop_update["detailed-status"] = "done"
            return

        except (LcmException, DbException) as e:
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
                if db_nsir:
                    db_nsir_update["detailed-status"] = "ERROR {}: {}".format(step, exc)
                    db_nsir_update["operational-status"] = "failed"
                if db_nsilcmop:
                    db_nsilcmop_update["detailed-status"] = "FAILED {}: {}".format(step, exc)
                    db_nsilcmop_update["operationState"] = nsilcmop_operation_state = "FAILED"
                    db_nsilcmop_update["statusEnteredTime"] = time()
            if db_nsir:
                db_nsir_update["_admin.nsilcmop"] = None
                db_nsir_update["_admin.nsiState"] = "TERMINATED"
                self.update_db_2("nsis", nsir_id, db_nsir_update)
            if db_nsilcmop:
                self.update_db_2("nsilcmops", nsilcmop_id, db_nsilcmop_update)

            if nsilcmop_operation_state:
                try:
                    await self.msg.aiowrite("nsi", "terminated", {"nsir_id": nsir_id, "nsilcmop_id": nsilcmop_id,
                                                                  "operationState": nsilcmop_operation_state})
                except Exception as e:
                    self.logger.error(logging_text + "kafka_write notification Exception {}".format(e))
            self.logger.debug(logging_text + "Exit")
            self.lcm_tasks.remove("nsi", nsir_id, nsilcmop_id, "nsi_terminate")
