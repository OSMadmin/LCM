# -*- coding: utf-8 -*-

import asyncio
import logging
import logging.handlers
import traceback
import ns
from ns import populate_dict as populate_dict
import ROclient
from lcm_utils import LcmException, LcmBase
from osm_common.dbbase import DbException
from time import time
from http import HTTPStatus
from copy import deepcopy


__author__ = "Felipe Vicens, Pol Alemany, Alfonso Tierno"


def get_iterable(in_dict, in_key):
    """
    Similar to <dict>.get(), but if value is None, False, ..., An empty tuple is returned instead
    :param in_dict: a dictionary
    :param in_key: the key to look for at in_dict
    :return: in_dict[in_var] or () if it is None or not present
    """
    if not in_dict.get(in_key):
        return ()
    return in_dict[in_key]


class NetsliceLcm(LcmBase):

    total_deploy_timeout = 2 * 3600   # global timeout for deployment

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
        self.ro_config = ro_config

        super().__init__(db, msg, fs, self.logger)

    def nsi_update_nsir(self, nsi_update_nsir, db_nsir, nsir_desc_RO):
        """
        Updates database nsir with the RO info for the created vld
        :param nsi_update_nsir: dictionary to be filled with the updated info
        :param db_nsir: content of db_nsir. This is also modified
        :param nsir_desc_RO: nsir descriptor from RO
        :return: Nothing, LcmException is raised on errors
        """

        for vld_index, vld in enumerate(get_iterable(db_nsir, "vld")):
            for net_RO in get_iterable(nsir_desc_RO, "nets"):
                if vld["id"] != net_RO.get("ns_net_osm_id"):
                    continue
                vld["vim-id"] = net_RO.get("vim_net_id")
                vld["name"] = net_RO.get("vim_name")
                vld["status"] = net_RO.get("status")
                vld["status-detailed"] = net_RO.get("error_msg")
                nsi_update_nsir["vld.{}".format(vld_index)] = vld
                break
            else:
                raise LcmException("ns_update_nsir: Not found vld={} at RO info".format(vld["id"]))

    async def instantiate(self, nsir_id, nsilcmop_id):
        logging_text = "Task netslice={} instantiate={} ".format(nsir_id, nsilcmop_id)
        self.logger.debug(logging_text + "Enter")
        # get all needed from database
        exc = None
        RO_nsir_id = None
        db_nsir = None
        db_nsilcmop = None
        db_nsir_update = {"_admin.nsilcmop": nsilcmop_id}
        db_nsilcmop_update = {}
        nsilcmop_operation_state = None
        vim_2_RO = {}
        RO = ROclient.ROClient(self.loop, **self.ro_config)
        start_deploy = time()

        def vim_account_2_RO(vim_account):
            """
            Translate a RO vim_account from OSM vim_account params
            :param ns_params: OSM instantiate params
            :return: The RO ns descriptor
            """
            if vim_account in vim_2_RO:
                return vim_2_RO[vim_account]

            db_vim = self.db.get_one("vim_accounts", {"_id": vim_account})
            if db_vim["_admin"]["operationalState"] != "ENABLED":
                raise LcmException("VIM={} is not available. operationalState={}".format(
                    vim_account, db_vim["_admin"]["operationalState"]))
            RO_vim_id = db_vim["_admin"]["deployed"]["RO"]
            vim_2_RO[vim_account] = RO_vim_id
            return RO_vim_id

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

            # TODO: Check Multiple VIMs networks: datacenters

            # Creating netslice VLDs networking before NS instantiation
            for netslice_subnet in get_iterable(nsir_admin, "netslice-subnet"):
                db_nsd = self.db.get_one("nsds", {"_id": netslice_subnet["nsdId"]})

                # Fist operate with VLDs inside netslice_subnet
                for vld_item in get_iterable(netslice_subnet, "vld"):
                    RO_ns_params = {}
                    RO_ns_params["name"] = vld_item["name"]
                    RO_ns_params["datacenter"] = vim_account_2_RO(db_nsir["datacenter"])

                    # TODO: Enable in the ns fake scenario the ip-profile
                    # if "ip-profile" in netslice-subnet:
                    #     populate_dict(RO_ns_params, ("networks", vld_params["name"], "ip-profile"),
                    #                 ip_profile_2_RO(vld_params["ip-profile"]))
                    # TODO: Check VDU type in all descriptors finding SRIOV / PT
                    # Updating network names and datacenters from instantiation parameters for each VLD
                    mgmt_network = False
                    for nsd_vld in get_iterable(db_nsd, "vld"):
                        if nsd_vld["name"] == vld_item["name"]:
                            if nsd_vld.get("mgmt-network"):
                                mgmt_network = True
                                break

                    # Creating scenario if vim-network-name / vim-network-id are present as instantiation parameter
                    # Use vim-network-id instantiation parameter
                    vim_network_option = None
                    if vld_item.get("vim-network-id"):
                        vim_network_option = "vim-network-id"
                    elif vld_item.get("vim-network-name"):   
                        vim_network_option = "vim-network-name"

                    if vim_network_option:
                        RO_vld_sites = []
                        if vld_item.get(vim_network_option):
                            if isinstance(vld_item[vim_network_option], dict):
                                for vim_account, vim_net in vld_item[vim_network_option].items():
                                    RO_vld_sites.append({
                                        "netmap-use": vim_net,
                                        "datacenter": vim_account_2_RO(vim_account)
                                    })
                            else:
                                RO_vld_sites.append({"netmap-use": vld_item[vim_network_option],
                                                    "datacenter": vim_account_2_RO(netslice_subnet["vimAccountId"])})
                        if RO_vld_sites:
                            populate_dict(RO_ns_params, ("networks", vld_item["name"], "sites"), RO_vld_sites)

                        if mgmt_network:
                            RO_ns_params["scenario"] = {"nets": [{"name": vld_item["name"],
                                                        "external": True, "type": "bridge"}]}
                        else:
                            RO_ns_params["scenario"] = {"nets": [{"name": vld_item["name"],
                                                        "external": False, "type": "bridge"}]}

                    # Use default netslice vim-network-name from template
                    else:
                        if mgmt_network:
                            RO_ns_params["scenario"] = {"nets": [{"name": vld_item["name"],
                                                        "external": True, "type": "bridge"}]}
                        else:
                            RO_ns_params["scenario"] = {"nets": [{"name": vld_item["name"],
                                                        "external": False, "type": "bridge"}]}

                    # Creating netslice-vld at RO
                    RO_nsir_id = db_nsir["_admin"].get("deployed", {}).get("RO", {}).get("nsir_id")

                    # if RO vlds are present use it unless in error status
                    if RO_nsir_id:
                        try:
                            step = db_nsir_update["detailed-status"] = "Looking for existing ns at RO"
                            self.logger.debug(logging_text + step + " RO_ns_id={}".format(RO_nsir_id))
                            desc = await RO.show("ns", RO_nsir_id)
                        except ROclient.ROClientException as e:
                            if e.http_code != HTTPStatus.NOT_FOUND:
                                raise
                            RO_nsir_id = db_nsir_update["_admin.deployed.RO.nsir_id"] = None
                        if RO_nsir_id:
                            ns_status, ns_status_info = RO.check_ns_status(desc)
                            db_nsir_update["_admin.deployed.RO.nsir_status"] = ns_status
                            if ns_status == "ERROR":
                                step = db_nsir_update["detailed-status"] = "Deleting ns at RO. RO_ns_id={}"\
                                                                           .format(RO_nsir_id)
                                self.logger.debug(logging_text + step)
                                await RO.delete("ns", RO_nsir_id)
                                RO_nsir_id = db_nsir_update["_admin.deployed.RO.nsir_id"] = None

                    # If network doesn't exists then create it
                    else:
                        step = db_nsir_update["detailed-status"] = "Checking dependencies"
                        self.logger.debug(logging_text + step)
                        # check if VIM is creating and wait  look if previous tasks in process
                        # TODO: Check the case for multiple datacenters specified in instantiation parameter
                        for vimAccountId_unit in RO_ns_params["datacenter"]:
                            task_name, task_dependency = self.lcm_tasks.lookfor_related("vim_account",
                                                                                        vimAccountId_unit)
                            if task_dependency:
                                step = "Waiting for related tasks to be completed: {}".format(task_name)
                                self.logger.debug(logging_text + step)
                                await asyncio.wait(task_dependency, timeout=3600)

                        step = db_nsir_update["detailed-status"] = "Creating netslice-vld at RO"
                        desc = await RO.create("ns", descriptor=RO_ns_params)
                        RO_nsir_id = db_nsir_update["_admin.deployed.RO.nsir_id"] = desc["uuid"]
                        db_nsir_update["_admin.nsState"] = "INSTANTIATED"
                        db_nsir_update["_admin.deployed.RO.nsir_status"] = "BUILD"
                        self.logger.debug(logging_text + "netslice-vld created at RO. RO_id={}".format(desc["uuid"]))
                        self.update_db_2("nsis", nsir_id, db_nsir_update)

                if RO_nsir_id:
                    # wait until NS scenario for netslice-vld is ready
                    step = ns_status_detailed = detailed_status = "Waiting netslice-vld ready at RO. RO_id={}"\
                                                                  .format(RO_nsir_id)
                    detailed_status_old = None
                    self.logger.debug(logging_text + step)

                    while time() <= start_deploy + self.total_deploy_timeout:
                        desc = await RO.show("ns", RO_nsir_id)
                        ns_status, ns_status_info = RO.check_ns_status(desc)
                        db_nsir_update["admin.deployed.RO.nsir_status"] = ns_status
                        db_nsir_update["admin.deployed.RO.netslice_scenario_id"] = desc.get("uuid")
                        netROinfo_list = []
                        name = desc.get("name")
                        if ns_status == "ERROR":
                            raise ROclient.ROClientException(ns_status_info)
                        elif ns_status == "BUILD":
                            detailed_status = ns_status_detailed + "; {}".format(ns_status_info)
                        elif ns_status == "ACTIVE":
                            for nets in get_iterable(desc, "nets"):
                                netROinfo = {"name": name, "vim_net_id": nets.get("vim_net_id"),
                                             "datacenter_id": nets.get("datacenter_id"),
                                             "vim_name": nets.get("vim_name")}
                                netROinfo_list.append(netROinfo)
                            db_nsir_update["admin.deployed.RO.vim_network_info"] = netROinfo_list
                            self.update_db_2("nsis", nsir_id, db_nsir_update)
                            break
                        else:
                            assert False, "ROclient.check_ns_status returns unknown {}".format(ns_status)
                        if detailed_status != detailed_status_old:
                            detailed_status_old = db_nsir_update["detailed-status"] = detailed_status
                            self.update_db_2("nsis", nsir_id, db_nsir_update)
                        await asyncio.sleep(5, loop=self.loop)
                    else:   # total_deploy_timeout
                        raise ROclient.ROClientException("Timeout waiting netslice-vld to be ready")

                    step = "Updating NSIR"
                    db_nsir = self.db.get_one("nsis", {"_id": nsir_id})
                    self.nsi_update_nsir(db_nsir_update, db_nsir, desc)

            # Iterate over the network services operation ids to instantiate NSs
            # TODO: (future improvement) look another way check the tasks instead of keep asking 
            # -> https://docs.python.org/3/library/asyncio-task.html#waiting-primitives
            # steps: declare ns_tasks, add task when terminate is called, await asyncio.wait(vca_task_list, timeout=300)
            # ns_tasks = []

            db_nsir = self.db.get_one("nsis", {"_id": nsir_id})
            nslcmop_ids = db_nsilcmop["operationParams"].get("nslcmops_ids")
            for nslcmop_id in nslcmop_ids:
                nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_id})
                nsr_id = nslcmop.get("nsInstanceId")
                # Overwrite instantiation parameters in netslice runtime
                if db_nsir.get("admin"):
                    if db_nsir["admin"].get("deployed"):
                        db_admin_deployed_nsir = db_nsir["admin"].get("deployed")
                        if db_admin_deployed_nsir.get("RO"):
                            RO_item = db_admin_deployed_nsir["RO"]
                            if RO_item.get("vim_network_info"):
                                for vim_network_info_item in RO_item["vim_network_info"]:
                                    if nslcmop.get("operationParams"):
                                        if nslcmop["operationParams"].get("vld"):
                                            for vld in nslcmop["operationParams"]["vld"]:
                                                if vld["name"] == vim_network_info_item.get("name"):
                                                    vld["vim-network-id"] = vim_network_info_item.get("vim_net_id")
                                                    if vld.get("vim-network-name"):
                                                        del vld["vim-network-name"]
                                            self.update_db_2("nslcmops", nslcmop_id, nslcmop)
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

                # TODO: Check admin and _admin
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
            try:
                if db_nsir:
                    db_nsir_update["_admin.nsiState"] = "INSTANTIATED"
                    db_nsir_update["_admin.nsilcmop"] = None
                    self.update_db_2("nsis", nsir_id, db_nsir_update)
                if db_nsilcmop:
                    self.update_db_2("nsilcmops", nsilcmop_id, db_nsilcmop_update)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
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
        RO = ROclient.ROClient(self.loop, **self.ro_config)
        failed_detail = []   # annotates all failed error messages
        nsilcmop_operation_state = None
        autoremove = False  # autoremove after terminated
        try:
            step = "Getting nsir={} from db".format(nsir_id)
            db_nsir = self.db.get_one("nsis", {"_id": nsir_id})
            nsir_deployed = deepcopy(db_nsir["admin"].get("deployed"))
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

            # Delete ns
            RO_nsir_id = RO_delete_action = None
            if nsir_deployed and nsir_deployed.get("RO"):
                RO_nsir_id = nsir_deployed["RO"].get("nsr_id")
                RO_delete_action = nsir_deployed["RO"].get("nsr_delete_action_id")
            try:
                if RO_nsir_id:
                    step = db_nsir_update["detailed-status"] = "Deleting ns at RO"
                    db_nsilcmop_update["detailed-status"] = "Deleting ns at RO"
                    self.logger.debug(logging_text + step)
                    desc = await RO.delete("ns", RO_nsir_id)
                    RO_delete_action = desc["action_id"]
                    db_nsir_update["_admin.deployed.RO.nsr_delete_action_id"] = RO_delete_action
                    db_nsir_update["_admin.deployed.RO.nsr_id"] = None
                    db_nsir_update["_admin.deployed.RO.nsr_status"] = "DELETED"
                if RO_delete_action:
                    # wait until NS is deleted from VIM
                    step = detailed_status = "Waiting ns deleted from VIM. RO_id={}".format(RO_nsir_id)
                    detailed_status_old = None
                    self.logger.debug(logging_text + step)

                    delete_timeout = 20 * 60   # 20 minutes
                    while delete_timeout > 0:
                        desc = await RO.show("ns", item_id_name=RO_nsir_id, extra_item="action",
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
                            detailed_status_old = db_nsilcmop_update["detailed-status"] = detailed_status
                            self.update_db_2("nslcmops", nslcmop_id, db_nsilcmop_update)
                    else:  # delete_timeout <= 0:
                        raise ROclient.ROClientException("Timeout waiting ns deleted from VIM")

            except ROclient.ROClientException as e:
                if e.http_code == 404:  # not found
                    db_nsir_update["_admin.deployed.RO.nsr_id"] = None
                    db_nsir_update["_admin.deployed.RO.nsr_status"] = "DELETED"
                    self.logger.debug(logging_text + "RO_ns_id={} already deleted".format(RO_nsir_id))
                elif e.http_code == 409:   # conflict
                    failed_detail.append("RO_ns_id={} delete conflict: {}".format(RO_nsir_id, e))
                    self.logger.debug(logging_text + failed_detail[-1])
                else:
                    failed_detail.append("RO_ns_id={} delete error: {}".format(RO_nsir_id, e))
                    self.logger.error(logging_text + failed_detail[-1])

            if failed_detail:
                self.logger.error(logging_text + " ;".join(failed_detail))
                db_nsir_update["operational-status"] = "failed"
                db_nsir_update["detailed-status"] = "Deletion errors " + "; ".join(failed_detail)
                db_nsilcmop_update["detailed-status"] = "; ".join(failed_detail)
                db_nsilcmop_update["operationState"] = nsilcmop_operation_state = "FAILED"
                db_nsilcmop_update["statusEnteredTime"] = time()
            else:
                db_nsir_update["operational-status"] = "terminated"
                db_nsir_update["detailed-status"] = "done"
                db_nsir_update["config-status"] = "configured"
                db_nsir_update["_admin.nsiState"] = "NOT_INSTANTIATED"
                db_nsilcmop_update["detailed-status"] = "Done"
                db_nsilcmop_update["operationState"] = nsilcmop_operation_state = "COMPLETED"
                db_nsilcmop_update["statusEnteredTime"] = time()
                if db_nsilcmop["operationParams"].get("autoremove"):
                    autoremove = True

            db_nsir_update["operational-status"] = "terminated"
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
            try:
                if db_nsir:
                    db_nsir_update["_admin.nsilcmop"] = None
                    self.update_db_2("nsis", nsir_id, db_nsir_update)
                if db_nsilcmop:
                    self.update_db_2("nsilcmops", nsilcmop_id, db_nsilcmop_update)
            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))

            if nsilcmop_operation_state:
                try:
                    await self.msg.aiowrite("nsi", "terminated", {"nsir_id": nsir_id, "nsilcmop_id": nsilcmop_id,
                                                                  "operationState": nsilcmop_operation_state,
                                                                  "autoremove": autoremove},
                                            loop=self.loop)
                except Exception as e:
                    self.logger.error(logging_text + "kafka_write notification Exception {}".format(e))
            self.logger.debug(logging_text + "Exit")
            self.lcm_tasks.remove("nsi", nsir_id, nsilcmop_id, "nsi_terminate")
