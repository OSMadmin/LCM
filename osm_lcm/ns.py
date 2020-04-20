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
import traceback
import json
from jinja2 import Environment, Template, meta, TemplateError, TemplateNotFound, TemplateSyntaxError

from osm_lcm import ROclient
from osm_lcm.lcm_utils import LcmException, LcmExceptionNoMgmtIP, LcmBase, deep_get, get_iterable, populate_dict
from n2vc.k8s_helm_conn import K8sHelmConnector
from n2vc.k8s_juju_conn import K8sJujuConnector

from osm_common.dbbase import DbException
from osm_common.fsbase import FsException

from n2vc.n2vc_juju_conn import N2VCJujuConnector
from n2vc.exceptions import N2VCException, N2VCNotFound, K8sException

from copy import copy, deepcopy
from http import HTTPStatus
from time import time
from uuid import uuid4
from functools import partial

__author__ = "Alfonso Tierno"


class NsLcm(LcmBase):
    timeout_vca_on_error = 5 * 60   # Time for charm from first time at blocked,error status to mark as failed
    timeout_ns_deploy = 2 * 3600   # default global timeout for deployment a ns
    timeout_ns_terminate = 1800   # default global timeout for un deployment a ns
    timeout_charm_delete = 10 * 60
    timeout_primitive = 10 * 60  # timeout for primitive execution
    timeout_progress_primitive = 2 * 60  # timeout for some progress in a primitive execution

    SUBOPERATION_STATUS_NOT_FOUND = -1
    SUBOPERATION_STATUS_NEW = -2
    SUBOPERATION_STATUS_SKIP = -3
    task_name_deploy_vca = "Deploying VCA"

    def __init__(self, db, msg, fs, lcm_tasks, config, loop):
        """
        Init, Connect to database, filesystem storage, and messaging
        :param config: two level dictionary with configuration. Top level should contain 'database', 'storage',
        :return: None
        """
        super().__init__(
            db=db,
            msg=msg,
            fs=fs,
            logger=logging.getLogger('lcm.ns')
        )

        self.loop = loop
        self.lcm_tasks = lcm_tasks
        self.timeout = config["timeout"]
        self.ro_config = config["ro_config"]
        self.vca_config = config["VCA"].copy()

        # create N2VC connector
        self.n2vc = N2VCJujuConnector(
            db=self.db,
            fs=self.fs,
            log=self.logger,
            loop=self.loop,
            url='{}:{}'.format(self.vca_config['host'], self.vca_config['port']),
            username=self.vca_config.get('user', None),
            vca_config=self.vca_config,
            on_update_db=self._on_update_n2vc_db
        )

        self.k8sclusterhelm = K8sHelmConnector(
            kubectl_command=self.vca_config.get("kubectlpath"),
            helm_command=self.vca_config.get("helmpath"),
            fs=self.fs,
            log=self.logger,
            db=self.db,
            on_update_db=None,
        )

        self.k8sclusterjuju = K8sJujuConnector(
            kubectl_command=self.vca_config.get("kubectlpath"),
            juju_command=self.vca_config.get("jujupath"),
            fs=self.fs,
            log=self.logger,
            db=self.db,
            on_update_db=None,
        )

        self.k8scluster_map = {
            "helm-chart": self.k8sclusterhelm,
            "chart": self.k8sclusterhelm,
            "juju-bundle": self.k8sclusterjuju,
            "juju": self.k8sclusterjuju,
        }
        # create RO client
        self.RO = ROclient.ROClient(self.loop, **self.ro_config)

    def _on_update_ro_db(self, nsrs_id, ro_descriptor):

        # self.logger.debug('_on_update_ro_db(nsrs_id={}'.format(nsrs_id))

        try:
            # TODO filter RO descriptor fields...

            # write to database
            db_dict = dict()
            # db_dict['deploymentStatus'] = yaml.dump(ro_descriptor, default_flow_style=False, indent=2)
            db_dict['deploymentStatus'] = ro_descriptor
            self.update_db_2("nsrs", nsrs_id, db_dict)

        except Exception as e:
            self.logger.warn('Cannot write database RO deployment for ns={} -> {}'.format(nsrs_id, e))

    async def _on_update_n2vc_db(self, table, filter, path, updated_data):

        # remove last dot from path (if exists)
        if path.endswith('.'):
            path = path[:-1]

        # self.logger.debug('_on_update_n2vc_db(table={}, filter={}, path={}, updated_data={}'
        #                   .format(table, filter, path, updated_data))

        try:

            nsr_id = filter.get('_id')

            # read ns record from database
            nsr = self.db.get_one(table='nsrs', q_filter=filter)
            current_ns_status = nsr.get('nsState')

            # get vca status for NS
            status_dict = await self.n2vc.get_status(namespace='.' + nsr_id, yaml_format=False)

            # vcaStatus
            db_dict = dict()
            db_dict['vcaStatus'] = status_dict

            # update configurationStatus for this VCA
            try:
                vca_index = int(path[path.rfind(".")+1:])

                vca_list = deep_get(target_dict=nsr, key_list=('_admin', 'deployed', 'VCA'))
                vca_status = vca_list[vca_index].get('status')

                configuration_status_list = nsr.get('configurationStatus')
                config_status = configuration_status_list[vca_index].get('status')

                if config_status == 'BROKEN' and vca_status != 'failed':
                    db_dict['configurationStatus'][vca_index] = 'READY'
                elif config_status != 'BROKEN' and vca_status == 'failed':
                    db_dict['configurationStatus'][vca_index] = 'BROKEN'
            except Exception as e:
                # not update configurationStatus
                self.logger.debug('Error updating vca_index (ignore): {}'.format(e))

            # if nsState = 'READY' check if juju is reporting some error => nsState = 'DEGRADED'
            # if nsState = 'DEGRADED' check if all is OK
            is_degraded = False
            if current_ns_status in ('READY', 'DEGRADED'):
                error_description = ''
                # check machines
                if status_dict.get('machines'):
                    for machine_id in status_dict.get('machines'):
                        machine = status_dict.get('machines').get(machine_id)
                        # check machine agent-status
                        if machine.get('agent-status'):
                            s = machine.get('agent-status').get('status')
                            if s != 'started':
                                is_degraded = True
                                error_description += 'machine {} agent-status={} ; '.format(machine_id, s)
                        # check machine instance status
                        if machine.get('instance-status'):
                            s = machine.get('instance-status').get('status')
                            if s != 'running':
                                is_degraded = True
                                error_description += 'machine {} instance-status={} ; '.format(machine_id, s)
                # check applications
                if status_dict.get('applications'):
                    for app_id in status_dict.get('applications'):
                        app = status_dict.get('applications').get(app_id)
                        # check application status
                        if app.get('status'):
                            s = app.get('status').get('status')
                            if s != 'active':
                                is_degraded = True
                                error_description += 'application {} status={} ; '.format(app_id, s)

                if error_description:
                    db_dict['errorDescription'] = error_description
                if current_ns_status == 'READY' and is_degraded:
                    db_dict['nsState'] = 'DEGRADED'
                if current_ns_status == 'DEGRADED' and not is_degraded:
                    db_dict['nsState'] = 'READY'

            # write to database
            self.update_db_2("nsrs", nsr_id, db_dict)

        except (asyncio.CancelledError, asyncio.TimeoutError):
            raise
        except Exception as e:
            self.logger.warn('Error updating NS state for ns={}: {}'.format(nsr_id, e))

    def vnfd2RO(self, vnfd, new_id=None, additionalParams=None, nsrId=None):
        """
        Converts creates a new vnfd descriptor for RO base on input OSM IM vnfd
        :param vnfd: input vnfd
        :param new_id: overrides vnf id if provided
        :param additionalParams: Instantiation params for VNFs provided
        :param nsrId: Id of the NSR
        :return: copy of vnfd
        """
        try:
            vnfd_RO = deepcopy(vnfd)
            # remove unused by RO configuration, monitoring, scaling and internal keys
            vnfd_RO.pop("_id", None)
            vnfd_RO.pop("_admin", None)
            vnfd_RO.pop("vnf-configuration", None)
            vnfd_RO.pop("monitoring-param", None)
            vnfd_RO.pop("scaling-group-descriptor", None)
            vnfd_RO.pop("kdu", None)
            vnfd_RO.pop("k8s-cluster", None)
            if new_id:
                vnfd_RO["id"] = new_id

            # parse cloud-init or cloud-init-file with the provided variables using Jinja2
            for vdu in get_iterable(vnfd_RO, "vdu"):
                cloud_init_file = None
                if vdu.get("cloud-init-file"):
                    base_folder = vnfd["_admin"]["storage"]
                    cloud_init_file = "{}/{}/cloud_init/{}".format(base_folder["folder"], base_folder["pkg-dir"],
                                                                   vdu["cloud-init-file"])
                    with self.fs.file_open(cloud_init_file, "r") as ci_file:
                        cloud_init_content = ci_file.read()
                    vdu.pop("cloud-init-file", None)
                elif vdu.get("cloud-init"):
                    cloud_init_content = vdu["cloud-init"]
                else:
                    continue

                env = Environment()
                ast = env.parse(cloud_init_content)
                mandatory_vars = meta.find_undeclared_variables(ast)
                if mandatory_vars:
                    for var in mandatory_vars:
                        if not additionalParams or var not in additionalParams.keys():
                            raise LcmException("Variable '{}' defined at vnfd[id={}]:vdu[id={}]:cloud-init/cloud-init-"
                                               "file, must be provided in the instantiation parameters inside the "
                                               "'additionalParamsForVnf' block".format(var, vnfd["id"], vdu["id"]))
                template = Template(cloud_init_content)
                cloud_init_content = template.render(additionalParams or {})
                vdu["cloud-init"] = cloud_init_content

            return vnfd_RO
        except FsException as e:
            raise LcmException("Error reading vnfd[id={}]:vdu[id={}]:cloud-init-file={}: {}".
                               format(vnfd["id"], vdu["id"], cloud_init_file, e))
        except (TemplateError, TemplateNotFound, TemplateSyntaxError) as e:
            raise LcmException("Error parsing Jinja2 to cloud-init content at vnfd[id={}]:vdu[id={}]: {}".
                               format(vnfd["id"], vdu["id"], e))

    def _ns_params_2_RO(self, ns_params, nsd, vnfd_dict, n2vc_key_list):
        """
        Creates a RO ns descriptor from OSM ns_instantiate params
        :param ns_params: OSM instantiate params
        :return: The RO ns descriptor
        """
        vim_2_RO = {}
        wim_2_RO = {}
        # TODO feature 1417: Check that no instantiation is set over PDU
        # check if PDU forces a concrete vim-network-id and add it
        # check if PDU contains a SDN-assist info (dpid, switch, port) and pass it to RO

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

        def wim_account_2_RO(wim_account):
            if isinstance(wim_account, str):
                if wim_account in wim_2_RO:
                    return wim_2_RO[wim_account]

                db_wim = self.db.get_one("wim_accounts", {"_id": wim_account})
                if db_wim["_admin"]["operationalState"] != "ENABLED":
                    raise LcmException("WIM={} is not available. operationalState={}".format(
                        wim_account, db_wim["_admin"]["operationalState"]))
                RO_wim_id = db_wim["_admin"]["deployed"]["RO-account"]
                wim_2_RO[wim_account] = RO_wim_id
                return RO_wim_id
            else:
                return wim_account

        def ip_profile_2_RO(ip_profile):
            RO_ip_profile = deepcopy((ip_profile))
            if "dns-server" in RO_ip_profile:
                if isinstance(RO_ip_profile["dns-server"], list):
                    RO_ip_profile["dns-address"] = []
                    for ds in RO_ip_profile.pop("dns-server"):
                        RO_ip_profile["dns-address"].append(ds['address'])
                else:
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
            "wim_account": wim_account_2_RO(ns_params.get("wimAccountId")),
            # "scenario": ns_params["nsdId"],
        }

        n2vc_key_list = n2vc_key_list or []
        for vnfd_ref, vnfd in vnfd_dict.items():
            vdu_needed_access = []
            mgmt_cp = None
            if vnfd.get("vnf-configuration"):
                ssh_required = deep_get(vnfd, ("vnf-configuration", "config-access", "ssh-access", "required"))
                if ssh_required and vnfd.get("mgmt-interface"):
                    if vnfd["mgmt-interface"].get("vdu-id"):
                        vdu_needed_access.append(vnfd["mgmt-interface"]["vdu-id"])
                    elif vnfd["mgmt-interface"].get("cp"):
                        mgmt_cp = vnfd["mgmt-interface"]["cp"]

            for vdu in vnfd.get("vdu", ()):
                if vdu.get("vdu-configuration"):
                    ssh_required = deep_get(vdu, ("vdu-configuration", "config-access", "ssh-access", "required"))
                    if ssh_required:
                        vdu_needed_access.append(vdu["id"])
                elif mgmt_cp:
                    for vdu_interface in vdu.get("interface"):
                        if vdu_interface.get("external-connection-point-ref") and \
                                vdu_interface["external-connection-point-ref"] == mgmt_cp:
                            vdu_needed_access.append(vdu["id"])
                            mgmt_cp = None
                            break

            if vdu_needed_access:
                for vnf_member in nsd.get("constituent-vnfd"):
                    if vnf_member["vnfd-id-ref"] != vnfd_ref:
                        continue
                    for vdu in vdu_needed_access:
                        populate_dict(RO_ns_params,
                                      ("vnfs", vnf_member["member-vnf-index"], "vdus", vdu, "mgmt_keys"),
                                      n2vc_key_list)

        if ns_params.get("vduImage"):
            RO_ns_params["vduImage"] = ns_params["vduImage"]

        if ns_params.get("ssh_keys"):
            RO_ns_params["cloud-config"] = {"key-pairs": ns_params["ssh_keys"]}
        for vnf_params in get_iterable(ns_params, "vnf"):
            for constituent_vnfd in nsd["constituent-vnfd"]:
                if constituent_vnfd["member-vnf-index"] == vnf_params["member-vnf-index"]:
                    vnf_descriptor = vnfd_dict[constituent_vnfd["vnfd-id-ref"]]
                    break
            else:
                raise LcmException("Invalid instantiate parameter vnf:member-vnf-index={} is not present at nsd:"
                                   "constituent-vnfd".format(vnf_params["member-vnf-index"]))
            if vnf_params.get("vimAccountId"):
                populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "datacenter"),
                              vim_account_2_RO(vnf_params["vimAccountId"]))

            for vdu_params in get_iterable(vnf_params, "vdu"):
                # TODO feature 1417: check that this VDU exist and it is not a PDU
                if vdu_params.get("volume"):
                    for volume_params in vdu_params["volume"]:
                        if volume_params.get("vim-volume-id"):
                            populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "vdus",
                                                         vdu_params["id"], "devices", volume_params["name"], "vim_id"),
                                          volume_params["vim-volume-id"])
                if vdu_params.get("interface"):
                    for interface_params in vdu_params["interface"]:
                        if interface_params.get("ip-address"):
                            populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "vdus",
                                                         vdu_params["id"], "interfaces", interface_params["name"],
                                                         "ip_address"),
                                          interface_params["ip-address"])
                        if interface_params.get("mac-address"):
                            populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "vdus",
                                                         vdu_params["id"], "interfaces", interface_params["name"],
                                                         "mac_address"),
                                          interface_params["mac-address"])
                        if interface_params.get("floating-ip-required"):
                            populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "vdus",
                                                         vdu_params["id"], "interfaces", interface_params["name"],
                                                         "floating-ip"),
                                          interface_params["floating-ip-required"])

            for internal_vld_params in get_iterable(vnf_params, "internal-vld"):
                if internal_vld_params.get("vim-network-name"):
                    populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "networks",
                                                 internal_vld_params["name"], "vim-network-name"),
                                  internal_vld_params["vim-network-name"])
                if internal_vld_params.get("vim-network-id"):
                    populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "networks",
                                                 internal_vld_params["name"], "vim-network-id"),
                                  internal_vld_params["vim-network-id"])
                if internal_vld_params.get("ip-profile"):
                    populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "networks",
                                                 internal_vld_params["name"], "ip-profile"),
                                  ip_profile_2_RO(internal_vld_params["ip-profile"]))
                if internal_vld_params.get("provider-network"):

                    populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "networks",
                                                 internal_vld_params["name"], "provider-network"),
                                  internal_vld_params["provider-network"].copy())

                for icp_params in get_iterable(internal_vld_params, "internal-connection-point"):
                    # look for interface
                    iface_found = False
                    for vdu_descriptor in vnf_descriptor["vdu"]:
                        for vdu_interface in vdu_descriptor["interface"]:
                            if vdu_interface.get("internal-connection-point-ref") == icp_params["id-ref"]:
                                if icp_params.get("ip-address"):
                                    populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "vdus",
                                                                 vdu_descriptor["id"], "interfaces",
                                                                 vdu_interface["name"], "ip_address"),
                                                  icp_params["ip-address"])

                                if icp_params.get("mac-address"):
                                    populate_dict(RO_ns_params, ("vnfs", vnf_params["member-vnf-index"], "vdus",
                                                                 vdu_descriptor["id"], "interfaces",
                                                                 vdu_interface["name"], "mac_address"),
                                                  icp_params["mac-address"])
                                iface_found = True
                                break
                        if iface_found:
                            break
                    else:
                        raise LcmException("Invalid instantiate parameter vnf:member-vnf-index[{}]:"
                                           "internal-vld:id-ref={} is not present at vnfd:internal-"
                                           "connection-point".format(vnf_params["member-vnf-index"],
                                                                     icp_params["id-ref"]))

        for vld_params in get_iterable(ns_params, "vld"):
            if "ip-profile" in vld_params:
                populate_dict(RO_ns_params, ("networks", vld_params["name"], "ip-profile"),
                              ip_profile_2_RO(vld_params["ip-profile"]))

            if vld_params.get("provider-network"):

                populate_dict(RO_ns_params, ("networks", vld_params["name"], "provider-network"),
                              vld_params["provider-network"].copy())

            if "wimAccountId" in vld_params and vld_params["wimAccountId"] is not None:
                populate_dict(RO_ns_params, ("networks", vld_params["name"], "wim_account"),
                              wim_account_2_RO(vld_params["wimAccountId"])),
            if vld_params.get("vim-network-name"):
                RO_vld_sites = []
                if isinstance(vld_params["vim-network-name"], dict):
                    for vim_account, vim_net in vld_params["vim-network-name"].items():
                        RO_vld_sites.append({
                            "netmap-use": vim_net,
                            "datacenter": vim_account_2_RO(vim_account)
                        })
                else:  # isinstance str
                    RO_vld_sites.append({"netmap-use": vld_params["vim-network-name"]})
                if RO_vld_sites:
                    populate_dict(RO_ns_params, ("networks", vld_params["name"], "sites"), RO_vld_sites)

            if vld_params.get("vim-network-id"):
                RO_vld_sites = []
                if isinstance(vld_params["vim-network-id"], dict):
                    for vim_account, vim_net in vld_params["vim-network-id"].items():
                        RO_vld_sites.append({
                            "netmap-use": vim_net,
                            "datacenter": vim_account_2_RO(vim_account)
                        })
                else:  # isinstance str
                    RO_vld_sites.append({"netmap-use": vld_params["vim-network-id"]})
                if RO_vld_sites:
                    populate_dict(RO_ns_params, ("networks", vld_params["name"], "sites"), RO_vld_sites)
            if vld_params.get("ns-net"):
                if isinstance(vld_params["ns-net"], dict):
                    for vld_id, instance_scenario_id in vld_params["ns-net"].items():
                        RO_vld_ns_net = {"instance_scenario_id": instance_scenario_id, "osm_id": vld_id}
                        populate_dict(RO_ns_params, ("networks", vld_params["name"], "use-network"), RO_vld_ns_net)
            if "vnfd-connection-point-ref" in vld_params:
                for cp_params in vld_params["vnfd-connection-point-ref"]:
                    # look for interface
                    for constituent_vnfd in nsd["constituent-vnfd"]:
                        if constituent_vnfd["member-vnf-index"] == cp_params["member-vnf-index-ref"]:
                            vnf_descriptor = vnfd_dict[constituent_vnfd["vnfd-id-ref"]]
                            break
                    else:
                        raise LcmException(
                            "Invalid instantiate parameter vld:vnfd-connection-point-ref:member-vnf-index-ref={} "
                            "is not present at nsd:constituent-vnfd".format(cp_params["member-vnf-index-ref"]))
                    match_cp = False
                    for vdu_descriptor in vnf_descriptor["vdu"]:
                        for interface_descriptor in vdu_descriptor["interface"]:
                            if interface_descriptor.get("external-connection-point-ref") == \
                                    cp_params["vnfd-connection-point-ref"]:
                                match_cp = True
                                break
                        if match_cp:
                            break
                    else:
                        raise LcmException(
                            "Invalid instantiate parameter vld:vnfd-connection-point-ref:member-vnf-index-ref={}:"
                            "vnfd-connection-point-ref={} is not present at vnfd={}".format(
                                cp_params["member-vnf-index-ref"],
                                cp_params["vnfd-connection-point-ref"],
                                vnf_descriptor["id"]))
                    if cp_params.get("ip-address"):
                        populate_dict(RO_ns_params, ("vnfs", cp_params["member-vnf-index-ref"], "vdus",
                                                     vdu_descriptor["id"], "interfaces",
                                                     interface_descriptor["name"], "ip_address"),
                                      cp_params["ip-address"])
                    if cp_params.get("mac-address"):
                        populate_dict(RO_ns_params, ("vnfs", cp_params["member-vnf-index-ref"], "vdus",
                                                     vdu_descriptor["id"], "interfaces",
                                                     interface_descriptor["name"], "mac_address"),
                                      cp_params["mac-address"])
        return RO_ns_params

    def scale_vnfr(self, db_vnfr, vdu_create=None, vdu_delete=None):
        # make a copy to do not change
        vdu_create = copy(vdu_create)
        vdu_delete = copy(vdu_delete)

        vdurs = db_vnfr.get("vdur")
        if vdurs is None:
            vdurs = []
        vdu_index = len(vdurs)
        while vdu_index:
            vdu_index -= 1
            vdur = vdurs[vdu_index]
            if vdur.get("pdu-type"):
                continue
            vdu_id_ref = vdur["vdu-id-ref"]
            if vdu_create and vdu_create.get(vdu_id_ref):
                for index in range(0, vdu_create[vdu_id_ref]):
                    vdur = deepcopy(vdur)
                    vdur["_id"] = str(uuid4())
                    vdur["count-index"] += 1
                    vdurs.insert(vdu_index+1+index, vdur)
                del vdu_create[vdu_id_ref]
            if vdu_delete and vdu_delete.get(vdu_id_ref):
                del vdurs[vdu_index]
                vdu_delete[vdu_id_ref] -= 1
                if not vdu_delete[vdu_id_ref]:
                    del vdu_delete[vdu_id_ref]
        # check all operations are done
        if vdu_create or vdu_delete:
            raise LcmException("Error scaling OUT VNFR for {}. There is not any existing vnfr. Scaled to 0?".format(
                vdu_create))
        if vdu_delete:
            raise LcmException("Error scaling IN VNFR for {}. There is not any existing vnfr. Scaled to 0?".format(
                vdu_delete))

        vnfr_update = {"vdur": vdurs}
        db_vnfr["vdur"] = vdurs
        self.update_db_2("vnfrs", db_vnfr["_id"], vnfr_update)

    def ns_update_nsr(self, ns_update_nsr, db_nsr, nsr_desc_RO):
        """
        Updates database nsr with the RO info for the created vld
        :param ns_update_nsr: dictionary to be filled with the updated info
        :param db_nsr: content of db_nsr. This is also modified
        :param nsr_desc_RO: nsr descriptor from RO
        :return: Nothing, LcmException is raised on errors
        """

        for vld_index, vld in enumerate(get_iterable(db_nsr, "vld")):
            for net_RO in get_iterable(nsr_desc_RO, "nets"):
                if vld["id"] != net_RO.get("ns_net_osm_id"):
                    continue
                vld["vim-id"] = net_RO.get("vim_net_id")
                vld["name"] = net_RO.get("vim_name")
                vld["status"] = net_RO.get("status")
                vld["status-detailed"] = net_RO.get("error_msg")
                ns_update_nsr["vld.{}".format(vld_index)] = vld
                break
            else:
                raise LcmException("ns_update_nsr: Not found vld={} at RO info".format(vld["id"]))

    def set_vnfr_at_error(self, db_vnfrs, error_text):
        try:
            for db_vnfr in db_vnfrs.values():
                vnfr_update = {"status": "ERROR"}
                for vdu_index, vdur in enumerate(get_iterable(db_vnfr, "vdur")):
                    if "status" not in vdur:
                        vdur["status"] = "ERROR"
                        vnfr_update["vdur.{}.status".format(vdu_index)] = "ERROR"
                        if error_text:
                            vdur["status-detailed"] = str(error_text)
                            vnfr_update["vdur.{}.status-detailed".format(vdu_index)] = "ERROR"
                self.update_db_2("vnfrs", db_vnfr["_id"], vnfr_update)
        except DbException as e:
            self.logger.error("Cannot update vnf. {}".format(e))

    def ns_update_vnfr(self, db_vnfrs, nsr_desc_RO):
        """
        Updates database vnfr with the RO info, e.g. ip_address, vim_id... Descriptor db_vnfrs is also updated
        :param db_vnfrs: dictionary with member-vnf-index: vnfr-content
        :param nsr_desc_RO: nsr descriptor from RO
        :return: Nothing, LcmException is raised on errors
        """
        for vnf_index, db_vnfr in db_vnfrs.items():
            for vnf_RO in nsr_desc_RO["vnfs"]:
                if vnf_RO["member_vnf_index"] != vnf_index:
                    continue
                vnfr_update = {}
                if vnf_RO.get("ip_address"):
                    db_vnfr["ip-address"] = vnfr_update["ip-address"] = vnf_RO["ip_address"].split(";")[0]
                elif not db_vnfr.get("ip-address"):
                    if db_vnfr.get("vdur"):   # if not VDUs, there is not ip_address
                        raise LcmExceptionNoMgmtIP("ns member_vnf_index '{}' has no IP address".format(vnf_index))

                for vdu_index, vdur in enumerate(get_iterable(db_vnfr, "vdur")):
                    vdur_RO_count_index = 0
                    if vdur.get("pdu-type"):
                        continue
                    for vdur_RO in get_iterable(vnf_RO, "vms"):
                        if vdur["vdu-id-ref"] != vdur_RO["vdu_osm_id"]:
                            continue
                        if vdur["count-index"] != vdur_RO_count_index:
                            vdur_RO_count_index += 1
                            continue
                        vdur["vim-id"] = vdur_RO.get("vim_vm_id")
                        if vdur_RO.get("ip_address"):
                            vdur["ip-address"] = vdur_RO["ip_address"].split(";")[0]
                        else:
                            vdur["ip-address"] = None
                        vdur["vdu-id-ref"] = vdur_RO.get("vdu_osm_id")
                        vdur["name"] = vdur_RO.get("vim_name")
                        vdur["status"] = vdur_RO.get("status")
                        vdur["status-detailed"] = vdur_RO.get("error_msg")
                        for ifacer in get_iterable(vdur, "interfaces"):
                            for interface_RO in get_iterable(vdur_RO, "interfaces"):
                                if ifacer["name"] == interface_RO.get("internal_name"):
                                    ifacer["ip-address"] = interface_RO.get("ip_address")
                                    ifacer["mac-address"] = interface_RO.get("mac_address")
                                    break
                            else:
                                raise LcmException("ns_update_vnfr: Not found member_vnf_index={} vdur={} interface={} "
                                                   "from VIM info"
                                                   .format(vnf_index, vdur["vdu-id-ref"], ifacer["name"]))
                        vnfr_update["vdur.{}".format(vdu_index)] = vdur
                        break
                    else:
                        raise LcmException("ns_update_vnfr: Not found member_vnf_index={} vdur={} count_index={} from "
                                           "VIM info".format(vnf_index, vdur["vdu-id-ref"], vdur["count-index"]))

                for vld_index, vld in enumerate(get_iterable(db_vnfr, "vld")):
                    for net_RO in get_iterable(nsr_desc_RO, "nets"):
                        if vld["id"] != net_RO.get("vnf_net_osm_id"):
                            continue
                        vld["vim-id"] = net_RO.get("vim_net_id")
                        vld["name"] = net_RO.get("vim_name")
                        vld["status"] = net_RO.get("status")
                        vld["status-detailed"] = net_RO.get("error_msg")
                        vnfr_update["vld.{}".format(vld_index)] = vld
                        break
                    else:
                        raise LcmException("ns_update_vnfr: Not found member_vnf_index={} vld={} from VIM info".format(
                            vnf_index, vld["id"]))

                self.update_db_2("vnfrs", db_vnfr["_id"], vnfr_update)
                break

            else:
                raise LcmException("ns_update_vnfr: Not found member_vnf_index={} from VIM info".format(vnf_index))

    def _get_ns_config_info(self, nsr_id):
        """
        Generates a mapping between vnf,vdu elements and the N2VC id
        :param nsr_id: id of nsr to get last  database _admin.deployed.VCA that contains this list
        :return: a dictionary with {osm-config-mapping: {}} where its element contains:
            "<member-vnf-index>": <N2VC-id>  for a vnf configuration, or
            "<member-vnf-index>.<vdu.id>.<vdu replica(0, 1,..)>": <N2VC-id>  for a vdu configuration
        """
        db_nsr = self.db.get_one("nsrs", {"_id": nsr_id})
        vca_deployed_list = db_nsr["_admin"]["deployed"]["VCA"]
        mapping = {}
        ns_config_info = {"osm-config-mapping": mapping}
        for vca in vca_deployed_list:
            if not vca["member-vnf-index"]:
                continue
            if not vca["vdu_id"]:
                mapping[vca["member-vnf-index"]] = vca["application"]
            else:
                mapping["{}.{}.{}".format(vca["member-vnf-index"], vca["vdu_id"], vca["vdu_count_index"])] =\
                    vca["application"]
        return ns_config_info

    @staticmethod
    def _get_initial_config_primitive_list(desc_primitive_list, vca_deployed):
        """
        Generates a list of initial-config-primitive based on the list provided by the descriptor. It includes internal
        primitives as verify-ssh-credentials, or config when needed
        :param desc_primitive_list: information of the descriptor
        :param vca_deployed: information of the deployed, needed for known if it is related to an NS, VNF, VDU and if
            this element contains a ssh public key
        :return: The modified list. Can ba an empty list, but always a list
        """
        if desc_primitive_list:
            primitive_list = desc_primitive_list.copy()
        else:
            primitive_list = []
        # look for primitive config, and get the position. None if not present
        config_position = None
        for index, primitive in enumerate(primitive_list):
            if primitive["name"] == "config":
                config_position = index
                break

        # for NS, add always a config primitive if not present (bug 874)
        if not vca_deployed["member-vnf-index"] and config_position is None:
            primitive_list.insert(0, {"name": "config", "parameter": []})
            config_position = 0
        # for VNF/VDU add verify-ssh-credentials after config
        if vca_deployed["member-vnf-index"] and config_position is not None and vca_deployed.get("ssh-public-key"):
            primitive_list.insert(config_position + 1, {"name": "verify-ssh-credentials", "parameter": []})
        return primitive_list

    async def instantiate_RO(self, logging_text, nsr_id, nsd, db_nsr, db_nslcmop, db_vnfrs, db_vnfds_ref,
                             n2vc_key_list, stage):
        try:
            db_nsr_update = {}
            RO_descriptor_number = 0   # number of descriptors created at RO
            vnf_index_2_RO_id = {}    # map between vnfd/nsd id to the id used at RO
            nslcmop_id = db_nslcmop["_id"]
            start_deploy = time()
            ns_params = db_nslcmop.get("operationParams")
            if ns_params and ns_params.get("timeout_ns_deploy"):
                timeout_ns_deploy = ns_params["timeout_ns_deploy"]
            else:
                timeout_ns_deploy = self.timeout.get("ns_deploy", self.timeout_ns_deploy)

            # Check for and optionally request placement optimization. Database will be updated if placement activated
            stage[2] = "Waiting for Placement."
            await self._do_placement(logging_text, db_nslcmop, db_vnfrs)

            # deploy RO

            # get vnfds, instantiate at RO
            for c_vnf in nsd.get("constituent-vnfd", ()):
                member_vnf_index = c_vnf["member-vnf-index"]
                vnfd = db_vnfds_ref[c_vnf['vnfd-id-ref']]
                vnfd_ref = vnfd["id"]

                stage[2] = "Creating vnfd='{}' member_vnf_index='{}' at RO".format(vnfd_ref, member_vnf_index)
                db_nsr_update["detailed-status"] = " ".join(stage)
                self.update_db_2("nsrs", nsr_id, db_nsr_update)
                self._write_op_status(nslcmop_id, stage)

                # self.logger.debug(logging_text + stage[2])
                vnfd_id_RO = "{}.{}.{}".format(nsr_id, RO_descriptor_number, member_vnf_index[:23])
                vnf_index_2_RO_id[member_vnf_index] = vnfd_id_RO
                RO_descriptor_number += 1

                # look position at deployed.RO.vnfd if not present it will be appended at the end
                for index, vnf_deployed in enumerate(db_nsr["_admin"]["deployed"]["RO"]["vnfd"]):
                    if vnf_deployed["member-vnf-index"] == member_vnf_index:
                        break
                else:
                    index = len(db_nsr["_admin"]["deployed"]["RO"]["vnfd"])
                    db_nsr["_admin"]["deployed"]["RO"]["vnfd"].append(None)

                # look if present
                RO_update = {"member-vnf-index": member_vnf_index}
                vnfd_list = await self.RO.get_list("vnfd", filter_by={"osm_id": vnfd_id_RO})
                if vnfd_list:
                    RO_update["id"] = vnfd_list[0]["uuid"]
                    self.logger.debug(logging_text + "vnfd='{}'  member_vnf_index='{}' exists at RO. Using RO_id={}".
                                      format(vnfd_ref, member_vnf_index, vnfd_list[0]["uuid"]))
                else:
                    vnfd_RO = self.vnfd2RO(vnfd, vnfd_id_RO, db_vnfrs[c_vnf["member-vnf-index"]].
                                           get("additionalParamsForVnf"), nsr_id)
                    desc = await self.RO.create("vnfd", descriptor=vnfd_RO)
                    RO_update["id"] = desc["uuid"]
                    self.logger.debug(logging_text + "vnfd='{}' member_vnf_index='{}' created at RO. RO_id={}".format(
                        vnfd_ref, member_vnf_index, desc["uuid"]))
                db_nsr_update["_admin.deployed.RO.vnfd.{}".format(index)] = RO_update
                db_nsr["_admin"]["deployed"]["RO"]["vnfd"][index] = RO_update

            # create nsd at RO
            nsd_ref = nsd["id"]

            stage[2] = "Creating nsd={} at RO".format(nsd_ref)
            db_nsr_update["detailed-status"] = " ".join(stage)
            self.update_db_2("nsrs", nsr_id, db_nsr_update)
            self._write_op_status(nslcmop_id, stage)

            # self.logger.debug(logging_text + stage[2])
            RO_osm_nsd_id = "{}.{}.{}".format(nsr_id, RO_descriptor_number, nsd_ref[:23])
            RO_descriptor_number += 1
            nsd_list = await self.RO.get_list("nsd", filter_by={"osm_id": RO_osm_nsd_id})
            if nsd_list:
                db_nsr_update["_admin.deployed.RO.nsd_id"] = RO_nsd_uuid = nsd_list[0]["uuid"]
                self.logger.debug(logging_text + "nsd={} exists at RO. Using RO_id={}".format(
                    nsd_ref, RO_nsd_uuid))
            else:
                nsd_RO = deepcopy(nsd)
                nsd_RO["id"] = RO_osm_nsd_id
                nsd_RO.pop("_id", None)
                nsd_RO.pop("_admin", None)
                for c_vnf in nsd_RO.get("constituent-vnfd", ()):
                    member_vnf_index = c_vnf["member-vnf-index"]
                    c_vnf["vnfd-id-ref"] = vnf_index_2_RO_id[member_vnf_index]
                for c_vld in nsd_RO.get("vld", ()):
                    for cp in c_vld.get("vnfd-connection-point-ref", ()):
                        member_vnf_index = cp["member-vnf-index-ref"]
                        cp["vnfd-id-ref"] = vnf_index_2_RO_id[member_vnf_index]

                desc = await self.RO.create("nsd", descriptor=nsd_RO)
                db_nsr_update["_admin.nsState"] = "INSTANTIATED"
                db_nsr_update["_admin.deployed.RO.nsd_id"] = RO_nsd_uuid = desc["uuid"]
                self.logger.debug(logging_text + "nsd={} created at RO. RO_id={}".format(nsd_ref, RO_nsd_uuid))
            self.update_db_2("nsrs", nsr_id, db_nsr_update)

            # Crate ns at RO
            stage[2] = "Creating nsd={} at RO".format(nsd_ref)
            db_nsr_update["detailed-status"] = " ".join(stage)
            self.update_db_2("nsrs", nsr_id, db_nsr_update)
            self._write_op_status(nslcmop_id, stage)

            # if present use it unless in error status
            RO_nsr_id = deep_get(db_nsr, ("_admin", "deployed", "RO", "nsr_id"))
            if RO_nsr_id:
                try:
                    stage[2] = "Looking for existing ns at RO"
                    db_nsr_update["detailed-status"] = " ".join(stage)
                    self.update_db_2("nsrs", nsr_id, db_nsr_update)
                    self._write_op_status(nslcmop_id, stage)
                    # self.logger.debug(logging_text + stage[2] + " RO_ns_id={}".format(RO_nsr_id))
                    desc = await self.RO.show("ns", RO_nsr_id)

                except ROclient.ROClientException as e:
                    if e.http_code != HTTPStatus.NOT_FOUND:
                        raise
                    RO_nsr_id = db_nsr_update["_admin.deployed.RO.nsr_id"] = None
                if RO_nsr_id:
                    ns_status, ns_status_info = self.RO.check_ns_status(desc)
                    db_nsr_update["_admin.deployed.RO.nsr_status"] = ns_status
                    if ns_status == "ERROR":
                        stage[2] = "Deleting ns at RO. RO_ns_id={}".format(RO_nsr_id)
                        self.logger.debug(logging_text + stage[2])
                        await self.RO.delete("ns", RO_nsr_id)
                        RO_nsr_id = db_nsr_update["_admin.deployed.RO.nsr_id"] = None
            if not RO_nsr_id:
                stage[2] = "Checking dependencies"
                db_nsr_update["detailed-status"] = " ".join(stage)
                self.update_db_2("nsrs", nsr_id, db_nsr_update)
                self._write_op_status(nslcmop_id, stage)
                # self.logger.debug(logging_text + stage[2])

                # check if VIM is creating and wait  look if previous tasks in process
                task_name, task_dependency = self.lcm_tasks.lookfor_related("vim_account", ns_params["vimAccountId"])
                if task_dependency:
                    stage[2] = "Waiting for related tasks '{}' to be completed".format(task_name)
                    self.logger.debug(logging_text + stage[2])
                    await asyncio.wait(task_dependency, timeout=3600)
                if ns_params.get("vnf"):
                    for vnf in ns_params["vnf"]:
                        if "vimAccountId" in vnf:
                            task_name, task_dependency = self.lcm_tasks.lookfor_related("vim_account",
                                                                                        vnf["vimAccountId"])
                        if task_dependency:
                            stage[2] = "Waiting for related tasks '{}' to be completed.".format(task_name)
                            self.logger.debug(logging_text + stage[2])
                            await asyncio.wait(task_dependency, timeout=3600)

                stage[2] = "Checking instantiation parameters."
                RO_ns_params = self._ns_params_2_RO(ns_params, nsd, db_vnfds_ref, n2vc_key_list)
                stage[2] = "Deploying ns at VIM."
                db_nsr_update["detailed-status"] = " ".join(stage)
                self.update_db_2("nsrs", nsr_id, db_nsr_update)
                self._write_op_status(nslcmop_id, stage)

                desc = await self.RO.create("ns", descriptor=RO_ns_params, name=db_nsr["name"], scenario=RO_nsd_uuid)
                RO_nsr_id = db_nsr_update["_admin.deployed.RO.nsr_id"] = desc["uuid"]
                db_nsr_update["_admin.nsState"] = "INSTANTIATED"
                db_nsr_update["_admin.deployed.RO.nsr_status"] = "BUILD"
                self.logger.debug(logging_text + "ns created at RO. RO_id={}".format(desc["uuid"]))

            # wait until NS is ready
            stage[2] = "Waiting VIM to deploy ns."
            db_nsr_update["detailed-status"] = " ".join(stage)
            self.update_db_2("nsrs", nsr_id, db_nsr_update)
            self._write_op_status(nslcmop_id, stage)
            detailed_status_old = None
            self.logger.debug(logging_text + stage[2] + " RO_ns_id={}".format(RO_nsr_id))

            old_desc = None
            while time() <= start_deploy + timeout_ns_deploy:
                desc = await self.RO.show("ns", RO_nsr_id)

                # deploymentStatus
                if desc != old_desc:
                    # desc has changed => update db
                    self._on_update_ro_db(nsrs_id=nsr_id, ro_descriptor=desc)
                    old_desc = desc

                    ns_status, ns_status_info = self.RO.check_ns_status(desc)
                    db_nsr_update["_admin.deployed.RO.nsr_status"] = ns_status
                    if ns_status == "ERROR":
                        raise ROclient.ROClientException(ns_status_info)
                    elif ns_status == "BUILD":
                        stage[2] = "VIM: ({})".format(ns_status_info)
                    elif ns_status == "ACTIVE":
                        stage[2] = "Waiting for management IP address reported by the VIM. Updating VNFRs."
                        try:
                            self.ns_update_vnfr(db_vnfrs, desc)
                            break
                        except LcmExceptionNoMgmtIP:
                            pass
                    else:
                        assert False, "ROclient.check_ns_status returns unknown {}".format(ns_status)
                    if stage[2] != detailed_status_old:
                        detailed_status_old = stage[2]
                        db_nsr_update["detailed-status"] = " ".join(stage)
                        self.update_db_2("nsrs", nsr_id, db_nsr_update)
                        self._write_op_status(nslcmop_id, stage)
                    await asyncio.sleep(5, loop=self.loop)
            else:  # timeout_ns_deploy
                raise ROclient.ROClientException("Timeout waiting ns to be ready")

            # Updating NSR
            self.ns_update_nsr(db_nsr_update, db_nsr, desc)

            db_nsr_update["_admin.deployed.RO.operational-status"] = "running"
            # db_nsr["_admin.deployed.RO.detailed-status"] = "Deployed at VIM"
            stage[2] = "Deployed at VIM"
            db_nsr_update["detailed-status"] = " ".join(stage)
            self.update_db_2("nsrs", nsr_id, db_nsr_update)
            self._write_op_status(nslcmop_id, stage)
            # await self._on_update_n2vc_db("nsrs", {"_id": nsr_id}, "_admin.deployed", db_nsr_update)
            # self.logger.debug(logging_text + "Deployed at VIM")
        except (ROclient.ROClientException, LcmException, DbException) as e:
            stage[2] = "ERROR deploying at VIM"
            self.set_vnfr_at_error(db_vnfrs, str(e))
            raise

    async def wait_vm_up_insert_key_ro(self, logging_text, nsr_id, vnfr_id, vdu_id, vdu_index, pub_key=None, user=None):
        """
        Wait for ip addres at RO, and optionally, insert public key in virtual machine
        :param logging_text: prefix use for logging
        :param nsr_id:
        :param vnfr_id:
        :param vdu_id:
        :param vdu_index:
        :param pub_key: public ssh key to inject, None to skip
        :param user: user to apply the public ssh key
        :return: IP address
        """

        # self.logger.debug(logging_text + "Starting wait_vm_up_insert_key_ro")
        ro_nsr_id = None
        ip_address = None
        nb_tries = 0
        target_vdu_id = None
        ro_retries = 0

        while True:

            ro_retries += 1
            if ro_retries >= 360:  # 1 hour
                raise LcmException("Not found _admin.deployed.RO.nsr_id for nsr_id: {}".format(nsr_id))

            await asyncio.sleep(10, loop=self.loop)

            # get ip address
            if not target_vdu_id:
                db_vnfr = self.db.get_one("vnfrs", {"_id": vnfr_id})

                if not vdu_id:  # for the VNF case
                    if db_vnfr.get("status") == "ERROR":
                        raise LcmException("Cannot inject ssh-key because target VNF is in error state")
                    ip_address = db_vnfr.get("ip-address")
                    if not ip_address:
                        continue
                    vdur = next((x for x in get_iterable(db_vnfr, "vdur") if x.get("ip-address") == ip_address), None)
                else:  # VDU case
                    vdur = next((x for x in get_iterable(db_vnfr, "vdur")
                                 if x.get("vdu-id-ref") == vdu_id and x.get("count-index") == vdu_index), None)

                if not vdur and len(db_vnfr.get("vdur", ())) == 1:  # If only one, this should be the target vdu
                    vdur = db_vnfr["vdur"][0]
                if not vdur:
                    raise LcmException("Not found vnfr_id={}, vdu_id={}, vdu_index={}".format(vnfr_id, vdu_id,
                                                                                              vdu_index))

                if vdur.get("pdu-type") or vdur.get("status") == "ACTIVE":
                    ip_address = vdur.get("ip-address")
                    if not ip_address:
                        continue
                    target_vdu_id = vdur["vdu-id-ref"]
                elif vdur.get("status") == "ERROR":
                    raise LcmException("Cannot inject ssh-key because target VM is in error state")

            if not target_vdu_id:
                continue

            # inject public key into machine
            if pub_key and user:
                # wait until NS is deployed at RO
                if not ro_nsr_id:
                    db_nsrs = self.db.get_one("nsrs", {"_id": nsr_id})
                    ro_nsr_id = deep_get(db_nsrs, ("_admin", "deployed", "RO", "nsr_id"))
                if not ro_nsr_id:
                    continue

                # self.logger.debug(logging_text + "Inserting RO key")
                if vdur.get("pdu-type"):
                    self.logger.error(logging_text + "Cannot inject ssh-ky to a PDU")
                    return ip_address
                try:
                    ro_vm_id = "{}-{}".format(db_vnfr["member-vnf-index-ref"], target_vdu_id)  # TODO add vdu_index
                    result_dict = await self.RO.create_action(
                        item="ns",
                        item_id_name=ro_nsr_id,
                        descriptor={"add_public_key": pub_key, "vms": [ro_vm_id], "user": user}
                    )
                    # result_dict contains the format {VM-id: {vim_result: 200, description: text}}
                    if not result_dict or not isinstance(result_dict, dict):
                        raise LcmException("Unknown response from RO when injecting key")
                    for result in result_dict.values():
                        if result.get("vim_result") == 200:
                            break
                        else:
                            raise ROclient.ROClientException("error injecting key: {}".format(
                                result.get("description")))
                    break
                except ROclient.ROClientException as e:
                    if not nb_tries:
                        self.logger.debug(logging_text + "error injecting key: {}. Retrying until {} seconds".
                                          format(e, 20*10))
                    nb_tries += 1
                    if nb_tries >= 20:
                        raise LcmException("Reaching max tries injecting key. Error: {}".format(e))
            else:
                break

        return ip_address

    async def _wait_dependent_n2vc(self, nsr_id, vca_deployed_list, vca_index):
        """
        Wait until dependent VCA deployments have been finished. NS wait for VNFs and VDUs. VNFs for VDUs
        """
        my_vca = vca_deployed_list[vca_index]
        if my_vca.get("vdu_id") or my_vca.get("kdu_name"):
            # vdu or kdu: no dependencies
            return
        timeout = 300
        while timeout >= 0:
            db_nsr = self.db.get_one("nsrs", {"_id": nsr_id})
            vca_deployed_list = db_nsr["_admin"]["deployed"]["VCA"]
            configuration_status_list = db_nsr["configurationStatus"]
            for index, vca_deployed in enumerate(configuration_status_list):
                if index == vca_index:
                    # myself
                    continue
                if not my_vca.get("member-vnf-index") or \
                        (vca_deployed.get("member-vnf-index") == my_vca.get("member-vnf-index")):
                    internal_status = configuration_status_list[index].get("status")
                    if internal_status == 'READY':
                        continue
                    elif internal_status == 'BROKEN':
                        raise LcmException("Configuration aborted because dependent charm/s has failed")
                    else:
                        break
            else:
                # no dependencies, return
                return
            await asyncio.sleep(10)
            timeout -= 1

        raise LcmException("Configuration aborted because dependent charm/s timeout")

    async def instantiate_N2VC(self, logging_text, vca_index, nsi_id, db_nsr, db_vnfr, vdu_id, kdu_name, vdu_index,
                               config_descriptor, deploy_params, base_folder, nslcmop_id, stage):
        nsr_id = db_nsr["_id"]
        db_update_entry = "_admin.deployed.VCA.{}.".format(vca_index)
        vca_deployed_list = db_nsr["_admin"]["deployed"]["VCA"]
        vca_deployed = db_nsr["_admin"]["deployed"]["VCA"][vca_index]
        db_dict = {
            'collection': 'nsrs',
            'filter': {'_id': nsr_id},
            'path': db_update_entry
        }
        step = ""
        try:

            element_type = 'NS'
            element_under_configuration = nsr_id

            vnfr_id = None
            if db_vnfr:
                vnfr_id = db_vnfr["_id"]

            namespace = "{nsi}.{ns}".format(
                nsi=nsi_id if nsi_id else "",
                ns=nsr_id)

            if vnfr_id:
                element_type = 'VNF'
                element_under_configuration = vnfr_id
                namespace += ".{}".format(vnfr_id)
                if vdu_id:
                    namespace += ".{}-{}".format(vdu_id, vdu_index or 0)
                    element_type = 'VDU'
                    element_under_configuration = "{}-{}".format(vdu_id, vdu_index or 0)
                elif kdu_name:
                    namespace += ".{}".format(kdu_name)
                    element_type = 'KDU'
                    element_under_configuration = kdu_name

            # Get artifact path
            self.fs.sync()  # Sync from FSMongo
            artifact_path = "{}/{}/charms/{}".format(
                base_folder["folder"],
                base_folder["pkg-dir"],
                config_descriptor["juju"]["charm"]
            )

            is_proxy_charm = deep_get(config_descriptor, ('juju', 'charm')) is not None
            if deep_get(config_descriptor, ('juju', 'proxy')) is False:
                is_proxy_charm = False

            # n2vc_redesign STEP 3.1

            # find old ee_id if exists
            ee_id = vca_deployed.get("ee_id")

            # create or register execution environment in VCA
            if is_proxy_charm:

                self._write_configuration_status(
                    nsr_id=nsr_id,
                    vca_index=vca_index,
                    status='CREATING',
                    element_under_configuration=element_under_configuration,
                    element_type=element_type
                )

                step = "create execution environment"
                self.logger.debug(logging_text + step)
                ee_id, credentials = await self.n2vc.create_execution_environment(namespace=namespace,
                                                                                  reuse_ee_id=ee_id,
                                                                                  db_dict=db_dict)

            else:
                step = "Waiting to VM being up and getting IP address"
                self.logger.debug(logging_text + step)
                rw_mgmt_ip = await self.wait_vm_up_insert_key_ro(logging_text, nsr_id, vnfr_id, vdu_id, vdu_index,
                                                                 user=None, pub_key=None)
                credentials = {"hostname": rw_mgmt_ip}
                # get username
                username = deep_get(config_descriptor, ("config-access", "ssh-access", "default-user"))
                # TODO remove this when changes on IM regarding config-access:ssh-access:default-user were
                #  merged. Meanwhile let's get username from initial-config-primitive
                if not username and config_descriptor.get("initial-config-primitive"):
                    for config_primitive in config_descriptor["initial-config-primitive"]:
                        for param in config_primitive.get("parameter", ()):
                            if param["name"] == "ssh-username":
                                username = param["value"]
                                break
                if not username:
                    raise LcmException("Cannot determine the username neither with 'initial-config-promitive' nor with "
                                       "'config-access.ssh-access.default-user'")
                credentials["username"] = username
                # n2vc_redesign STEP 3.2

                self._write_configuration_status(
                    nsr_id=nsr_id,
                    vca_index=vca_index,
                    status='REGISTERING',
                    element_under_configuration=element_under_configuration,
                    element_type=element_type
                )

                step = "register execution environment {}".format(credentials)
                self.logger.debug(logging_text + step)
                ee_id = await self.n2vc.register_execution_environment(credentials=credentials, namespace=namespace,
                                                                       db_dict=db_dict)

            # for compatibility with MON/POL modules, the need model and application name at database
            # TODO ask to N2VC instead of assuming the format "model_name.application_name"
            ee_id_parts = ee_id.split('.')
            model_name = ee_id_parts[0]
            application_name = ee_id_parts[1]
            db_nsr_update = {db_update_entry + "model": model_name,
                             db_update_entry + "application": application_name,
                             db_update_entry + "ee_id": ee_id}

            # n2vc_redesign STEP 3.3

            step = "Install configuration Software"

            self._write_configuration_status(
                nsr_id=nsr_id,
                vca_index=vca_index,
                status='INSTALLING SW',
                element_under_configuration=element_under_configuration,
                element_type=element_type,
                other_update=db_nsr_update
            )

            # TODO check if already done
            self.logger.debug(logging_text + step)
            config = None
            if not is_proxy_charm:
                initial_config_primitive_list = config_descriptor.get('initial-config-primitive')
                if initial_config_primitive_list:
                    for primitive in initial_config_primitive_list:
                        if primitive["name"] == "config":
                            config = self._map_primitive_params(
                                primitive,
                                {},
                                deploy_params
                            )
                            break
            await self.n2vc.install_configuration_sw(
                ee_id=ee_id,
                artifact_path=artifact_path,
                db_dict=db_dict,
                config=config
            )

            # write in db flag of configuration_sw already installed
            self.update_db_2("nsrs", nsr_id, {db_update_entry + "config_sw_installed": True})

            # add relations for this VCA (wait for other peers related with this VCA)
            await self._add_vca_relations(logging_text=logging_text, nsr_id=nsr_id, vca_index=vca_index)

            # if SSH access is required, then get execution environment SSH public
            if is_proxy_charm:  # if native charm we have waited already to VM be UP
                pub_key = None
                user = None
                if deep_get(config_descriptor, ("config-access", "ssh-access", "required")):
                    # Needed to inject a ssh key
                    user = deep_get(config_descriptor, ("config-access", "ssh-access", "default-user"))
                    step = "Install configuration Software, getting public ssh key"
                    pub_key = await self.n2vc.get_ee_ssh_public__key(ee_id=ee_id, db_dict=db_dict)

                    step = "Insert public key into VM user={} ssh_key={}".format(user, pub_key)
                else:
                    step = "Waiting to VM being up and getting IP address"
                self.logger.debug(logging_text + step)

                # n2vc_redesign STEP 5.1
                # wait for RO (ip-address) Insert pub_key into VM
                if vnfr_id:
                    rw_mgmt_ip = await self.wait_vm_up_insert_key_ro(logging_text, nsr_id, vnfr_id, vdu_id, vdu_index,
                                                                     user=user, pub_key=pub_key)
                else:
                    rw_mgmt_ip = None   # This is for a NS configuration

                self.logger.debug(logging_text + ' VM_ip_address={}'.format(rw_mgmt_ip))

            # store rw_mgmt_ip in deploy params for later replacement
            deploy_params["rw_mgmt_ip"] = rw_mgmt_ip

            # n2vc_redesign STEP 6  Execute initial config primitive
            step = 'execute initial config primitive'
            initial_config_primitive_list = config_descriptor.get('initial-config-primitive')

            # sort initial config primitives by 'seq'
            if initial_config_primitive_list:
                try:
                    initial_config_primitive_list.sort(key=lambda val: int(val['seq']))
                except Exception as e:
                    self.logger.error(logging_text + step + ": " + str(e))
            else:
                self.logger.debug(logging_text + step + ": No initial-config-primitive")

            # add config if not present for NS charm
            initial_config_primitive_list = self._get_initial_config_primitive_list(initial_config_primitive_list,
                                                                                    vca_deployed)

            # wait for dependent primitives execution (NS -> VNF -> VDU)
            if initial_config_primitive_list:
                await self._wait_dependent_n2vc(nsr_id, vca_deployed_list, vca_index)

            # stage, in function of element type: vdu, kdu, vnf or ns
            my_vca = vca_deployed_list[vca_index]
            if my_vca.get("vdu_id") or my_vca.get("kdu_name"):
                # VDU or KDU
                stage[0] = 'Stage 3/5: running Day-1 primitives for VDU.'
            elif my_vca.get("member-vnf-index"):
                # VNF
                stage[0] = 'Stage 4/5: running Day-1 primitives for VNF.'
            else:
                # NS
                stage[0] = 'Stage 5/5: running Day-1 primitives for NS.'

            self._write_configuration_status(
                nsr_id=nsr_id,
                vca_index=vca_index,
                status='EXECUTING PRIMITIVE'
            )

            self._write_op_status(
                op_id=nslcmop_id,
                stage=stage
            )

            check_if_terminated_needed = True
            for initial_config_primitive in initial_config_primitive_list:
                # adding information on the vca_deployed if it is a NS execution environment
                if not vca_deployed["member-vnf-index"]:
                    deploy_params["ns_config_info"] = json.dumps(self._get_ns_config_info(nsr_id))
                # TODO check if already done
                primitive_params_ = self._map_primitive_params(initial_config_primitive, {}, deploy_params)

                step = "execute primitive '{}' params '{}'".format(initial_config_primitive["name"], primitive_params_)
                self.logger.debug(logging_text + step)
                await self.n2vc.exec_primitive(
                    ee_id=ee_id,
                    primitive_name=initial_config_primitive["name"],
                    params_dict=primitive_params_,
                    db_dict=db_dict
                )
                # Once some primitive has been exec, check and write at db if it needs to exec terminated primitives
                if check_if_terminated_needed:
                    if config_descriptor.get('terminate-config-primitive'):
                        self.update_db_2("nsrs", nsr_id, {db_update_entry + "needed_terminate": True})
                    check_if_terminated_needed = False

                # TODO register in database that primitive is done

            step = "instantiated at VCA"
            self.logger.debug(logging_text + step)

            self._write_configuration_status(
                nsr_id=nsr_id,
                vca_index=vca_index,
                status='READY'
            )

        except Exception as e:  # TODO not use Exception but N2VC exception
            # self.update_db_2("nsrs", nsr_id, {db_update_entry + "instantiation": "FAILED"})
            if not isinstance(e, (DbException, N2VCException, LcmException, asyncio.CancelledError)):
                self.logger.error("Exception while {} : {}".format(step, e), exc_info=True)
            self._write_configuration_status(
                nsr_id=nsr_id,
                vca_index=vca_index,
                status='BROKEN'
            )
            raise LcmException("{} {}".format(step, e)) from e

    def _write_ns_status(self, nsr_id: str, ns_state: str, current_operation: str, current_operation_id: str,
                         error_description: str = None, error_detail: str = None, other_update: dict = None):
        """
        Update db_nsr fields.
        :param nsr_id:
        :param ns_state:
        :param current_operation:
        :param current_operation_id:
        :param error_description:
        :param error_detail:
        :param other_update: Other required changes at database if provided, will be cleared
        :return:
        """
        try:
            db_dict = other_update or {}
            db_dict["_admin.nslcmop"] = current_operation_id    # for backward compatibility
            db_dict["_admin.current-operation"] = current_operation_id
            db_dict["_admin.operation-type"] = current_operation if current_operation != "IDLE" else None
            db_dict["currentOperation"] = current_operation
            db_dict["currentOperationID"] = current_operation_id
            db_dict["errorDescription"] = error_description
            db_dict["errorDetail"] = error_detail

            if ns_state:
                db_dict["nsState"] = ns_state
            self.update_db_2("nsrs", nsr_id, db_dict)
        except DbException as e:
            self.logger.warn('Error writing NS status, ns={}: {}'.format(nsr_id, e))

    def _write_op_status(self, op_id: str, stage: list = None, error_message: str = None, queuePosition: int = 0,
                         operation_state: str = None, other_update: dict = None):
        try:
            db_dict = other_update or {}
            db_dict['queuePosition'] = queuePosition
            if isinstance(stage, list):
                db_dict['stage'] = stage[0]
                db_dict['detailed-status'] = " ".join(stage)
            elif stage is not None:
                db_dict['stage'] = str(stage)

            if error_message is not None:
                db_dict['errorMessage'] = error_message
            if operation_state is not None:
                db_dict['operationState'] = operation_state
                db_dict["statusEnteredTime"] = time()
            self.update_db_2("nslcmops", op_id, db_dict)
        except DbException as e:
            self.logger.warn('Error writing OPERATION status for op_id: {} -> {}'.format(op_id, e))

    def _write_all_config_status(self, db_nsr: dict, status: str):
        try:
            nsr_id = db_nsr["_id"]
            # configurationStatus
            config_status = db_nsr.get('configurationStatus')
            if config_status:
                db_nsr_update = {"configurationStatus.{}.status".format(index): status for index, v in
                                 enumerate(config_status) if v}
                # update status
                self.update_db_2("nsrs", nsr_id, db_nsr_update)

        except DbException as e:
            self.logger.warn('Error writing all configuration status, ns={}: {}'.format(nsr_id, e))

    def _write_configuration_status(self, nsr_id: str, vca_index: int, status: str = None,
                                    element_under_configuration: str = None, element_type: str = None,
                                    other_update: dict = None):

        # self.logger.debug('_write_configuration_status(): vca_index={}, status={}'
        #                   .format(vca_index, status))

        try:
            db_path = 'configurationStatus.{}.'.format(vca_index)
            db_dict = other_update or {}
            if status:
                db_dict[db_path + 'status'] = status
            if element_under_configuration:
                db_dict[db_path + 'elementUnderConfiguration'] = element_under_configuration
            if element_type:
                db_dict[db_path + 'elementType'] = element_type
            self.update_db_2("nsrs", nsr_id, db_dict)
        except DbException as e:
            self.logger.warn('Error writing configuration status={}, ns={}, vca_index={}: {}'
                             .format(status, nsr_id, vca_index, e))

    async def _do_placement(self, logging_text, db_nslcmop, db_vnfrs):
        """
        Check and computes the placement, (vim account where to deploy). If it is decided by an external tool, it
        sends the request via kafka and wait until the result is wrote at database (nslcmops _admin.plca).
        Database is used because the result can be obtained from a different LCM worker in case of HA.
        :param logging_text: contains the prefix for logging, with the ns and nslcmop identifiers
        :param db_nslcmop: database content of nslcmop
        :param db_vnfrs: database content of vnfrs, indexed by member-vnf-index.
        :return: None. Modifies database vnfrs and parameter db_vnfr with the computed 'vim-account-id'
        """
        nslcmop_id = db_nslcmop['_id']
        placement_engine = deep_get(db_nslcmop, ('operationParams', 'placement-engine'))
        if placement_engine == "PLA":
            self.logger.debug(logging_text + "Invoke and wait for placement optimization")
            await self.msg.aiowrite("pla", "get_placement", {'nslcmopId': nslcmop_id}, loop=self.loop)
            db_poll_interval = 5
            wait = db_poll_interval * 10
            pla_result = None
            while not pla_result and wait >= 0:
                await asyncio.sleep(db_poll_interval)
                wait -= db_poll_interval
                db_nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_id})
                pla_result = deep_get(db_nslcmop, ('_admin', 'pla'))

            if not pla_result:
                raise LcmException("Placement timeout for nslcmopId={}".format(nslcmop_id))

            for pla_vnf in pla_result['vnf']:
                vnfr = db_vnfrs.get(pla_vnf['member-vnf-index'])
                if not pla_vnf.get('vimAccountId') or not vnfr:
                    continue
                self.db.set_one("vnfrs", {"_id": vnfr["_id"]}, {"vim-account-id": pla_vnf['vimAccountId']})
                # Modifies db_vnfrs
                vnfr["vim-account-id"] = pla_vnf['vimAccountId']
        return

    def update_nsrs_with_pla_result(self, params):
        try:
            nslcmop_id = deep_get(params, ('placement', 'nslcmopId'))
            self.update_db_2("nslcmops", nslcmop_id, {"_admin.pla": params.get('placement')})
        except Exception as e:
            self.logger.warn('Update failed for nslcmop_id={}:{}'.format(nslcmop_id, e))

    async def instantiate(self, nsr_id, nslcmop_id):
        """

        :param nsr_id: ns instance to deploy
        :param nslcmop_id: operation to run
        :return:
        """

        # Try to lock HA task here
        task_is_locked_by_me = self.lcm_tasks.lock_HA('ns', 'nslcmops', nslcmop_id)
        if not task_is_locked_by_me:
            self.logger.debug('instantiate() task is not locked by me, ns={}'.format(nsr_id))
            return

        logging_text = "Task ns={} instantiate={} ".format(nsr_id, nslcmop_id)
        self.logger.debug(logging_text + "Enter")

        # get all needed from database

        # database nsrs record
        db_nsr = None

        # database nslcmops record
        db_nslcmop = None

        # update operation on nsrs
        db_nsr_update = {}
        # update operation on nslcmops
        db_nslcmop_update = {}

        nslcmop_operation_state = None
        db_vnfrs = {}     # vnf's info indexed by member-index
        # n2vc_info = {}
        tasks_dict_info = {}  # from task to info text
        exc = None
        error_list = []
        stage = ['Stage 1/5: preparation of the environment.', "Waiting for previous operations to terminate.", ""]
        # ^ stage, step, VIM progress
        try:
            # wait for any previous tasks in process
            await self.lcm_tasks.waitfor_related_HA('ns', 'nslcmops', nslcmop_id)

            # STEP 0: Reading database (nslcmops, nsrs, nsds, vnfrs, vnfds)
            stage[1] = "Reading from database,"
            # nsState="BUILDING", currentOperation="INSTANTIATING", currentOperationID=nslcmop_id
            db_nsr_update["detailed-status"] = "creating"
            db_nsr_update["operational-status"] = "init"
            self._write_ns_status(
                nsr_id=nsr_id,
                ns_state="BUILDING",
                current_operation="INSTANTIATING",
                current_operation_id=nslcmop_id,
                other_update=db_nsr_update
            )
            self._write_op_status(
                op_id=nslcmop_id,
                stage=stage,
                queuePosition=0
            )

            # read from db: operation
            stage[1] = "Getting nslcmop={} from db".format(nslcmop_id)
            db_nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_id})
            ns_params = db_nslcmop.get("operationParams")
            if ns_params and ns_params.get("timeout_ns_deploy"):
                timeout_ns_deploy = ns_params["timeout_ns_deploy"]
            else:
                timeout_ns_deploy = self.timeout.get("ns_deploy", self.timeout_ns_deploy)

            # read from db: ns
            stage[1] = "Getting nsr={} from db".format(nsr_id)
            db_nsr = self.db.get_one("nsrs", {"_id": nsr_id})
            # nsd is replicated into ns (no db read)
            nsd = db_nsr["nsd"]
            # nsr_name = db_nsr["name"]   # TODO short-name??

            # read from db: vnf's of this ns
            stage[1] = "Getting vnfrs from db"
            self.logger.debug(logging_text + stage[1])
            db_vnfrs_list = self.db.get_list("vnfrs", {"nsr-id-ref": nsr_id})

            # read from db: vnfd's for every vnf
            db_vnfds_ref = {}     # every vnfd data indexed by vnf name
            db_vnfds = {}         # every vnfd data indexed by vnf id
            db_vnfds_index = {}   # every vnfd data indexed by vnf member-index

            # for each vnf in ns, read vnfd
            for vnfr in db_vnfrs_list:
                db_vnfrs[vnfr["member-vnf-index-ref"]] = vnfr   # vnf's dict indexed by member-index: '1', '2', etc
                vnfd_id = vnfr["vnfd-id"]                       # vnfd uuid for this vnf
                vnfd_ref = vnfr["vnfd-ref"]                     # vnfd name for this vnf
                # if we haven't this vnfd, read it from db
                if vnfd_id not in db_vnfds:
                    # read from db
                    stage[1] = "Getting vnfd={} id='{}' from db".format(vnfd_id, vnfd_ref)
                    self.logger.debug(logging_text + stage[1])
                    vnfd = self.db.get_one("vnfds", {"_id": vnfd_id})

                    # store vnfd
                    db_vnfds_ref[vnfd_ref] = vnfd     # vnfd's indexed by name
                    db_vnfds[vnfd_id] = vnfd          # vnfd's indexed by id
                db_vnfds_index[vnfr["member-vnf-index-ref"]] = db_vnfds[vnfd_id]  # vnfd's indexed by member-index

            # Get or generates the _admin.deployed.VCA list
            vca_deployed_list = None
            if db_nsr["_admin"].get("deployed"):
                vca_deployed_list = db_nsr["_admin"]["deployed"].get("VCA")
            if vca_deployed_list is None:
                vca_deployed_list = []
                configuration_status_list = []
                db_nsr_update["_admin.deployed.VCA"] = vca_deployed_list
                db_nsr_update["configurationStatus"] = configuration_status_list
                # add _admin.deployed.VCA to db_nsr dictionary, value=vca_deployed_list
                populate_dict(db_nsr, ("_admin", "deployed", "VCA"), vca_deployed_list)
            elif isinstance(vca_deployed_list, dict):
                # maintain backward compatibility. Change a dict to list at database
                vca_deployed_list = list(vca_deployed_list.values())
                db_nsr_update["_admin.deployed.VCA"] = vca_deployed_list
                populate_dict(db_nsr, ("_admin", "deployed", "VCA"), vca_deployed_list)

            if not isinstance(deep_get(db_nsr, ("_admin", "deployed", "RO", "vnfd")), list):
                populate_dict(db_nsr, ("_admin", "deployed", "RO", "vnfd"), [])
                db_nsr_update["_admin.deployed.RO.vnfd"] = []

            # set state to INSTANTIATED. When instantiated NBI will not delete directly
            db_nsr_update["_admin.nsState"] = "INSTANTIATED"
            self.update_db_2("nsrs", nsr_id, db_nsr_update)

            # n2vc_redesign STEP 2 Deploy Network Scenario
            stage[0] = 'Stage 2/5: deployment of KDUs, VMs and execution environments.'
            self._write_op_status(
                op_id=nslcmop_id,
                stage=stage
            )

            stage[1] = "Deploying KDUs,"
            # self.logger.debug(logging_text + "Before deploy_kdus")
            # Call to deploy_kdus in case exists the "vdu:kdu" param
            await self.deploy_kdus(
                logging_text=logging_text,
                nsr_id=nsr_id,
                nslcmop_id=nslcmop_id,
                db_vnfrs=db_vnfrs,
                db_vnfds=db_vnfds,
                task_instantiation_info=tasks_dict_info,
            )

            stage[1] = "Getting VCA public key."
            # n2vc_redesign STEP 1 Get VCA public ssh-key
            # feature 1429. Add n2vc public key to needed VMs
            n2vc_key = self.n2vc.get_public_key()
            n2vc_key_list = [n2vc_key]
            if self.vca_config.get("public_key"):
                n2vc_key_list.append(self.vca_config["public_key"])

            stage[1] = "Deploying NS at VIM."
            task_ro = asyncio.ensure_future(
                self.instantiate_RO(
                    logging_text=logging_text,
                    nsr_id=nsr_id,
                    nsd=nsd,
                    db_nsr=db_nsr,
                    db_nslcmop=db_nslcmop,
                    db_vnfrs=db_vnfrs,
                    db_vnfds_ref=db_vnfds_ref,
                    n2vc_key_list=n2vc_key_list,
                    stage=stage
                )
            )
            self.lcm_tasks.register("ns", nsr_id, nslcmop_id, "instantiate_RO", task_ro)
            tasks_dict_info[task_ro] = "Deploying at VIM"

            # n2vc_redesign STEP 3 to 6 Deploy N2VC
            stage[1] = "Deploying Execution Environments."
            self.logger.debug(logging_text + stage[1])

            nsi_id = None  # TODO put nsi_id when this nsr belongs to a NSI
            # get_iterable() returns a value from a dict or empty tuple if key does not exist
            for c_vnf in get_iterable(nsd, "constituent-vnfd"):
                vnfd_id = c_vnf["vnfd-id-ref"]
                vnfd = db_vnfds_ref[vnfd_id]
                member_vnf_index = str(c_vnf["member-vnf-index"])
                db_vnfr = db_vnfrs[member_vnf_index]
                base_folder = vnfd["_admin"]["storage"]
                vdu_id = None
                vdu_index = 0
                vdu_name = None
                kdu_name = None

                # Get additional parameters
                deploy_params = {}
                if db_vnfr.get("additionalParamsForVnf"):
                    deploy_params = self._format_additional_params(db_vnfr["additionalParamsForVnf"].copy())

                descriptor_config = vnfd.get("vnf-configuration")
                if descriptor_config and descriptor_config.get("juju"):
                    self._deploy_n2vc(
                        logging_text=logging_text + "member_vnf_index={} ".format(member_vnf_index),
                        db_nsr=db_nsr,
                        db_vnfr=db_vnfr,
                        nslcmop_id=nslcmop_id,
                        nsr_id=nsr_id,
                        nsi_id=nsi_id,
                        vnfd_id=vnfd_id,
                        vdu_id=vdu_id,
                        kdu_name=kdu_name,
                        member_vnf_index=member_vnf_index,
                        vdu_index=vdu_index,
                        vdu_name=vdu_name,
                        deploy_params=deploy_params,
                        descriptor_config=descriptor_config,
                        base_folder=base_folder,
                        task_instantiation_info=tasks_dict_info,
                        stage=stage
                    )

                # Deploy charms for each VDU that supports one.
                for vdud in get_iterable(vnfd, 'vdu'):
                    vdu_id = vdud["id"]
                    descriptor_config = vdud.get('vdu-configuration')
                    vdur = next((x for x in db_vnfr["vdur"] if x["vdu-id-ref"] == vdu_id), None)
                    if vdur.get("additionalParams"):
                        deploy_params_vdu = self._format_additional_params(vdur["additionalParams"])
                    else:
                        deploy_params_vdu = deploy_params
                    if descriptor_config and descriptor_config.get("juju"):
                        # look for vdu index in the db_vnfr["vdu"] section
                        # for vdur_index, vdur in enumerate(db_vnfr["vdur"]):
                        #     if vdur["vdu-id-ref"] == vdu_id:
                        #         break
                        # else:
                        #     raise LcmException("Mismatch vdu_id={} not found in the vnfr['vdur'] list for "
                        #                        "member_vnf_index={}".format(vdu_id, member_vnf_index))
                        # vdu_name = vdur.get("name")
                        vdu_name = None
                        kdu_name = None
                        for vdu_index in range(int(vdud.get("count", 1))):
                            # TODO vnfr_params["rw_mgmt_ip"] = vdur["ip-address"]
                            self._deploy_n2vc(
                                logging_text=logging_text + "member_vnf_index={}, vdu_id={}, vdu_index={} ".format(
                                    member_vnf_index, vdu_id, vdu_index),
                                db_nsr=db_nsr,
                                db_vnfr=db_vnfr,
                                nslcmop_id=nslcmop_id,
                                nsr_id=nsr_id,
                                nsi_id=nsi_id,
                                vnfd_id=vnfd_id,
                                vdu_id=vdu_id,
                                kdu_name=kdu_name,
                                member_vnf_index=member_vnf_index,
                                vdu_index=vdu_index,
                                vdu_name=vdu_name,
                                deploy_params=deploy_params_vdu,
                                descriptor_config=descriptor_config,
                                base_folder=base_folder,
                                task_instantiation_info=tasks_dict_info,
                                stage=stage
                            )
                for kdud in get_iterable(vnfd, 'kdu'):
                    kdu_name = kdud["name"]
                    descriptor_config = kdud.get('kdu-configuration')
                    if descriptor_config and descriptor_config.get("juju"):
                        vdu_id = None
                        vdu_index = 0
                        vdu_name = None
                        # look for vdu index in the db_vnfr["vdu"] section
                        # for vdur_index, vdur in enumerate(db_vnfr["vdur"]):
                        #     if vdur["vdu-id-ref"] == vdu_id:
                        #         break
                        # else:
                        #     raise LcmException("Mismatch vdu_id={} not found in the vnfr['vdur'] list for "
                        #                        "member_vnf_index={}".format(vdu_id, member_vnf_index))
                        # vdu_name = vdur.get("name")
                        # vdu_name = None

                        self._deploy_n2vc(
                            logging_text=logging_text,
                            db_nsr=db_nsr,
                            db_vnfr=db_vnfr,
                            nslcmop_id=nslcmop_id,
                            nsr_id=nsr_id,
                            nsi_id=nsi_id,
                            vnfd_id=vnfd_id,
                            vdu_id=vdu_id,
                            kdu_name=kdu_name,
                            member_vnf_index=member_vnf_index,
                            vdu_index=vdu_index,
                            vdu_name=vdu_name,
                            deploy_params=deploy_params,
                            descriptor_config=descriptor_config,
                            base_folder=base_folder,
                            task_instantiation_info=tasks_dict_info,
                            stage=stage
                        )

            # Check if this NS has a charm configuration
            descriptor_config = nsd.get("ns-configuration")
            if descriptor_config and descriptor_config.get("juju"):
                vnfd_id = None
                db_vnfr = None
                member_vnf_index = None
                vdu_id = None
                kdu_name = None
                vdu_index = 0
                vdu_name = None

                # Get additional parameters
                deploy_params = {}
                if db_nsr.get("additionalParamsForNs"):
                    deploy_params = self._format_additional_params(db_nsr["additionalParamsForNs"].copy())
                base_folder = nsd["_admin"]["storage"]
                self._deploy_n2vc(
                    logging_text=logging_text,
                    db_nsr=db_nsr,
                    db_vnfr=db_vnfr,
                    nslcmop_id=nslcmop_id,
                    nsr_id=nsr_id,
                    nsi_id=nsi_id,
                    vnfd_id=vnfd_id,
                    vdu_id=vdu_id,
                    kdu_name=kdu_name,
                    member_vnf_index=member_vnf_index,
                    vdu_index=vdu_index,
                    vdu_name=vdu_name,
                    deploy_params=deploy_params,
                    descriptor_config=descriptor_config,
                    base_folder=base_folder,
                    task_instantiation_info=tasks_dict_info,
                    stage=stage
                )

            # rest of staff will be done at finally

        except (ROclient.ROClientException, DbException, LcmException, N2VCException) as e:
            self.logger.error(logging_text + "Exit Exception while '{}': {}".format(stage[1], e))
            exc = e
        except asyncio.CancelledError:
            self.logger.error(logging_text + "Cancelled Exception while '{}'".format(stage[1]))
            exc = "Operation was cancelled"
        except Exception as e:
            exc = traceback.format_exc()
            self.logger.critical(logging_text + "Exit Exception while '{}': {}".format(stage[1], e), exc_info=True)
        finally:
            if exc:
                error_list.append(str(exc))
            try:
                # wait for pending tasks
                if tasks_dict_info:
                    stage[1] = "Waiting for instantiate pending tasks."
                    self.logger.debug(logging_text + stage[1])
                    error_list += await self._wait_for_tasks(logging_text, tasks_dict_info, timeout_ns_deploy,
                                                             stage, nslcmop_id, nsr_id=nsr_id)
                stage[1] = stage[2] = ""
            except asyncio.CancelledError:
                error_list.append("Cancelled")
                # TODO cancel all tasks
            except Exception as exc:
                error_list.append(str(exc))

            # update operation-status
            db_nsr_update["operational-status"] = "running"
            # let's begin with VCA 'configured' status (later we can change it)
            db_nsr_update["config-status"] = "configured"
            for task, task_name in tasks_dict_info.items():
                if not task.done() or task.cancelled() or task.exception():
                    if task_name.startswith(self.task_name_deploy_vca):
                        # A N2VC task is pending
                        db_nsr_update["config-status"] = "failed"
                    else:
                        # RO or KDU task is pending
                        db_nsr_update["operational-status"] = "failed"

            # update status at database
            if error_list:
                error_detail = ". ".join(error_list)
                self.logger.error(logging_text + error_detail)
                error_description_nslcmop = 'Stage: {}. Detail: {}'.format(stage[0], error_detail)
                error_description_nsr = 'Operation: INSTANTIATING.{}, Stage {}'.format(nslcmop_id, stage[0])

                db_nsr_update["detailed-status"] = error_description_nsr + " Detail: " + error_detail
                db_nslcmop_update["detailed-status"] = error_detail
                nslcmop_operation_state = "FAILED"
                ns_state = "BROKEN"
            else:
                error_detail = None
                error_description_nsr = error_description_nslcmop = None
                ns_state = "READY"
                db_nsr_update["detailed-status"] = "Done"
                db_nslcmop_update["detailed-status"] = "Done"
                nslcmop_operation_state = "COMPLETED"

            if db_nsr:
                self._write_ns_status(
                    nsr_id=nsr_id,
                    ns_state=ns_state,
                    current_operation="IDLE",
                    current_operation_id=None,
                    error_description=error_description_nsr,
                    error_detail=error_detail,
                    other_update=db_nsr_update
                )
            if db_nslcmop:
                self._write_op_status(
                    op_id=nslcmop_id,
                    stage="",
                    error_message=error_description_nslcmop,
                    operation_state=nslcmop_operation_state,
                    other_update=db_nslcmop_update,
                )

            if nslcmop_operation_state:
                try:
                    await self.msg.aiowrite("ns", "instantiated", {"nsr_id": nsr_id, "nslcmop_id": nslcmop_id,
                                                                   "operationState": nslcmop_operation_state},
                                            loop=self.loop)
                except Exception as e:
                    self.logger.error(logging_text + "kafka_write notification Exception {}".format(e))

            self.logger.debug(logging_text + "Exit")
            self.lcm_tasks.remove("ns", nsr_id, nslcmop_id, "ns_instantiate")

    async def _add_vca_relations(self, logging_text, nsr_id, vca_index: int, timeout: int = 3600) -> bool:

        # steps:
        # 1. find all relations for this VCA
        # 2. wait for other peers related
        # 3. add relations

        try:

            # STEP 1: find all relations for this VCA

            # read nsr record
            db_nsr = self.db.get_one("nsrs", {"_id": nsr_id})

            # this VCA data
            my_vca = deep_get(db_nsr, ('_admin', 'deployed', 'VCA'))[vca_index]

            # read all ns-configuration relations
            ns_relations = list()
            db_ns_relations = deep_get(db_nsr, ('nsd', 'ns-configuration', 'relation'))
            if db_ns_relations:
                for r in db_ns_relations:
                    # check if this VCA is in the relation
                    if my_vca.get('member-vnf-index') in\
                            (r.get('entities')[0].get('id'), r.get('entities')[1].get('id')):
                        ns_relations.append(r)

            # read all vnf-configuration relations
            vnf_relations = list()
            db_vnfd_list = db_nsr.get('vnfd-id')
            if db_vnfd_list:
                for vnfd in db_vnfd_list:
                    db_vnfd = self.db.get_one("vnfds", {"_id": vnfd})
                    db_vnf_relations = deep_get(db_vnfd, ('vnf-configuration', 'relation'))
                    if db_vnf_relations:
                        for r in db_vnf_relations:
                            # check if this VCA is in the relation
                            if my_vca.get('vdu_id') in (r.get('entities')[0].get('id'), r.get('entities')[1].get('id')):
                                vnf_relations.append(r)

            # if no relations, terminate
            if not ns_relations and not vnf_relations:
                self.logger.debug(logging_text + ' No relations')
                return True

            self.logger.debug(logging_text + ' adding relations\n    {}\n    {}'.format(ns_relations, vnf_relations))

            # add all relations
            start = time()
            while True:
                # check timeout
                now = time()
                if now - start >= timeout:
                    self.logger.error(logging_text + ' : timeout adding relations')
                    return False

                # reload nsr from database (we need to update record: _admin.deloyed.VCA)
                db_nsr = self.db.get_one("nsrs", {"_id": nsr_id})

                # for each defined NS relation, find the VCA's related
                for r in ns_relations:
                    from_vca_ee_id = None
                    to_vca_ee_id = None
                    from_vca_endpoint = None
                    to_vca_endpoint = None
                    vca_list = deep_get(db_nsr, ('_admin', 'deployed', 'VCA'))
                    for vca in vca_list:
                        if vca.get('member-vnf-index') == r.get('entities')[0].get('id') \
                                and vca.get('config_sw_installed'):
                            from_vca_ee_id = vca.get('ee_id')
                            from_vca_endpoint = r.get('entities')[0].get('endpoint')
                        if vca.get('member-vnf-index') == r.get('entities')[1].get('id') \
                                and vca.get('config_sw_installed'):
                            to_vca_ee_id = vca.get('ee_id')
                            to_vca_endpoint = r.get('entities')[1].get('endpoint')
                    if from_vca_ee_id and to_vca_ee_id:
                        # add relation
                        await self.n2vc.add_relation(
                            ee_id_1=from_vca_ee_id,
                            ee_id_2=to_vca_ee_id,
                            endpoint_1=from_vca_endpoint,
                            endpoint_2=to_vca_endpoint)
                        # remove entry from relations list
                        ns_relations.remove(r)
                    else:
                        # check failed peers
                        try:
                            vca_status_list = db_nsr.get('configurationStatus')
                            if vca_status_list:
                                for i in range(len(vca_list)):
                                    vca = vca_list[i]
                                    vca_status = vca_status_list[i]
                                    if vca.get('member-vnf-index') == r.get('entities')[0].get('id'):
                                        if vca_status.get('status') == 'BROKEN':
                                            # peer broken: remove relation from list
                                            ns_relations.remove(r)
                                    if vca.get('member-vnf-index') == r.get('entities')[1].get('id'):
                                        if vca_status.get('status') == 'BROKEN':
                                            # peer broken: remove relation from list
                                            ns_relations.remove(r)
                        except Exception:
                            # ignore
                            pass

                # for each defined VNF relation, find the VCA's related
                for r in vnf_relations:
                    from_vca_ee_id = None
                    to_vca_ee_id = None
                    from_vca_endpoint = None
                    to_vca_endpoint = None
                    vca_list = deep_get(db_nsr, ('_admin', 'deployed', 'VCA'))
                    for vca in vca_list:
                        if vca.get('vdu_id') == r.get('entities')[0].get('id') and vca.get('config_sw_installed'):
                            from_vca_ee_id = vca.get('ee_id')
                            from_vca_endpoint = r.get('entities')[0].get('endpoint')
                        if vca.get('vdu_id') == r.get('entities')[1].get('id') and vca.get('config_sw_installed'):
                            to_vca_ee_id = vca.get('ee_id')
                            to_vca_endpoint = r.get('entities')[1].get('endpoint')
                    if from_vca_ee_id and to_vca_ee_id:
                        # add relation
                        await self.n2vc.add_relation(
                            ee_id_1=from_vca_ee_id,
                            ee_id_2=to_vca_ee_id,
                            endpoint_1=from_vca_endpoint,
                            endpoint_2=to_vca_endpoint)
                        # remove entry from relations list
                        vnf_relations.remove(r)
                    else:
                        # check failed peers
                        try:
                            vca_status_list = db_nsr.get('configurationStatus')
                            if vca_status_list:
                                for i in range(len(vca_list)):
                                    vca = vca_list[i]
                                    vca_status = vca_status_list[i]
                                    if vca.get('vdu_id') == r.get('entities')[0].get('id'):
                                        if vca_status.get('status') == 'BROKEN':
                                            # peer broken: remove relation from list
                                            ns_relations.remove(r)
                                    if vca.get('vdu_id') == r.get('entities')[1].get('id'):
                                        if vca_status.get('status') == 'BROKEN':
                                            # peer broken: remove relation from list
                                            ns_relations.remove(r)
                        except Exception:
                            # ignore
                            pass

                # wait for next try
                await asyncio.sleep(5.0)

                if not ns_relations and not vnf_relations:
                    self.logger.debug('Relations added')
                    break

            return True

        except Exception as e:
            self.logger.warn(logging_text + ' ERROR adding relations: {}'.format(e))
            return False

    def _write_db_callback(self, task, item, _id, on_done=None, on_exc=None):
        """
        callback for kdu install intended to store the returned kdu_instance at database
        :return: None
        """
        db_update = {}
        try:
            result = task.result()
            if on_done:
                db_update[on_done] = str(result)
        except Exception as e:
            if on_exc:
                db_update[on_exc] = str(e)
        if db_update:
            try:
                self.update_db_2(item, _id, db_update)
            except Exception:
                pass

    async def deploy_kdus(self, logging_text, nsr_id, nslcmop_id, db_vnfrs, db_vnfds, task_instantiation_info):
        # Launch kdus if present in the descriptor

        k8scluster_id_2_uuic = {"helm-chart": {}, "juju-bundle": {}}

        def _get_cluster_id(cluster_id, cluster_type):
            nonlocal k8scluster_id_2_uuic
            if cluster_id in k8scluster_id_2_uuic[cluster_type]:
                return k8scluster_id_2_uuic[cluster_type][cluster_id]

            db_k8scluster = self.db.get_one("k8sclusters", {"_id": cluster_id}, fail_on_empty=False)
            if not db_k8scluster:
                raise LcmException("K8s cluster {} cannot be found".format(cluster_id))
            k8s_id = deep_get(db_k8scluster, ("_admin", cluster_type, "id"))
            if not k8s_id:
                raise LcmException("K8s cluster '{}' has not been initilized for '{}'".format(cluster_id, cluster_type))
            k8scluster_id_2_uuic[cluster_type][cluster_id] = k8s_id
            return k8s_id

        logging_text += "Deploy kdus: "
        step = ""
        try:
            db_nsr_update = {"_admin.deployed.K8s": []}
            self.update_db_2("nsrs", nsr_id, db_nsr_update)

            index = 0
            updated_cluster_list = []

            for vnfr_data in db_vnfrs.values():
                for kdur in get_iterable(vnfr_data, "kdur"):
                    desc_params = self._format_additional_params(kdur.get("additionalParams"))
                    vnfd_id = vnfr_data.get('vnfd-id')
                    namespace = kdur.get("k8s-namespace")
                    if kdur.get("helm-chart"):
                        kdumodel = kdur["helm-chart"]
                        k8sclustertype = "helm-chart"
                    elif kdur.get("juju-bundle"):
                        kdumodel = kdur["juju-bundle"]
                        k8sclustertype = "juju-bundle"
                    else:
                        raise LcmException("kdu type for kdu='{}.{}' is neither helm-chart nor "
                                           "juju-bundle. Maybe an old NBI version is running".
                                           format(vnfr_data["member-vnf-index-ref"], kdur["kdu-name"]))
                    # check if kdumodel is a file and exists
                    try:
                        storage = deep_get(db_vnfds.get(vnfd_id), ('_admin', 'storage'))
                        if storage and storage.get('pkg-dir'):  # may be not present if vnfd has not artifacts
                            # path format: /vnfdid/pkkdir/helm-charts|juju-bundles/kdumodel
                            filename = '{}/{}/{}s/{}'.format(storage["folder"], storage["'pkg-dir"], k8sclustertype,
                                                             kdumodel)
                            if self.fs.file_exists(filename, mode='file') or self.fs.file_exists(filename, mode='dir'):
                                kdumodel = self.fs.path + filename
                    except (asyncio.TimeoutError, asyncio.CancelledError):
                        raise
                    except Exception:       # it is not a file
                        pass

                    k8s_cluster_id = kdur["k8s-cluster"]["id"]
                    step = "Synchronize repos for k8s cluster '{}'".format(k8s_cluster_id)
                    cluster_uuid = _get_cluster_id(k8s_cluster_id, k8sclustertype)

                    if k8sclustertype == "helm-chart" and cluster_uuid not in updated_cluster_list:
                        del_repo_list, added_repo_dict = await asyncio.ensure_future(
                            self.k8sclusterhelm.synchronize_repos(cluster_uuid=cluster_uuid))
                        if del_repo_list or added_repo_dict:
                            unset = {'_admin.helm_charts_added.' + item: None for item in del_repo_list}
                            updated = {'_admin.helm_charts_added.' +
                                       item: name for item, name in added_repo_dict.items()}
                            self.logger.debug(logging_text + "repos synchronized on k8s cluster '{}' to_delete: {}, "
                                                             "to_add: {}".format(k8s_cluster_id, del_repo_list,
                                                                                 added_repo_dict))
                            self.db.set_one("k8sclusters", {"_id": k8s_cluster_id}, updated, unset=unset)
                        updated_cluster_list.append(cluster_uuid)

                    step = "Instantiating KDU {}.{} in k8s cluster {}".format(vnfr_data["member-vnf-index-ref"],
                                                                              kdur["kdu-name"], k8s_cluster_id)

                    k8s_instace_info = {"kdu-instance": None,
                                        "k8scluster-uuid": cluster_uuid,
                                        "k8scluster-type": k8sclustertype,
                                        "member-vnf-index": vnfr_data["member-vnf-index-ref"],
                                        "kdu-name": kdur["kdu-name"],
                                        "kdu-model": kdumodel,
                                        "namespace": namespace}
                    db_path = "_admin.deployed.K8s.{}".format(index)
                    db_nsr_update[db_path] = k8s_instace_info
                    self.update_db_2("nsrs", nsr_id, db_nsr_update)

                    db_dict = {"collection": "nsrs",
                               "filter": {"_id": nsr_id},
                               "path": db_path}

                    task = asyncio.ensure_future(
                        self.k8scluster_map[k8sclustertype].install(cluster_uuid=cluster_uuid, kdu_model=kdumodel,
                                                                    atomic=True, params=desc_params,
                                                                    db_dict=db_dict, timeout=600,
                                                                    kdu_name=kdur["kdu-name"], namespace=namespace))

                    task.add_done_callback(partial(self._write_db_callback, item="nsrs", _id=nsr_id,
                                                   on_done=db_path + ".kdu-instance",
                                                   on_exc=db_path + ".detailed-status"))
                    self.lcm_tasks.register("ns", nsr_id, nslcmop_id, "instantiate_KDU-{}".format(index), task)
                    task_instantiation_info[task] = "Deploying KDU {}".format(kdur["kdu-name"])

                    index += 1

        except (LcmException, asyncio.CancelledError):
            raise
        except Exception as e:
            msg = "Exception {} while {}: {}".format(type(e).__name__, step, e)
            if isinstance(e, (N2VCException, DbException)):
                self.logger.error(logging_text + msg)
            else:
                self.logger.critical(logging_text + msg, exc_info=True)
            raise LcmException(msg)
        finally:
            if db_nsr_update:
                self.update_db_2("nsrs", nsr_id, db_nsr_update)

    def _deploy_n2vc(self, logging_text, db_nsr, db_vnfr, nslcmop_id, nsr_id, nsi_id, vnfd_id, vdu_id,
                     kdu_name, member_vnf_index, vdu_index, vdu_name, deploy_params, descriptor_config,
                     base_folder, task_instantiation_info, stage):
        # launch instantiate_N2VC in a asyncio task and register task object
        # Look where information of this charm is at database <nsrs>._admin.deployed.VCA
        # if not found, create one entry and update database

        # fill db_nsr._admin.deployed.VCA.<index>
        vca_index = -1
        for vca_index, vca_deployed in enumerate(db_nsr["_admin"]["deployed"]["VCA"]):
            if not vca_deployed:
                continue
            if vca_deployed.get("member-vnf-index") == member_vnf_index and \
                    vca_deployed.get("vdu_id") == vdu_id and \
                    vca_deployed.get("kdu_name") == kdu_name and \
                    vca_deployed.get("vdu_count_index", 0) == vdu_index:
                break
        else:
            # not found, create one.
            vca_deployed = {
                "member-vnf-index": member_vnf_index,
                "vdu_id": vdu_id,
                "kdu_name": kdu_name,
                "vdu_count_index": vdu_index,
                "operational-status": "init",  # TODO revise
                "detailed-status": "",  # TODO revise
                "step": "initial-deploy",   # TODO revise
                "vnfd_id": vnfd_id,
                "vdu_name": vdu_name,
            }
            vca_index += 1

            # create VCA and configurationStatus in db
            db_dict = {
                "_admin.deployed.VCA.{}".format(vca_index): vca_deployed,
                "configurationStatus.{}".format(vca_index): dict()
            }
            self.update_db_2("nsrs", nsr_id, db_dict)

            db_nsr["_admin"]["deployed"]["VCA"].append(vca_deployed)

        # Launch task
        task_n2vc = asyncio.ensure_future(
            self.instantiate_N2VC(
                logging_text=logging_text,
                vca_index=vca_index,
                nsi_id=nsi_id,
                db_nsr=db_nsr,
                db_vnfr=db_vnfr,
                vdu_id=vdu_id,
                kdu_name=kdu_name,
                vdu_index=vdu_index,
                deploy_params=deploy_params,
                config_descriptor=descriptor_config,
                base_folder=base_folder,
                nslcmop_id=nslcmop_id,
                stage=stage
            )
        )
        self.lcm_tasks.register("ns", nsr_id, nslcmop_id, "instantiate_N2VC-{}".format(vca_index), task_n2vc)
        task_instantiation_info[task_n2vc] = self.task_name_deploy_vca + " {}.{}".format(
            member_vnf_index or "", vdu_id or "")

    # Check if this VNFD has a configured terminate action
    def _has_terminate_config_primitive(self, vnfd):
        vnf_config = vnfd.get("vnf-configuration")
        if vnf_config and vnf_config.get("terminate-config-primitive"):
            return True
        else:
            return False

    @staticmethod
    def _get_terminate_config_primitive_seq_list(vnfd):
        """ Get a numerically sorted list of the sequences for this VNFD's terminate action """
        # No need to check for existing primitive twice, already done before
        vnf_config = vnfd.get("vnf-configuration")
        seq_list = vnf_config.get("terminate-config-primitive")
        # Get all 'seq' tags in seq_list, order sequences numerically, ascending.
        seq_list_sorted = sorted(seq_list, key=lambda x: int(x['seq']))
        return seq_list_sorted

    @staticmethod
    def _create_nslcmop(nsr_id, operation, params):
        """
        Creates a ns-lcm-opp content to be stored at database.
        :param nsr_id: internal id of the instance
        :param operation: instantiate, terminate, scale, action, ...
        :param params: user parameters for the operation
        :return: dictionary following SOL005 format
        """
        # Raise exception if invalid arguments
        if not (nsr_id and operation and params):
            raise LcmException(
                "Parameters 'nsr_id', 'operation' and 'params' needed to create primitive not provided")
        now = time()
        _id = str(uuid4())
        nslcmop = {
            "id": _id,
            "_id": _id,
            # COMPLETED,PARTIALLY_COMPLETED,FAILED_TEMP,FAILED,ROLLING_BACK,ROLLED_BACK
            "operationState": "PROCESSING",
            "statusEnteredTime": now,
            "nsInstanceId": nsr_id,
            "lcmOperationType": operation,
            "startTime": now,
            "isAutomaticInvocation": False,
            "operationParams": params,
            "isCancelPending": False,
            "links": {
                "self": "/osm/nslcm/v1/ns_lcm_op_occs/" + _id,
                "nsInstance": "/osm/nslcm/v1/ns_instances/" + nsr_id,
            }
        }
        return nslcmop

    def _format_additional_params(self, params):
        params = params or {}
        for key, value in params.items():
            if str(value).startswith("!!yaml "):
                params[key] = yaml.safe_load(value[7:])
        return params

    def _get_terminate_primitive_params(self, seq, vnf_index):
        primitive = seq.get('name')
        primitive_params = {}
        params = {
            "member_vnf_index": vnf_index,
            "primitive": primitive,
            "primitive_params": primitive_params,
        }
        desc_params = {}
        return self._map_primitive_params(seq, params, desc_params)

    # sub-operations

    def _retry_or_skip_suboperation(self, db_nslcmop, op_index):
        op = deep_get(db_nslcmop, ('_admin', 'operations'), [])[op_index]
        if op.get('operationState') == 'COMPLETED':
            # b. Skip sub-operation
            # _ns_execute_primitive() or RO.create_action() will NOT be executed
            return self.SUBOPERATION_STATUS_SKIP
        else:
            # c. Reintent executing sub-operation
            # The sub-operation exists, and operationState != 'COMPLETED'
            # Update operationState = 'PROCESSING' to indicate a reintent.
            operationState = 'PROCESSING'
            detailed_status = 'In progress'
            self._update_suboperation_status(
                db_nslcmop, op_index, operationState, detailed_status)
            # Return the sub-operation index
            # _ns_execute_primitive() or RO.create_action() will be called from scale()
            # with arguments extracted from the sub-operation
            return op_index

    # Find a sub-operation where all keys in a matching dictionary must match
    # Returns the index of the matching sub-operation, or SUBOPERATION_STATUS_NOT_FOUND if no match
    def _find_suboperation(self, db_nslcmop, match):
        if (db_nslcmop and match):
            op_list = db_nslcmop.get('_admin', {}).get('operations', [])
            for i, op in enumerate(op_list):
                if all(op.get(k) == match[k] for k in match):
                    return i
        return self.SUBOPERATION_STATUS_NOT_FOUND

    # Update status for a sub-operation given its index
    def _update_suboperation_status(self, db_nslcmop, op_index, operationState, detailed_status):
        # Update DB for HA tasks
        q_filter = {'_id': db_nslcmop['_id']}
        update_dict = {'_admin.operations.{}.operationState'.format(op_index): operationState,
                       '_admin.operations.{}.detailed-status'.format(op_index): detailed_status}
        self.db.set_one("nslcmops",
                        q_filter=q_filter,
                        update_dict=update_dict,
                        fail_on_empty=False)

    # Add sub-operation, return the index of the added sub-operation
    # Optionally, set operationState, detailed-status, and operationType
    # Status and type are currently set for 'scale' sub-operations:
    # 'operationState' : 'PROCESSING' | 'COMPLETED' | 'FAILED'
    # 'detailed-status' : status message
    # 'operationType': may be any type, in the case of scaling: 'PRE-SCALE' | 'POST-SCALE'
    # Status and operation type are currently only used for 'scale', but NOT for 'terminate' sub-operations.
    def _add_suboperation(self, db_nslcmop, vnf_index, vdu_id, vdu_count_index, vdu_name, primitive, 
                          mapped_primitive_params, operationState=None, detailed_status=None, operationType=None,
                          RO_nsr_id=None, RO_scaling_info=None):
        if not db_nslcmop:
            return self.SUBOPERATION_STATUS_NOT_FOUND
        # Get the "_admin.operations" list, if it exists
        db_nslcmop_admin = db_nslcmop.get('_admin', {})
        op_list = db_nslcmop_admin.get('operations')
        # Create or append to the "_admin.operations" list
        new_op = {'member_vnf_index': vnf_index,
                  'vdu_id': vdu_id,
                  'vdu_count_index': vdu_count_index,
                  'primitive': primitive,
                  'primitive_params': mapped_primitive_params}
        if operationState:
            new_op['operationState'] = operationState
        if detailed_status:
            new_op['detailed-status'] = detailed_status
        if operationType:
            new_op['lcmOperationType'] = operationType
        if RO_nsr_id:
            new_op['RO_nsr_id'] = RO_nsr_id
        if RO_scaling_info:
            new_op['RO_scaling_info'] = RO_scaling_info
        if not op_list:
            # No existing operations, create key 'operations' with current operation as first list element
            db_nslcmop_admin.update({'operations': [new_op]})
            op_list = db_nslcmop_admin.get('operations')
        else:
            # Existing operations, append operation to list
            op_list.append(new_op)

        db_nslcmop_update = {'_admin.operations': op_list}
        self.update_db_2("nslcmops", db_nslcmop['_id'], db_nslcmop_update)
        op_index = len(op_list) - 1
        return op_index

    # Helper methods for scale() sub-operations

    # pre-scale/post-scale:
    # Check for 3 different cases:
    # a. New: First time execution, return SUBOPERATION_STATUS_NEW
    # b. Skip: Existing sub-operation exists, operationState == 'COMPLETED', return SUBOPERATION_STATUS_SKIP
    # c. Reintent: Existing sub-operation exists, operationState != 'COMPLETED', return op_index to re-execute
    def _check_or_add_scale_suboperation(self, db_nslcmop, vnf_index, vnf_config_primitive, primitive_params,
                                         operationType, RO_nsr_id=None, RO_scaling_info=None):
        # Find this sub-operation
        if (RO_nsr_id and RO_scaling_info):
            operationType = 'SCALE-RO'
            match = {
                'member_vnf_index': vnf_index,
                'RO_nsr_id': RO_nsr_id,
                'RO_scaling_info': RO_scaling_info,
            }
        else:
            match = {
                'member_vnf_index': vnf_index,
                'primitive': vnf_config_primitive,
                'primitive_params': primitive_params,
                'lcmOperationType': operationType
            }
        op_index = self._find_suboperation(db_nslcmop, match)
        if op_index == self.SUBOPERATION_STATUS_NOT_FOUND:
            # a. New sub-operation
            # The sub-operation does not exist, add it.
            # _ns_execute_primitive() will be called from scale() as usual, with non-modified arguments
            # The following parameters are set to None for all kind of scaling:
            vdu_id = None
            vdu_count_index = None
            vdu_name = None
            if RO_nsr_id and RO_scaling_info:
                vnf_config_primitive = None
                primitive_params = None
            else:
                RO_nsr_id = None
                RO_scaling_info = None
            # Initial status for sub-operation
            operationState = 'PROCESSING'
            detailed_status = 'In progress'
            # Add sub-operation for pre/post-scaling (zero or more operations)
            self._add_suboperation(db_nslcmop,
                                   vnf_index,
                                   vdu_id,
                                   vdu_count_index,
                                   vdu_name,
                                   vnf_config_primitive,
                                   primitive_params,
                                   operationState,
                                   detailed_status,
                                   operationType,
                                   RO_nsr_id,
                                   RO_scaling_info)
            return self.SUBOPERATION_STATUS_NEW
        else:
            # Return either SUBOPERATION_STATUS_SKIP (operationState == 'COMPLETED'),
            # or op_index (operationState != 'COMPLETED')
            return self._retry_or_skip_suboperation(db_nslcmop, op_index)

    # Function to return execution_environment id

    def _get_ee_id(self, vnf_index, vdu_id, vca_deployed_list):
        # TODO vdu_index_count
        for vca in vca_deployed_list:
            if vca["member-vnf-index"] == vnf_index and vca["vdu_id"] == vdu_id:
                return vca["ee_id"]

    async def destroy_N2VC(self, logging_text, db_nslcmop, vca_deployed, config_descriptor, vca_index, destroy_ee=True):
        """
        Execute the terminate primitives and destroy the execution environment (if destroy_ee=False
        :param logging_text:
        :param db_nslcmop:
        :param vca_deployed: Dictionary of deployment info at db_nsr._admin.depoloyed.VCA.<INDEX>
        :param config_descriptor: Configuration descriptor of the NSD, VNFD, VNFD.vdu or VNFD.kdu
        :param vca_index: index in the database _admin.deployed.VCA
        :param destroy_ee: False to do not destroy, because it will be destroyed all of then at once
        :return: None or exception
        """
        # execute terminate_primitives
        terminate_primitives = config_descriptor.get("terminate-config-primitive")
        vdu_id = vca_deployed.get("vdu_id")
        vdu_count_index = vca_deployed.get("vdu_count_index")
        vdu_name = vca_deployed.get("vdu_name")
        vnf_index = vca_deployed.get("member-vnf-index")
        if terminate_primitives and vca_deployed.get("needed_terminate"):
            # Get all 'seq' tags in seq_list, order sequences numerically, ascending.
            terminate_primitives = sorted(terminate_primitives, key=lambda x: int(x['seq']))
            for seq in terminate_primitives:
                # For each sequence in list, get primitive and call _ns_execute_primitive()
                step = "Calling terminate action for vnf_member_index={} primitive={}".format(
                    vnf_index, seq.get("name"))
                self.logger.debug(logging_text + step)
                # Create the primitive for each sequence, i.e. "primitive": "touch"
                primitive = seq.get('name')
                mapped_primitive_params = self._get_terminate_primitive_params(seq, vnf_index)
                # The following 3 parameters are currently set to None for 'terminate':
                # vdu_id, vdu_count_index, vdu_name

                # Add sub-operation
                self._add_suboperation(db_nslcmop,
                                       vnf_index,
                                       vdu_id,
                                       vdu_count_index,
                                       vdu_name,
                                       primitive,
                                       mapped_primitive_params)
                # Sub-operations: Call _ns_execute_primitive() instead of action()
                try:
                    result, result_detail = await self._ns_execute_primitive(vca_deployed["ee_id"], primitive,
                                                                             mapped_primitive_params)
                except LcmException:
                    # this happens when VCA is not deployed. In this case it is not needed to terminate
                    continue
                result_ok = ['COMPLETED', 'PARTIALLY_COMPLETED']
                if result not in result_ok:
                    raise LcmException("terminate_primitive {}  for vnf_member_index={} fails with "
                                       "error {}".format(seq.get("name"), vnf_index, result_detail))
            # set that this VCA do not need terminated
            db_update_entry = "_admin.deployed.VCA.{}.needed_terminate".format(vca_index)
            self.update_db_2("nsrs", db_nslcmop["nsInstanceId"], {db_update_entry: False})

        if destroy_ee:
            await self.n2vc.delete_execution_environment(vca_deployed["ee_id"])

    async def _delete_all_N2VC(self, db_nsr: dict):
        self._write_all_config_status(db_nsr=db_nsr, status='TERMINATING')
        namespace = "." + db_nsr["_id"]
        try:
            await self.n2vc.delete_namespace(namespace=namespace, total_timeout=self.timeout_charm_delete)
        except N2VCNotFound:  # already deleted. Skip
            pass
        self._write_all_config_status(db_nsr=db_nsr, status='DELETED')

    async def _terminate_RO(self, logging_text, nsr_deployed, nsr_id, nslcmop_id, stage):
        """
        Terminates a deployment from RO
        :param logging_text:
        :param nsr_deployed: db_nsr._admin.deployed
        :param nsr_id:
        :param nslcmop_id:
        :param stage: list of string with the content to write on db_nslcmop.detailed-status.
            this method will update only the index 2, but it will write on database the concatenated content of the list
        :return:
        """
        db_nsr_update = {}
        failed_detail = []
        ro_nsr_id = ro_delete_action = None
        if nsr_deployed and nsr_deployed.get("RO"):
            ro_nsr_id = nsr_deployed["RO"].get("nsr_id")
            ro_delete_action = nsr_deployed["RO"].get("nsr_delete_action_id")
        try:
            if ro_nsr_id:
                stage[2] = "Deleting ns from VIM."
                db_nsr_update["detailed-status"] = " ".join(stage)
                self._write_op_status(nslcmop_id, stage)
                self.logger.debug(logging_text + stage[2])
                self.update_db_2("nsrs", nsr_id, db_nsr_update)
                self._write_op_status(nslcmop_id, stage)
                desc = await self.RO.delete("ns", ro_nsr_id)
                ro_delete_action = desc["action_id"]
                db_nsr_update["_admin.deployed.RO.nsr_delete_action_id"] = ro_delete_action
                db_nsr_update["_admin.deployed.RO.nsr_id"] = None
                db_nsr_update["_admin.deployed.RO.nsr_status"] = "DELETED"
            if ro_delete_action:
                # wait until NS is deleted from VIM
                stage[2] = "Waiting ns deleted from VIM."
                detailed_status_old = None
                self.logger.debug(logging_text + stage[2] + " RO_id={} ro_delete_action={}".format(ro_nsr_id,
                                                                                                   ro_delete_action))
                self.update_db_2("nsrs", nsr_id, db_nsr_update)
                self._write_op_status(nslcmop_id, stage)

                delete_timeout = 20 * 60  # 20 minutes
                while delete_timeout > 0:
                    desc = await self.RO.show(
                        "ns",
                        item_id_name=ro_nsr_id,
                        extra_item="action",
                        extra_item_id=ro_delete_action)

                    # deploymentStatus
                    self._on_update_ro_db(nsrs_id=nsr_id, ro_descriptor=desc)

                    ns_status, ns_status_info = self.RO.check_action_status(desc)
                    if ns_status == "ERROR":
                        raise ROclient.ROClientException(ns_status_info)
                    elif ns_status == "BUILD":
                        stage[2] = "Deleting from VIM {}".format(ns_status_info)
                    elif ns_status == "ACTIVE":
                        db_nsr_update["_admin.deployed.RO.nsr_delete_action_id"] = None
                        db_nsr_update["_admin.deployed.RO.nsr_status"] = "DELETED"
                        break
                    else:
                        assert False, "ROclient.check_action_status returns unknown {}".format(ns_status)
                    if stage[2] != detailed_status_old:
                        detailed_status_old = stage[2]
                        db_nsr_update["detailed-status"] = " ".join(stage)
                        self._write_op_status(nslcmop_id, stage)
                        self.update_db_2("nsrs", nsr_id, db_nsr_update)
                    await asyncio.sleep(5, loop=self.loop)
                    delete_timeout -= 5
                else:  # delete_timeout <= 0:
                    raise ROclient.ROClientException("Timeout waiting ns deleted from VIM")

        except Exception as e:
            self.update_db_2("nsrs", nsr_id, db_nsr_update)
            if isinstance(e, ROclient.ROClientException) and e.http_code == 404:  # not found
                db_nsr_update["_admin.deployed.RO.nsr_id"] = None
                db_nsr_update["_admin.deployed.RO.nsr_status"] = "DELETED"
                db_nsr_update["_admin.deployed.RO.nsr_delete_action_id"] = None
                self.logger.debug(logging_text + "RO_ns_id={} already deleted".format(ro_nsr_id))
            elif isinstance(e, ROclient.ROClientException) and e.http_code == 409:  # conflict
                failed_detail.append("delete conflict: {}".format(e))
                self.logger.debug(logging_text + "RO_ns_id={} delete conflict: {}".format(ro_nsr_id, e))
            else:
                failed_detail.append("delete error: {}".format(e))
                self.logger.error(logging_text + "RO_ns_id={} delete error: {}".format(ro_nsr_id, e))

        # Delete nsd
        if not failed_detail and deep_get(nsr_deployed, ("RO", "nsd_id")):
            ro_nsd_id = nsr_deployed["RO"]["nsd_id"]
            try:
                stage[2] = "Deleting nsd from RO."
                db_nsr_update["detailed-status"] = " ".join(stage)
                self.update_db_2("nsrs", nsr_id, db_nsr_update)
                self._write_op_status(nslcmop_id, stage)
                await self.RO.delete("nsd", ro_nsd_id)
                self.logger.debug(logging_text + "ro_nsd_id={} deleted".format(ro_nsd_id))
                db_nsr_update["_admin.deployed.RO.nsd_id"] = None
            except Exception as e:
                if isinstance(e, ROclient.ROClientException) and e.http_code == 404:  # not found
                    db_nsr_update["_admin.deployed.RO.nsd_id"] = None
                    self.logger.debug(logging_text + "ro_nsd_id={} already deleted".format(ro_nsd_id))
                elif isinstance(e, ROclient.ROClientException) and e.http_code == 409:  # conflict
                    failed_detail.append("ro_nsd_id={} delete conflict: {}".format(ro_nsd_id, e))
                    self.logger.debug(logging_text + failed_detail[-1])
                else:
                    failed_detail.append("ro_nsd_id={} delete error: {}".format(ro_nsd_id, e))
                    self.logger.error(logging_text + failed_detail[-1])

        if not failed_detail and deep_get(nsr_deployed, ("RO", "vnfd")):
            for index, vnf_deployed in enumerate(nsr_deployed["RO"]["vnfd"]):
                if not vnf_deployed or not vnf_deployed["id"]:
                    continue
                try:
                    ro_vnfd_id = vnf_deployed["id"]
                    stage[2] = "Deleting member_vnf_index={} ro_vnfd_id={} from RO.".format(
                        vnf_deployed["member-vnf-index"], ro_vnfd_id)
                    db_nsr_update["detailed-status"] = " ".join(stage)
                    self.update_db_2("nsrs", nsr_id, db_nsr_update)
                    self._write_op_status(nslcmop_id, stage)
                    await self.RO.delete("vnfd", ro_vnfd_id)
                    self.logger.debug(logging_text + "ro_vnfd_id={} deleted".format(ro_vnfd_id))
                    db_nsr_update["_admin.deployed.RO.vnfd.{}.id".format(index)] = None
                except Exception as e:
                    if isinstance(e, ROclient.ROClientException) and e.http_code == 404:  # not found
                        db_nsr_update["_admin.deployed.RO.vnfd.{}.id".format(index)] = None
                        self.logger.debug(logging_text + "ro_vnfd_id={} already deleted ".format(ro_vnfd_id))
                    elif isinstance(e, ROclient.ROClientException) and e.http_code == 409:  # conflict
                        failed_detail.append("ro_vnfd_id={} delete conflict: {}".format(ro_vnfd_id, e))
                        self.logger.debug(logging_text + failed_detail[-1])
                    else:
                        failed_detail.append("ro_vnfd_id={} delete error: {}".format(ro_vnfd_id, e))
                        self.logger.error(logging_text + failed_detail[-1])

        if failed_detail:
            stage[2] = "Error deleting from VIM"
        else:
            stage[2] = "Deleted from VIM"
        db_nsr_update["detailed-status"] = " ".join(stage)
        self.update_db_2("nsrs", nsr_id, db_nsr_update)
        self._write_op_status(nslcmop_id, stage)

        if failed_detail:
            raise LcmException("; ".join(failed_detail))

    async def terminate(self, nsr_id, nslcmop_id):
        # Try to lock HA task here
        task_is_locked_by_me = self.lcm_tasks.lock_HA('ns', 'nslcmops', nslcmop_id)
        if not task_is_locked_by_me:
            return

        logging_text = "Task ns={} terminate={} ".format(nsr_id, nslcmop_id)
        self.logger.debug(logging_text + "Enter")
        timeout_ns_terminate = self.timeout_ns_terminate
        db_nsr = None
        db_nslcmop = None
        exc = None
        error_list = []   # annotates all failed error messages
        db_nslcmop_update = {}
        autoremove = False  # autoremove after terminated
        tasks_dict_info = {}
        db_nsr_update = {}
        stage = ["Stage 1/3: Preparing task.", "Waiting for previous operations to terminate.", ""]
        # ^ contains [stage, step, VIM-status]
        try:
            # wait for any previous tasks in process
            await self.lcm_tasks.waitfor_related_HA("ns", 'nslcmops', nslcmop_id)

            stage[1] = "Getting nslcmop={} from db.".format(nslcmop_id)
            db_nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_id})
            operation_params = db_nslcmop.get("operationParams") or {}
            if operation_params.get("timeout_ns_terminate"):
                timeout_ns_terminate = operation_params["timeout_ns_terminate"]
            stage[1] = "Getting nsr={} from db.".format(nsr_id)
            db_nsr = self.db.get_one("nsrs", {"_id": nsr_id})

            db_nsr_update["operational-status"] = "terminating"
            db_nsr_update["config-status"] = "terminating"
            self._write_ns_status(
                nsr_id=nsr_id,
                ns_state="TERMINATING",
                current_operation="TERMINATING",
                current_operation_id=nslcmop_id,
                other_update=db_nsr_update
            )
            self._write_op_status(
                op_id=nslcmop_id,
                queuePosition=0,
                stage=stage
            )
            nsr_deployed = deepcopy(db_nsr["_admin"].get("deployed")) or {}
            if db_nsr["_admin"]["nsState"] == "NOT_INSTANTIATED":
                return

            stage[1] = "Getting vnf descriptors from db."
            db_vnfrs_list = self.db.get_list("vnfrs", {"nsr-id-ref": nsr_id})
            db_vnfds_from_id = {}
            db_vnfds_from_member_index = {}
            # Loop over VNFRs
            for vnfr in db_vnfrs_list:
                vnfd_id = vnfr["vnfd-id"]
                if vnfd_id not in db_vnfds_from_id:
                    vnfd = self.db.get_one("vnfds", {"_id": vnfd_id})
                    db_vnfds_from_id[vnfd_id] = vnfd
                db_vnfds_from_member_index[vnfr["member-vnf-index-ref"]] = db_vnfds_from_id[vnfd_id]

            # Destroy individual execution environments when there are terminating primitives.
            # Rest of EE will be deleted at once
            if not operation_params.get("skip_terminate_primitives"):
                stage[0] = "Stage 2/3 execute terminating primitives."
                stage[1] = "Looking execution environment that needs terminate."
                self.logger.debug(logging_text + stage[1])
                for vca_index, vca in enumerate(get_iterable(nsr_deployed, "VCA")):
                    config_descriptor = None
                    if not vca or not vca.get("ee_id") or not vca.get("needed_terminate"):
                        continue
                    if not vca.get("member-vnf-index"):
                        # ns
                        config_descriptor = db_nsr.get("ns-configuration")
                    elif vca.get("vdu_id"):
                        db_vnfd = db_vnfds_from_member_index[vca["member-vnf-index"]]
                        vdud = next((vdu for vdu in db_vnfd.get("vdu", ()) if vdu["id"] == vca.get("vdu_id")), None)
                        if vdud:
                            config_descriptor = vdud.get("vdu-configuration")
                    elif vca.get("kdu_name"):
                        db_vnfd = db_vnfds_from_member_index[vca["member-vnf-index"]]
                        kdud = next((kdu for kdu in db_vnfd.get("kdu", ()) if kdu["name"] == vca.get("kdu_name")), None)
                        if kdud:
                            config_descriptor = kdud.get("kdu-configuration")
                    else:
                        config_descriptor = db_vnfds_from_member_index[vca["member-vnf-index"]].get("vnf-configuration")
                    task = asyncio.ensure_future(self.destroy_N2VC(logging_text, db_nslcmop, vca, config_descriptor,
                                                                   vca_index, False))
                    tasks_dict_info[task] = "Terminating VCA {}".format(vca.get("ee_id"))

                # wait for pending tasks of terminate primitives
                if tasks_dict_info:
                    self.logger.debug(logging_text + 'Waiting for terminate primitive pending tasks...')
                    error_list = await self._wait_for_tasks(logging_text, tasks_dict_info,
                                                            min(self.timeout_charm_delete, timeout_ns_terminate),
                                                            stage, nslcmop_id)
                    if error_list:
                        return   # raise LcmException("; ".join(error_list))
                    tasks_dict_info.clear()

            # remove All execution environments at once
            stage[0] = "Stage 3/3 delete all."

            if nsr_deployed.get("VCA"):
                stage[1] = "Deleting all execution environments."
                self.logger.debug(logging_text + stage[1])
                task_delete_ee = asyncio.ensure_future(asyncio.wait_for(self._delete_all_N2VC(db_nsr=db_nsr),
                                                                        timeout=self.timeout_charm_delete))
                # task_delete_ee = asyncio.ensure_future(self.n2vc.delete_namespace(namespace="." + nsr_id))
                tasks_dict_info[task_delete_ee] = "Terminating all VCA"

            # Delete from k8scluster
            stage[1] = "Deleting KDUs."
            self.logger.debug(logging_text + stage[1])
            # print(nsr_deployed)
            for kdu in get_iterable(nsr_deployed, "K8s"):
                if not kdu or not kdu.get("kdu-instance"):
                    continue
                kdu_instance = kdu.get("kdu-instance")
                if kdu.get("k8scluster-type") in self.k8scluster_map:
                    task_delete_kdu_instance = asyncio.ensure_future(
                        self.k8scluster_map[kdu["k8scluster-type"]].uninstall(
                            cluster_uuid=kdu.get("k8scluster-uuid"),
                            kdu_instance=kdu_instance))
                else:
                    self.logger.error(logging_text + "Unknown k8s deployment type {}".
                                      format(kdu.get("k8scluster-type")))
                    continue
                tasks_dict_info[task_delete_kdu_instance] = "Terminating KDU '{}'".format(kdu.get("kdu-name"))

            # remove from RO
            stage[1] = "Deleting ns from VIM."
            task_delete_ro = asyncio.ensure_future(
                self._terminate_RO(logging_text, nsr_deployed, nsr_id, nslcmop_id, stage))
            tasks_dict_info[task_delete_ro] = "Removing deployment from VIM"

            # rest of staff will be done at finally

        except (ROclient.ROClientException, DbException, LcmException, N2VCException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except asyncio.CancelledError:
            self.logger.error(logging_text + "Cancelled Exception while '{}'".format(stage[1]))
            exc = "Operation was cancelled"
        except Exception as e:
            exc = traceback.format_exc()
            self.logger.critical(logging_text + "Exit Exception while '{}': {}".format(stage[1], e), exc_info=True)
        finally:
            if exc:
                error_list.append(str(exc))
            try:
                # wait for pending tasks
                if tasks_dict_info:
                    stage[1] = "Waiting for terminate pending tasks."
                    self.logger.debug(logging_text + stage[1])
                    error_list += await self._wait_for_tasks(logging_text, tasks_dict_info, timeout_ns_terminate,
                                                             stage, nslcmop_id)
                stage[1] = stage[2] = ""
            except asyncio.CancelledError:
                error_list.append("Cancelled")
                # TODO cancell all tasks
            except Exception as exc:
                error_list.append(str(exc))
            # update status at database
            if error_list:
                error_detail = "; ".join(error_list)
                # self.logger.error(logging_text + error_detail)
                error_description_nslcmop = 'Stage: {}. Detail: {}'.format(stage[0], error_detail)
                error_description_nsr = 'Operation: TERMINATING.{}, Stage {}.'.format(nslcmop_id, stage[0])

                db_nsr_update["operational-status"] = "failed"
                db_nsr_update["detailed-status"] = error_description_nsr + " Detail: " + error_detail
                db_nslcmop_update["detailed-status"] = error_detail
                nslcmop_operation_state = "FAILED"
                ns_state = "BROKEN"
            else:
                error_detail = None
                error_description_nsr = error_description_nslcmop = None
                ns_state = "NOT_INSTANTIATED"
                db_nsr_update["operational-status"] = "terminated"
                db_nsr_update["detailed-status"] = "Done"
                db_nsr_update["_admin.nsState"] = "NOT_INSTANTIATED"
                db_nslcmop_update["detailed-status"] = "Done"
                nslcmop_operation_state = "COMPLETED"

            if db_nsr:
                self._write_ns_status(
                    nsr_id=nsr_id,
                    ns_state=ns_state,
                    current_operation="IDLE",
                    current_operation_id=None,
                    error_description=error_description_nsr,
                    error_detail=error_detail,
                    other_update=db_nsr_update
                )
            if db_nslcmop:
                self._write_op_status(
                    op_id=nslcmop_id,
                    stage="",
                    error_message=error_description_nslcmop,
                    operation_state=nslcmop_operation_state,
                    other_update=db_nslcmop_update,
                )
                autoremove = operation_params.get("autoremove", False)
            if nslcmop_operation_state:
                try:
                    await self.msg.aiowrite("ns", "terminated", {"nsr_id": nsr_id, "nslcmop_id": nslcmop_id,
                                                                 "operationState": nslcmop_operation_state,
                                                                 "autoremove": autoremove},
                                            loop=self.loop)
                except Exception as e:
                    self.logger.error(logging_text + "kafka_write notification Exception {}".format(e))

            self.logger.debug(logging_text + "Exit")
            self.lcm_tasks.remove("ns", nsr_id, nslcmop_id, "ns_terminate")

    async def _wait_for_tasks(self, logging_text, created_tasks_info, timeout, stage, nslcmop_id, nsr_id=None):
        time_start = time()
        error_detail_list = []
        error_list = []
        pending_tasks = list(created_tasks_info.keys())
        num_tasks = len(pending_tasks)
        num_done = 0
        stage[1] = "{}/{}.".format(num_done, num_tasks)
        self._write_op_status(nslcmop_id, stage)
        while pending_tasks:
            new_error = None
            _timeout = timeout + time_start - time()
            done, pending_tasks = await asyncio.wait(pending_tasks, timeout=_timeout,
                                                     return_when=asyncio.FIRST_COMPLETED)
            num_done += len(done)
            if not done:   # Timeout
                for task in pending_tasks:
                    new_error = created_tasks_info[task] + ": Timeout"
                    error_detail_list.append(new_error)
                    error_list.append(new_error)
                break
            for task in done:
                if task.cancelled():
                    exc = "Cancelled"
                else:
                    exc = task.exception()
                if exc:
                    if isinstance(exc, asyncio.TimeoutError):
                        exc = "Timeout"
                    new_error = created_tasks_info[task] + ": {}".format(exc)
                    error_list.append(created_tasks_info[task])
                    error_detail_list.append(new_error)
                    if isinstance(exc, (str, DbException, N2VCException, ROclient.ROClientException, LcmException,
                                        K8sException)):
                        self.logger.error(logging_text + new_error)
                    else:
                        exc_traceback = "".join(traceback.format_exception(None, exc, exc.__traceback__))
                        self.logger.error(logging_text + created_tasks_info[task] + exc_traceback)
                else:
                    self.logger.debug(logging_text + created_tasks_info[task] + ": Done")
            stage[1] = "{}/{}.".format(num_done, num_tasks)
            if new_error:
                stage[1] += " Errors: " + ". ".join(error_detail_list) + "."
                if nsr_id:  # update also nsr
                    self.update_db_2("nsrs", nsr_id, {"errorDescription": "Error at: " + ", ".join(error_list),
                                                      "errorDetail": ". ".join(error_detail_list)})
            self._write_op_status(nslcmop_id, stage)
        return error_detail_list

    @staticmethod
    def _map_primitive_params(primitive_desc, params, instantiation_params):
        """
        Generates the params to be provided to charm before executing primitive. If user does not provide a parameter,
        The default-value is used. If it is between < > it look for a value at instantiation_params
        :param primitive_desc: portion of VNFD/NSD that describes primitive
        :param params: Params provided by user
        :param instantiation_params: Instantiation params provided by user
        :return: a dictionary with the calculated params
        """
        calculated_params = {}
        for parameter in primitive_desc.get("parameter", ()):
            param_name = parameter["name"]
            if param_name in params:
                calculated_params[param_name] = params[param_name]
            elif "default-value" in parameter or "value" in parameter:
                if "value" in parameter:
                    calculated_params[param_name] = parameter["value"]
                else:
                    calculated_params[param_name] = parameter["default-value"]
                if isinstance(calculated_params[param_name], str) and calculated_params[param_name].startswith("<") \
                        and calculated_params[param_name].endswith(">"):
                    if calculated_params[param_name][1:-1] in instantiation_params:
                        calculated_params[param_name] = instantiation_params[calculated_params[param_name][1:-1]]
                    else:
                        raise LcmException("Parameter {} needed to execute primitive {} not provided".
                                           format(calculated_params[param_name], primitive_desc["name"]))
            else:
                raise LcmException("Parameter {} needed to execute primitive {} not provided".
                                   format(param_name, primitive_desc["name"]))

            if isinstance(calculated_params[param_name], (dict, list, tuple)):
                calculated_params[param_name] = yaml.safe_dump(calculated_params[param_name], default_flow_style=True,
                                                               width=256)
            elif isinstance(calculated_params[param_name], str) and calculated_params[param_name].startswith("!!yaml "):
                calculated_params[param_name] = calculated_params[param_name][7:]

        # add always ns_config_info if primitive name is config
        if primitive_desc["name"] == "config":
            if "ns_config_info" in instantiation_params:
                calculated_params["ns_config_info"] = instantiation_params["ns_config_info"]
        return calculated_params

    def _look_for_deployed_vca(self, deployed_vca, member_vnf_index, vdu_id, vdu_count_index, kdu_name=None):
        # find vca_deployed record for this action. Raise LcmException if not found or there is not any id.
        for vca in deployed_vca:
            if not vca:
                continue
            if member_vnf_index != vca["member-vnf-index"] or vdu_id != vca["vdu_id"]:
                continue
            if vdu_count_index is not None and vdu_count_index != vca["vdu_count_index"]:
                continue
            if kdu_name and kdu_name != vca["kdu_name"]:
                continue
            break
        else:
            # vca_deployed not found
            raise LcmException("charm for member_vnf_index={} vdu_id={} kdu_name={} vdu_count_index={} is not "
                               "deployed".format(member_vnf_index, vdu_id, kdu_name, vdu_count_index))

        # get ee_id
        ee_id = vca.get("ee_id")
        if not ee_id:
            raise LcmException("charm for member_vnf_index={} vdu_id={} kdu_name={} vdu_count_index={} has not "
                               "execution environment"
                               .format(member_vnf_index, vdu_id, kdu_name, vdu_count_index))
        return ee_id

    async def _ns_execute_primitive(self, ee_id, primitive, primitive_params, retries=0,
                                    retries_interval=30, timeout=None) -> (str, str):
        try:
            if primitive == "config":
                primitive_params = {"params": primitive_params}

            while retries >= 0:
                try:
                    output = await asyncio.wait_for(
                        self.n2vc.exec_primitive(
                            ee_id=ee_id,
                            primitive_name=primitive,
                            params_dict=primitive_params,
                            progress_timeout=self.timeout_progress_primitive,
                            total_timeout=self.timeout_primitive),
                        timeout=timeout or self.timeout_primitive)
                    # execution was OK
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # asyncio.TimeoutError
                    if isinstance(e, asyncio.TimeoutError):
                        e = "Timeout"
                    retries -= 1
                    if retries >= 0:
                        self.logger.debug('Error executing action {} on {} -> {}'.format(primitive, ee_id, e))
                        # wait and retry
                        await asyncio.sleep(retries_interval, loop=self.loop)
                    else:
                        return 'FAILED', str(e)

            return 'COMPLETED', output

        except (LcmException, asyncio.CancelledError):
            raise
        except Exception as e:
            return 'FAIL', 'Error executing action {}: {}'.format(primitive, e)

    async def action(self, nsr_id, nslcmop_id):

        # Try to lock HA task here
        task_is_locked_by_me = self.lcm_tasks.lock_HA('ns', 'nslcmops', nslcmop_id)
        if not task_is_locked_by_me:
            return

        logging_text = "Task ns={} action={} ".format(nsr_id, nslcmop_id)
        self.logger.debug(logging_text + "Enter")
        # get all needed from database
        db_nsr = None
        db_nslcmop = None
        db_nsr_update = {}
        db_nslcmop_update = {}
        nslcmop_operation_state = None
        error_description_nslcmop = None
        exc = None
        try:
            # wait for any previous tasks in process
            step = "Waiting for previous operations to terminate"
            await self.lcm_tasks.waitfor_related_HA('ns', 'nslcmops', nslcmop_id)

            self._write_ns_status(
                nsr_id=nsr_id,
                ns_state=None,
                current_operation="RUNNING ACTION",
                current_operation_id=nslcmop_id
            )

            step = "Getting information from database"
            db_nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_id})
            db_nsr = self.db.get_one("nsrs", {"_id": nsr_id})

            nsr_deployed = db_nsr["_admin"].get("deployed")
            vnf_index = db_nslcmop["operationParams"].get("member_vnf_index")
            vdu_id = db_nslcmop["operationParams"].get("vdu_id")
            kdu_name = db_nslcmop["operationParams"].get("kdu_name")
            vdu_count_index = db_nslcmop["operationParams"].get("vdu_count_index")
            primitive = db_nslcmop["operationParams"]["primitive"]
            primitive_params = db_nslcmop["operationParams"]["primitive_params"]
            timeout_ns_action = db_nslcmop["operationParams"].get("timeout_ns_action", self.timeout_primitive)

            if vnf_index:
                step = "Getting vnfr from database"
                db_vnfr = self.db.get_one("vnfrs", {"member-vnf-index-ref": vnf_index, "nsr-id-ref": nsr_id})
                step = "Getting vnfd from database"
                db_vnfd = self.db.get_one("vnfds", {"_id": db_vnfr["vnfd-id"]})
            else:
                step = "Getting nsd from database"
                db_nsd = self.db.get_one("nsds", {"_id": db_nsr["nsd-id"]})

            # for backward compatibility
            if nsr_deployed and isinstance(nsr_deployed.get("VCA"), dict):
                nsr_deployed["VCA"] = list(nsr_deployed["VCA"].values())
                db_nsr_update["_admin.deployed.VCA"] = nsr_deployed["VCA"]
                self.update_db_2("nsrs", nsr_id, db_nsr_update)

            # look for primitive
            config_primitive_desc = None
            if vdu_id:
                for vdu in get_iterable(db_vnfd, "vdu"):
                    if vdu_id == vdu["id"]:
                        for config_primitive in deep_get(vdu, ("vdu-configuration", "config-primitive"), ()):
                            if config_primitive["name"] == primitive:
                                config_primitive_desc = config_primitive
                                break
                        break
            elif kdu_name:
                for kdu in get_iterable(db_vnfd, "kdu"):
                    if kdu_name == kdu["name"]:
                        for config_primitive in deep_get(kdu, ("kdu-configuration", "config-primitive"), ()):
                            if config_primitive["name"] == primitive:
                                config_primitive_desc = config_primitive
                                break
                        break
            elif vnf_index:
                for config_primitive in deep_get(db_vnfd, ("vnf-configuration", "config-primitive"), ()):
                    if config_primitive["name"] == primitive:
                        config_primitive_desc = config_primitive
                        break
            else:
                for config_primitive in deep_get(db_nsd, ("ns-configuration", "config-primitive"), ()):
                    if config_primitive["name"] == primitive:
                        config_primitive_desc = config_primitive
                        break

            if not config_primitive_desc and not (kdu_name and primitive in ("upgrade", "rollback", "status")):
                raise LcmException("Primitive {} not found at [ns|vnf|vdu]-configuration:config-primitive ".
                                   format(primitive))

            if vnf_index:
                if vdu_id:
                    vdur = next((x for x in db_vnfr["vdur"] if x["vdu-id-ref"] == vdu_id), None)
                    desc_params = self._format_additional_params(vdur.get("additionalParams"))
                elif kdu_name:
                    kdur = next((x for x in db_vnfr["kdur"] if x["kdu-name"] == kdu_name), None)
                    desc_params = self._format_additional_params(kdur.get("additionalParams"))
                else:
                    desc_params = self._format_additional_params(db_vnfr.get("additionalParamsForVnf"))
            else:
                desc_params = self._format_additional_params(db_nsr.get("additionalParamsForNs"))

            if kdu_name:
                kdu_action = True if not deep_get(kdu, ("kdu-configuration", "juju")) else False

            # TODO check if ns is in a proper status
            if kdu_name and (primitive in ("upgrade", "rollback", "status") or kdu_action):
                # kdur and desc_params already set from before
                if primitive_params:
                    desc_params.update(primitive_params)
                # TODO Check if we will need something at vnf level
                for index, kdu in enumerate(get_iterable(nsr_deployed, "K8s")):
                    if kdu_name == kdu["kdu-name"] and kdu["member-vnf-index"] == vnf_index:
                        break
                else:
                    raise LcmException("KDU '{}' for vnf '{}' not deployed".format(kdu_name, vnf_index))

                if kdu.get("k8scluster-type") not in self.k8scluster_map:
                    msg = "unknown k8scluster-type '{}'".format(kdu.get("k8scluster-type"))
                    raise LcmException(msg)

                db_dict = {"collection": "nsrs",
                           "filter": {"_id": nsr_id},
                           "path": "_admin.deployed.K8s.{}".format(index)}
                self.logger.debug(logging_text + "Exec k8s {} on {}.{}".format(primitive, vnf_index, kdu_name))
                step = "Executing kdu {}".format(primitive)
                if primitive == "upgrade":
                    if desc_params.get("kdu_model"):
                        kdu_model = desc_params.get("kdu_model")
                        del desc_params["kdu_model"]
                    else:
                        kdu_model = kdu.get("kdu-model")
                        parts = kdu_model.split(sep=":")
                        if len(parts) == 2:
                            kdu_model = parts[0]

                    detailed_status = await asyncio.wait_for(
                        self.k8scluster_map[kdu["k8scluster-type"]].upgrade(
                            cluster_uuid=kdu.get("k8scluster-uuid"),
                            kdu_instance=kdu.get("kdu-instance"),
                            atomic=True, kdu_model=kdu_model,
                            params=desc_params, db_dict=db_dict,
                            timeout=timeout_ns_action),
                        timeout=timeout_ns_action + 10)
                    self.logger.debug(logging_text + " Upgrade of kdu {} done".format(detailed_status))
                elif primitive == "rollback":
                    detailed_status = await asyncio.wait_for(
                        self.k8scluster_map[kdu["k8scluster-type"]].rollback(
                            cluster_uuid=kdu.get("k8scluster-uuid"),
                            kdu_instance=kdu.get("kdu-instance"),
                            db_dict=db_dict),
                        timeout=timeout_ns_action)
                elif primitive == "status":
                    detailed_status = await asyncio.wait_for(
                        self.k8scluster_map[kdu["k8scluster-type"]].status_kdu(
                            cluster_uuid=kdu.get("k8scluster-uuid"),
                            kdu_instance=kdu.get("kdu-instance")),
                        timeout=timeout_ns_action)
                else:
                    kdu_instance = kdu.get("kdu-instance") or "{}-{}".format(kdu["kdu-name"], nsr_id)
                    params = self._map_primitive_params(config_primitive_desc, primitive_params, desc_params)

                    detailed_status = await asyncio.wait_for(
                        self.k8scluster_map[kdu["k8scluster-type"]].exec_primitive(
                            cluster_uuid=kdu.get("k8scluster-uuid"),
                            kdu_instance=kdu_instance,
                            primitive_name=primitive,
                            params=params, db_dict=db_dict,
                            timeout=timeout_ns_action),
                        timeout=timeout_ns_action)

                if detailed_status:
                    nslcmop_operation_state = 'COMPLETED'
                else:
                    detailed_status = ''
                    nslcmop_operation_state = 'FAILED'
            else:
                nslcmop_operation_state, detailed_status = await self._ns_execute_primitive(
                    self._look_for_deployed_vca(nsr_deployed["VCA"],
                                                member_vnf_index=vnf_index,
                                                vdu_id=vdu_id,
                                                vdu_count_index=vdu_count_index),
                    primitive=primitive,
                    primitive_params=self._map_primitive_params(config_primitive_desc, primitive_params, desc_params),
                    timeout=timeout_ns_action)

            db_nslcmop_update["detailed-status"] = detailed_status
            error_description_nslcmop = detailed_status if nslcmop_operation_state == "FAILED" else ""
            self.logger.debug(logging_text + " task Done with result {} {}".format(nslcmop_operation_state,
                                                                                   detailed_status))
            return  # database update is called inside finally

        except (DbException, LcmException, N2VCException, K8sException) as e:
            self.logger.error(logging_text + "Exit Exception {}".format(e))
            exc = e
        except asyncio.CancelledError:
            self.logger.error(logging_text + "Cancelled Exception while '{}'".format(step))
            exc = "Operation was cancelled"
        except asyncio.TimeoutError:
            self.logger.error(logging_text + "Timeout while '{}'".format(step))
            exc = "Timeout"
        except Exception as e:
            exc = traceback.format_exc()
            self.logger.critical(logging_text + "Exit Exception {} {}".format(type(e).__name__, e), exc_info=True)
        finally:
            if exc:
                db_nslcmop_update["detailed-status"] = detailed_status = error_description_nslcmop = \
                    "FAILED {}: {}".format(step, exc)
                nslcmop_operation_state = "FAILED"
            if db_nsr:
                self._write_ns_status(
                    nsr_id=nsr_id,
                    ns_state=db_nsr["nsState"],   # TODO check if degraded. For the moment use previous status
                    current_operation="IDLE",
                    current_operation_id=None,
                    # error_description=error_description_nsr,
                    # error_detail=error_detail,
                    other_update=db_nsr_update
                )

            if db_nslcmop:
                self._write_op_status(
                    op_id=nslcmop_id,
                    stage="",
                    error_message=error_description_nslcmop,
                    operation_state=nslcmop_operation_state,
                    other_update=db_nslcmop_update,
                )

            if nslcmop_operation_state:
                try:
                    await self.msg.aiowrite("ns", "actioned", {"nsr_id": nsr_id, "nslcmop_id": nslcmop_id,
                                                               "operationState": nslcmop_operation_state},
                                            loop=self.loop)
                except Exception as e:
                    self.logger.error(logging_text + "kafka_write notification Exception {}".format(e))
            self.logger.debug(logging_text + "Exit")
            self.lcm_tasks.remove("ns", nsr_id, nslcmop_id, "ns_action")
            return nslcmop_operation_state, detailed_status

    async def scale(self, nsr_id, nslcmop_id):

        # Try to lock HA task here
        task_is_locked_by_me = self.lcm_tasks.lock_HA('ns', 'nslcmops', nslcmop_id)
        if not task_is_locked_by_me:
            return

        logging_text = "Task ns={} scale={} ".format(nsr_id, nslcmop_id)
        self.logger.debug(logging_text + "Enter")
        # get all needed from database
        db_nsr = None
        db_nslcmop = None
        db_nslcmop_update = {}
        nslcmop_operation_state = None
        db_nsr_update = {}
        exc = None
        # in case of error, indicates what part of scale was failed to put nsr at error status
        scale_process = None
        old_operational_status = ""
        old_config_status = ""
        vnfr_scaled = False
        try:
            # wait for any previous tasks in process
            step = "Waiting for previous operations to terminate"
            await self.lcm_tasks.waitfor_related_HA('ns', 'nslcmops', nslcmop_id)

            self._write_ns_status(
                nsr_id=nsr_id,
                ns_state=None,
                current_operation="SCALING",
                current_operation_id=nslcmop_id
            )

            step = "Getting nslcmop from database"
            self.logger.debug(step + " after having waited for previous tasks to be completed")
            db_nslcmop = self.db.get_one("nslcmops", {"_id": nslcmop_id})
            step = "Getting nsr from database"
            db_nsr = self.db.get_one("nsrs", {"_id": nsr_id})

            old_operational_status = db_nsr["operational-status"]
            old_config_status = db_nsr["config-status"]
            step = "Parsing scaling parameters"
            # self.logger.debug(step)
            db_nsr_update["operational-status"] = "scaling"
            self.update_db_2("nsrs", nsr_id, db_nsr_update)
            nsr_deployed = db_nsr["_admin"].get("deployed")

            #######
            nsr_deployed = db_nsr["_admin"].get("deployed")
            vnf_index = db_nslcmop["operationParams"].get("member_vnf_index")
            # vdu_id = db_nslcmop["operationParams"].get("vdu_id")
            # vdu_count_index = db_nslcmop["operationParams"].get("vdu_count_index")
            # vdu_name = db_nslcmop["operationParams"].get("vdu_name")
            #######

            RO_nsr_id = nsr_deployed["RO"]["nsr_id"]
            vnf_index = db_nslcmop["operationParams"]["scaleVnfData"]["scaleByStepData"]["member-vnf-index"]
            scaling_group = db_nslcmop["operationParams"]["scaleVnfData"]["scaleByStepData"]["scaling-group-descriptor"]
            scaling_type = db_nslcmop["operationParams"]["scaleVnfData"]["scaleVnfType"]
            # scaling_policy = db_nslcmop["operationParams"]["scaleVnfData"]["scaleByStepData"].get("scaling-policy")

            # for backward compatibility
            if nsr_deployed and isinstance(nsr_deployed.get("VCA"), dict):
                nsr_deployed["VCA"] = list(nsr_deployed["VCA"].values())
                db_nsr_update["_admin.deployed.VCA"] = nsr_deployed["VCA"]
                self.update_db_2("nsrs", nsr_id, db_nsr_update)

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
            step = "Sending scale order to VIM"
            nb_scale_op = 0
            if not db_nsr["_admin"].get("scaling-group"):
                self.update_db_2("nsrs", nsr_id, {"_admin.scaling-group": [{"name": scaling_group, "nb-scale-op": 0}]})
                admin_scale_index = 0
            else:
                for admin_scale_index, admin_scale_info in enumerate(db_nsr["_admin"]["scaling-group"]):
                    if admin_scale_info["name"] == scaling_group:
                        nb_scale_op = admin_scale_info.get("nb-scale-op", 0)
                        break
                else:  # not found, set index one plus last element and add new entry with the name
                    admin_scale_index += 1
                    db_nsr_update["_admin.scaling-group.{}.name".format(admin_scale_index)] = scaling_group
            RO_scaling_info = []
            vdu_scaling_info = {"scaling_group_name": scaling_group, "vdu": []}
            if scaling_type == "SCALE_OUT":
                # count if max-instance-count is reached
                max_instance_count = scaling_descriptor.get("max-instance-count", 10)
                # self.logger.debug("MAX_INSTANCE_COUNT is {}".format(max_instance_count))
                if nb_scale_op >= max_instance_count:
                    raise LcmException("reached the limit of {} (max-instance-count) "
                                       "scaling-out operations for the "
                                       "scaling-group-descriptor '{}'".format(nb_scale_op, scaling_group))

                nb_scale_op += 1
                vdu_scaling_info["scaling_direction"] = "OUT"
                vdu_scaling_info["vdu-create"] = {}
                for vdu_scale_info in scaling_descriptor["vdu"]:
                    RO_scaling_info.append({"osm_vdu_id": vdu_scale_info["vdu-id-ref"], "member-vnf-index": vnf_index,
                                            "type": "create", "count": vdu_scale_info.get("count", 1)})
                    vdu_scaling_info["vdu-create"][vdu_scale_info["vdu-id-ref"]] = vdu_scale_info.get("count", 1)

            elif scaling_type == "SCALE_IN":
                # count if min-instance-count is reached
                min_instance_count = 0
                if "min-instance-count" in scaling_descriptor and scaling_descriptor["min-instance-count"] is not None:
                    min_instance_count = int(scaling_descriptor["min-instance-count"])
                if nb_scale_op <= min_instance_count:
                    raise LcmException("reached the limit of {} (min-instance-count) scaling-in operations for the "
                                       "scaling-group-descriptor '{}'".format(nb_scale_op, scaling_group))
                nb_scale_op -= 1
                vdu_scaling_info["scaling_direction"] = "IN"
                vdu_scaling_info["vdu-delete"] = {}
                for vdu_scale_info in scaling_descriptor["vdu"]:
                    RO_scaling_info.append({"osm_vdu_id": vdu_scale_info["vdu-id-ref"], "member-vnf-index": vnf_index,
                                            "type": "delete", "count": vdu_scale_info.get("count", 1)})
                    vdu_scaling_info["vdu-delete"][vdu_scale_info["vdu-id-ref"]] = vdu_scale_info.get("count", 1)

            # update VDU_SCALING_INFO with the VDUs to delete ip_addresses
            vdu_create = vdu_scaling_info.get("vdu-create")
            vdu_delete = copy(vdu_scaling_info.get("vdu-delete"))
            if vdu_scaling_info["scaling_direction"] == "IN":
                for vdur in reversed(db_vnfr["vdur"]):
                    if vdu_delete.get(vdur["vdu-id-ref"]):
                        vdu_delete[vdur["vdu-id-ref"]] -= 1
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
                vdu_delete = vdu_scaling_info.pop("vdu-delete")

            # PRE-SCALE BEGIN
            step = "Executing pre-scale vnf-config-primitive"
            if scaling_descriptor.get("scaling-config-action"):
                for scaling_config_action in scaling_descriptor["scaling-config-action"]:
                    if (scaling_config_action.get("trigger") == "pre-scale-in" and scaling_type == "SCALE_IN") \
                       or (scaling_config_action.get("trigger") == "pre-scale-out" and scaling_type == "SCALE_OUT"):
                        vnf_config_primitive = scaling_config_action["vnf-config-primitive-name-ref"]
                        step = db_nslcmop_update["detailed-status"] = \
                            "executing pre-scale scaling-config-action '{}'".format(vnf_config_primitive)

                        # look for primitive
                        for config_primitive in db_vnfd.get("vnf-configuration", {}).get("config-primitive", ()):
                            if config_primitive["name"] == vnf_config_primitive:
                                break
                        else:
                            raise LcmException(
                                "Invalid vnfd descriptor at scaling-group-descriptor[name='{}']:scaling-config-action"
                                "[vnf-config-primitive-name-ref='{}'] does not match any vnf-configuration:config-"
                                "primitive".format(scaling_group, config_primitive))

                        vnfr_params = {"VDU_SCALE_INFO": vdu_scaling_info}
                        if db_vnfr.get("additionalParamsForVnf"):
                            vnfr_params.update(db_vnfr["additionalParamsForVnf"])

                        scale_process = "VCA"
                        db_nsr_update["config-status"] = "configuring pre-scaling"
                        primitive_params = self._map_primitive_params(config_primitive, {}, vnfr_params)

                        # Pre-scale reintent check: Check if this sub-operation has been executed before
                        op_index = self._check_or_add_scale_suboperation(
                            db_nslcmop, nslcmop_id, vnf_index, vnf_config_primitive, primitive_params, 'PRE-SCALE')
                        if (op_index == self.SUBOPERATION_STATUS_SKIP):
                            # Skip sub-operation
                            result = 'COMPLETED'
                            result_detail = 'Done'
                            self.logger.debug(logging_text +
                                              "vnf_config_primitive={} Skipped sub-operation, result {} {}".format(
                                                  vnf_config_primitive, result, result_detail))
                        else:
                            if (op_index == self.SUBOPERATION_STATUS_NEW):
                                # New sub-operation: Get index of this sub-operation
                                op_index = len(db_nslcmop.get('_admin', {}).get('operations')) - 1
                                self.logger.debug(logging_text + "vnf_config_primitive={} New sub-operation".
                                                  format(vnf_config_primitive))
                            else:
                                # Reintent:  Get registered params for this existing sub-operation
                                op = db_nslcmop.get('_admin', {}).get('operations', [])[op_index]
                                vnf_index = op.get('member_vnf_index')
                                vnf_config_primitive = op.get('primitive')
                                primitive_params = op.get('primitive_params')
                                self.logger.debug(logging_text + "vnf_config_primitive={} Sub-operation reintent".
                                                  format(vnf_config_primitive))
                            # Execute the primitive, either with new (first-time) or registered (reintent) args
                            result, result_detail = await self._ns_execute_primitive(
                                self._look_for_deployed_vca(nsr_deployed["VCA"],
                                                            member_vnf_index=vnf_index,
                                                            vdu_id=None,
                                                            vdu_count_index=None),
                                vnf_config_primitive, primitive_params)
                            self.logger.debug(logging_text + "vnf_config_primitive={} Done with result {} {}".format(
                                vnf_config_primitive, result, result_detail))
                            # Update operationState = COMPLETED | FAILED
                            self._update_suboperation_status(
                                db_nslcmop, op_index, result, result_detail)

                        if result == "FAILED":
                            raise LcmException(result_detail)
                        db_nsr_update["config-status"] = old_config_status
                        scale_process = None
            # PRE-SCALE END

            # SCALE RO - BEGIN
            # Should this block be skipped if 'RO_nsr_id' == None ?
            # if (RO_nsr_id and RO_scaling_info):
            if RO_scaling_info:
                scale_process = "RO"
                # Scale RO reintent check: Check if this sub-operation has been executed before
                op_index = self._check_or_add_scale_suboperation(
                    db_nslcmop, vnf_index, None, None, 'SCALE-RO', RO_nsr_id, RO_scaling_info)
                if (op_index == self.SUBOPERATION_STATUS_SKIP):
                    # Skip sub-operation
                    result = 'COMPLETED'
                    result_detail = 'Done'
                    self.logger.debug(logging_text + "Skipped sub-operation RO, result {} {}".format(
                        result, result_detail))
                else:
                    if (op_index == self.SUBOPERATION_STATUS_NEW):
                        # New sub-operation: Get index of this sub-operation
                        op_index = len(db_nslcmop.get('_admin', {}).get('operations')) - 1
                        self.logger.debug(logging_text + "New sub-operation RO")
                    else:
                        # Reintent:  Get registered params for this existing sub-operation
                        op = db_nslcmop.get('_admin', {}).get('operations', [])[op_index]
                        RO_nsr_id = op.get('RO_nsr_id')
                        RO_scaling_info = op.get('RO_scaling_info')
                        self.logger.debug(logging_text + "Sub-operation RO reintent".format(
                            vnf_config_primitive))

                    RO_desc = await self.RO.create_action("ns", RO_nsr_id, {"vdu-scaling": RO_scaling_info})
                    db_nsr_update["_admin.scaling-group.{}.nb-scale-op".format(admin_scale_index)] = nb_scale_op
                    db_nsr_update["_admin.scaling-group.{}.time".format(admin_scale_index)] = time()
                    # wait until ready
                    RO_nslcmop_id = RO_desc["instance_action_id"]
                    db_nslcmop_update["_admin.deploy.RO"] = RO_nslcmop_id

                    RO_task_done = False
                    step = detailed_status = "Waiting RO_task_id={} to complete the scale action.".format(RO_nslcmop_id)
                    detailed_status_old = None
                    self.logger.debug(logging_text + step)

                    deployment_timeout = 1 * 3600   # One hour
                    while deployment_timeout > 0:
                        if not RO_task_done:
                            desc = await self.RO.show("ns", item_id_name=RO_nsr_id, extra_item="action",
                                                      extra_item_id=RO_nslcmop_id)

                            # deploymentStatus
                            self._on_update_ro_db(nsrs_id=nsr_id, ro_descriptor=desc)

                            ns_status, ns_status_info = self.RO.check_action_status(desc)
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

                            if ns_status == "ERROR":
                                raise ROclient.ROClientException(ns_status_info)
                            elif ns_status == "BUILD":
                                detailed_status = step + "; {}".format(ns_status_info)
                            elif ns_status == "ACTIVE":
                                step = detailed_status = \
                                    "Waiting for management IP address reported by the VIM. Updating VNFRs"
                                if not vnfr_scaled:
                                    self.scale_vnfr(db_vnfr, vdu_create=vdu_create, vdu_delete=vdu_delete)
                                    vnfr_scaled = True
                                try:
                                    desc = await self.RO.show("ns", RO_nsr_id)

                                    # deploymentStatus
                                    self._on_update_ro_db(nsrs_id=nsr_id, ro_descriptor=desc)

                                    # nsr_deployed["nsr_ip"] = RO.get_ns_vnf_info(desc)
                                    self.ns_update_vnfr({db_vnfr["member-vnf-index-ref"]: db_vnfr}, desc)
                                    break
                                except LcmExceptionNoMgmtIP:
                                    pass
                            else:
                                assert False, "ROclient.check_ns_status returns unknown {}".format(ns_status)
                        if detailed_status != detailed_status_old:
                            self._update_suboperation_status(
                                db_nslcmop, op_index, 'COMPLETED', detailed_status)
                            detailed_status_old = db_nslcmop_update["detailed-status"] = detailed_status
                            self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)

                        await asyncio.sleep(5, loop=self.loop)
                        deployment_timeout -= 5
                    if deployment_timeout <= 0:
                        self._update_suboperation_status(
                            db_nslcmop, nslcmop_id, op_index, 'FAILED', "Timeout when waiting for ns to get ready")
                        raise ROclient.ROClientException("Timeout waiting ns to be ready")

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

                    self._update_suboperation_status(db_nslcmop, op_index, 'COMPLETED', 'Done')
            # SCALE RO - END

            scale_process = None
            if db_nsr_update:
                self.update_db_2("nsrs", nsr_id, db_nsr_update)

            # POST-SCALE BEGIN
            # execute primitive service POST-SCALING
            step = "Executing post-scale vnf-config-primitive"
            if scaling_descriptor.get("scaling-config-action"):
                for scaling_config_action in scaling_descriptor["scaling-config-action"]:
                    if (scaling_config_action.get("trigger") == "post-scale-in" and scaling_type == "SCALE_IN") \
                       or (scaling_config_action.get("trigger") == "post-scale-out" and scaling_type == "SCALE_OUT"):
                        vnf_config_primitive = scaling_config_action["vnf-config-primitive-name-ref"]
                        step = db_nslcmop_update["detailed-status"] = \
                            "executing post-scale scaling-config-action '{}'".format(vnf_config_primitive)

                        vnfr_params = {"VDU_SCALE_INFO": vdu_scaling_info}
                        if db_vnfr.get("additionalParamsForVnf"):
                            vnfr_params.update(db_vnfr["additionalParamsForVnf"])

                        # look for primitive
                        for config_primitive in db_vnfd.get("vnf-configuration", {}).get("config-primitive", ()):
                            if config_primitive["name"] == vnf_config_primitive:
                                break
                        else:
                            raise LcmException("Invalid vnfd descriptor at scaling-group-descriptor[name='{}']:"
                                               "scaling-config-action[vnf-config-primitive-name-ref='{}'] does not "
                                               "match any vnf-configuration:config-primitive".format(scaling_group,
                                                                                                     config_primitive))
                        scale_process = "VCA"
                        db_nsr_update["config-status"] = "configuring post-scaling"
                        primitive_params = self._map_primitive_params(config_primitive, {}, vnfr_params)

                        # Post-scale reintent check: Check if this sub-operation has been executed before
                        op_index = self._check_or_add_scale_suboperation(
                            db_nslcmop, nslcmop_id, vnf_index, vnf_config_primitive, primitive_params, 'POST-SCALE')
                        if op_index == self.SUBOPERATION_STATUS_SKIP:
                            # Skip sub-operation
                            result = 'COMPLETED'
                            result_detail = 'Done'
                            self.logger.debug(logging_text +
                                              "vnf_config_primitive={} Skipped sub-operation, result {} {}".
                                              format(vnf_config_primitive, result, result_detail))
                        else:
                            if op_index == self.SUBOPERATION_STATUS_NEW:
                                # New sub-operation: Get index of this sub-operation
                                op_index = len(db_nslcmop.get('_admin', {}).get('operations')) - 1
                                self.logger.debug(logging_text + "vnf_config_primitive={} New sub-operation".
                                                  format(vnf_config_primitive))
                            else:
                                # Reintent:  Get registered params for this existing sub-operation
                                op = db_nslcmop.get('_admin', {}).get('operations', [])[op_index]
                                vnf_index = op.get('member_vnf_index')
                                vnf_config_primitive = op.get('primitive')
                                primitive_params = op.get('primitive_params')
                                self.logger.debug(logging_text + "vnf_config_primitive={} Sub-operation reintent".
                                                  format(vnf_config_primitive))
                            # Execute the primitive, either with new (first-time) or registered (reintent) args
                            result, result_detail = await self._ns_execute_primitive(
                                self._look_for_deployed_vca(nsr_deployed["VCA"],
                                                            member_vnf_index=vnf_index,
                                                            vdu_id=None,
                                                            vdu_count_index=None),
                                vnf_config_primitive, primitive_params)
                            self.logger.debug(logging_text + "vnf_config_primitive={} Done with result {} {}".format(
                                vnf_config_primitive, result, result_detail))
                            # Update operationState = COMPLETED | FAILED
                            self._update_suboperation_status(
                                db_nslcmop, op_index, result, result_detail)

                        if result == "FAILED":
                            raise LcmException(result_detail)
                        db_nsr_update["config-status"] = old_config_status
                        scale_process = None
            # POST-SCALE END

            db_nslcmop_update["operationState"] = nslcmop_operation_state = "COMPLETED"
            db_nslcmop_update["statusEnteredTime"] = time()
            db_nslcmop_update["detailed-status"] = "done"
            db_nsr_update["detailed-status"] = ""  # "scaled {} {}".format(scaling_group, scaling_type)
            db_nsr_update["operational-status"] = "running" if old_operational_status == "failed" \
                else old_operational_status
            db_nsr_update["config-status"] = old_config_status
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
            self._write_ns_status(
                nsr_id=nsr_id,
                ns_state=None,
                current_operation="IDLE",
                current_operation_id=None
            )
            if exc:
                if db_nslcmop:
                    db_nslcmop_update["detailed-status"] = "FAILED {}: {}".format(step, exc)
                    db_nslcmop_update["operationState"] = nslcmop_operation_state = "FAILED"
                    db_nslcmop_update["statusEnteredTime"] = time()
                if db_nsr:
                    db_nsr_update["operational-status"] = old_operational_status
                    db_nsr_update["config-status"] = old_config_status
                    db_nsr_update["detailed-status"] = ""
                    if scale_process:
                        if "VCA" in scale_process:
                            db_nsr_update["config-status"] = "failed"
                        if "RO" in scale_process:
                            db_nsr_update["operational-status"] = "failed"
                        db_nsr_update["detailed-status"] = "FAILED scaling nslcmop={} {}: {}".format(nslcmop_id, step,
                                                                                                     exc)
            try:
                if db_nslcmop and db_nslcmop_update:
                    self.update_db_2("nslcmops", nslcmop_id, db_nslcmop_update)
                if db_nsr:
                    self._write_ns_status(
                        nsr_id=nsr_id,
                        ns_state=None,
                        current_operation="IDLE",
                        current_operation_id=None,
                        other_update=db_nsr_update
                    )

            except DbException as e:
                self.logger.error(logging_text + "Cannot update database: {}".format(e))
            if nslcmop_operation_state:
                try:
                    await self.msg.aiowrite("ns", "scaled", {"nsr_id": nsr_id, "nslcmop_id": nslcmop_id,
                                                             "operationState": nslcmop_operation_state},
                                            loop=self.loop)
                    # if cooldown_time:
                    #     await asyncio.sleep(cooldown_time, loop=self.loop)
                    # await self.msg.aiowrite("ns","scaled-cooldown-time", {"nsr_id": nsr_id, "nslcmop_id": nslcmop_id})
                except Exception as e:
                    self.logger.error(logging_text + "kafka_write notification Exception {}".format(e))
            self.logger.debug(logging_text + "Exit")
            self.lcm_tasks.remove("ns", nsr_id, nslcmop_id, "ns_scale")
