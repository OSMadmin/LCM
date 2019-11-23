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
#
# For those usages not covered by the Apache License, Version 2.0 please
# contact: esousa@whitestack.com or alfonso.tiernosepulveda@telefonica.com
##


import asynctest   # pip3 install asynctest --user
import asyncio
import yaml
# import logging
from os import getenv
from osm_lcm.ns import NsLcm
from osm_common.dbmongo import DbMongo
from osm_common.msgkafka import MsgKafka
from osm_common.fslocal import FsLocal
from osm_lcm.lcm_utils import TaskRegistry
from n2vc.vnf import N2VC
from uuid import uuid4

from osm_lcm.tests import test_db_descriptors as descriptors

__author__ = "Alfonso Tierno <alfonso.tiernosepulveda@telefonica.com>"

""" Perform unittests using asynctest of osm_lcm.ns module
It allows, if some testing ENV are supplied, testing without mocking some external libraries for debugging:
    OSMLCMTEST_NS_PUBKEY: public ssh-key returned by N2VC to inject to VMs
    OSMLCMTEST_NS_NAME: change name of NS
    OSMLCMTEST_PACKAGES_PATH: path where the vnf-packages are stored (de-compressed), each one on a 'vnfd_id' folder
    OSMLCMTEST_NS_IPADDRESS: IP address where emulated VMs are reached. Comma separate list
    OSMLCMTEST_RO_VIMID: VIM id of RO target vim IP. Obtain it with openmano datcenter-list on RO container
    OSMLCMTEST_VCA_NOMOCK: Do no mock the VCA, N2VC library, for debugging it
    OSMLCMTEST_RO_NOMOCK: Do no mock the ROClient library, for debugging it
    OSMLCMTEST_DB_NOMOCK: Do no mock the database library, for debugging it
    OSMLCMTEST_FS_NOMOCK: Do no mock the File Storage library, for debugging it
    OSMLCMTEST_LOGGING_NOMOCK: Do no mock the logging
    OSMLCM_VCA_XXX: configuration of N2VC
    OSMLCM_RO_XXX: configuration of RO
"""


vca_config = {   # TODO replace with os.get_env to get other configurations
    "host": getenv("OSMLCM_VCA_HOST", "vca"),
    "port": getenv("OSMLCM_VCA_PORT", 17070),
    "user": getenv("OSMLCM_VCA_USER", "admin"),
    "secret": getenv("OSMLCM_VCA_SECRET", "vca"),
    "pubkey": getenv("OSMLCM_VCA_PUBKEY", None),
    'cacert': getenv("OSMLCM_VCA_CACERT", None)
}

ro_config = {
    "endpoint_url": "http://{}:{}/openmano".format(getenv("OSMLCM_RO_HOST", "ro"), getenv("OSMLCM_RO_PORT", "9090")),
    "tenant": getenv("OSMLCM_RO_TENANT", "osm"),
    "logger_name": "lcm.ROclient",
    "loglevel": "DEBUG",
}


class TestMyNS(asynctest.TestCase):

    def _db_get_one(self, table, q_filter=None, fail_on_empty=True, fail_on_more=True):
        if table not in self.db_content:
            self.assertTrue(False, "db.get_one called with table={}".format(table))
        for db_item in self.db_content[table]:
            if db_item["_id"] == q_filter["_id"]:
                return db_item
        else:
            self.assertTrue(False, "db.get_one, table={}, not found _id={}".format(table, q_filter["_id"]))

    def _db_get_list(self, table, q_filter=None):
        if table not in self.db_content:
            self.assertTrue(False, "db.get_list called with table={} not found".format(table))
        return self.db_content[table]

    def _db_set_one(self, table, q_filter, update_dict, fail_on_empty=True, unset=None, pull=None, push=None):
        db_item = self._db_get_one(table, q_filter, fail_on_empty=fail_on_empty)
        for k, v in update_dict.items():
            db_nested = db_item
            k_list = k.split(".")
            for k_nested in k_list[0:-1]:
                if isinstance(db_nested, list):
                    db_nested = db_nested[int(k_nested)]
                else:
                    if k_nested not in db_nested:
                        db_nested[k_nested] = {}
                    db_nested = db_nested[k_nested]
            k_nested = k_list[-1]
            if isinstance(db_nested, list):
                if int(k_nested) < len(db_nested):
                    db_nested[int(k_nested)] = v
                else:
                    db_nested.insert(int(k_nested), v)
            else:
                db_nested[k_nested] = v

    async def _n2vc_DeployCharms(self, model_name, application_name, vnfd, charm_path, params={}, machine_spec={},
                                 callback=None, *callback_args):
        if callback:
            for status, message in (("maintenance", "installing sofwware"), ("active", "Ready!")):
                # call callback after some time
                asyncio.sleep(5, loop=self.loop)
                callback(model_name, application_name, status, message, *callback_args)

    @staticmethod
    def _n2vc_FormatApplicationName(*args):
        num_calls = 0
        while True:
            yield "app_name-{}".format(num_calls)
            num_calls += 1

    def _n2vc_CreateExecutionEnvironment(self, namespace, reuse_ee_id, db_dict):
        k_list = namespace.split(".")
        ee_id = k_list[1] + "."
        if len(k_list) >= 2:
            for k in k_list[2:4]:
                ee_id += k[:8]
        else:
            ee_id += "_NS_"
        return ee_id, {}

    def _ro_show(self, *args, **kwargs):
        ro_ns_desc = yaml.load(descriptors.db_ro_ns_text, Loader=yaml.Loader)

        # if ip address provided, replace descriptor
        ip_addresses = getenv("OSMLCMTEST_NS_IPADDRESS", "")
        if ip_addresses:
            ip_addresses_list = ip_addresses.split(",")
            for vnf in ro_ns_desc["vnfs"]:
                if not ip_addresses_list:
                    break
                vnf["ip_address"] = ip_addresses_list[0]
                for vm in vnf["vms"]:
                    if not ip_addresses_list:
                        break
                    vm["ip_address"] = ip_addresses_list.pop(0)

        while True:
            yield ro_ns_desc
            for net in ro_ns_desc["nets"]:
                if net["status"] != "ACTIVE":
                    net["status"] = "ACTIVE"
                    break
            else:
                for vnf in ro_ns_desc["vnfs"]:
                    for vm in vnf["vms"]:
                        if vm["status"] != "ACTIVE":
                            vm["status"] = "ACTIVE"
                            break

    def _ro_create(self, *args, **kwargs):
        while True:
            yield {"uuid": str(uuid4())}

    def _return_uuid(self, *args, **kwargs):
        return str(uuid4())

    async def setUp(self):
        # Mock DB
        if not getenv("OSMLCMTEST_DB_NOMOCK"):
            self.db = asynctest.Mock(DbMongo())
            self.db.get_one.side_effect = self._db_get_one
            self.db.get_list.side_effect = self._db_get_list
            self.db.set_one.side_effect = self._db_set_one
            self.db_content = {
                "nsrs": yaml.load(descriptors.db_nsrs_text, Loader=yaml.Loader),
                "nslcmops": yaml.load(descriptors.db_nslcmops_text, Loader=yaml.Loader),
                "vnfrs": yaml.load(descriptors.db_vnfrs_text, Loader=yaml.Loader),
                "vnfds": yaml.load(descriptors.db_vnfds_text, Loader=yaml.Loader),
                "vim_accounts": yaml.load(descriptors.db_vim_accounts_text, Loader=yaml.Loader),
            }
            self.db_vim_accounts = yaml.load(descriptors.db_vim_accounts_text, Loader=yaml.Loader)

        # Mock kafka
        self.msg = asynctest.Mock(MsgKafka())

        # Mock filesystem
        if not getenv("OSMLCMTEST_FS_NOMOCK"):
            self.fs = asynctest.Mock(FsLocal())
            self.fs.get_params.return_value = {"path": getenv("OSMLCMTEST_PACKAGES_PATH", "./test/temp/packages")}
            self.fs.file_open = asynctest.mock_open()
            # self.fs.file_open.return_value.__enter__.return_value = asynctest.MagicMock()  # called on a python "with"
            # self.fs.file_open.return_value.__enter__.return_value.read.return_value = ""   # empty file

        # Mock TaskRegistry
        self.lcm_tasks = asynctest.Mock(TaskRegistry())
        self.lcm_tasks.lock_HA.return_value = True
        self.lcm_tasks.waitfor_related_HA.return_value = None
        self.lcm_tasks.lookfor_related.return_value = ("", [])

        # Create NsLCM class
        self.my_ns = NsLcm(self.db, self.msg, self.fs, self.lcm_tasks, ro_config, vca_config, self.loop)

        # Mock logging
        if not getenv("OSMLCMTEST_LOGGING_NOMOCK"):
            self.my_ns.logger = asynctest.Mock(self.my_ns.logger)

        # Mock VCA - N2VC
        if not getenv("OSMLCMTEST_VCA_NOMOCK"):
            pub_key = getenv("OSMLCMTEST_NS_PUBKEY", "ssh-rsa test-pub-key t@osm.com")
            self.my_ns.n2vc = asynctest.Mock(N2VC())
            self.my_ns.n2vc.GetPublicKey.return_value = getenv("OSMLCM_VCA_PUBKEY", "public_key")
            # allow several versions of n2vc
            self.my_ns.n2vc.FormatApplicationName = asynctest.Mock(side_effect=self._n2vc_FormatApplicationName())
            self.my_ns.n2vc.DeployCharms = asynctest.CoroutineMock(side_effect=self._n2vc_DeployCharms)
            self.my_ns.n2vc.create_execution_environment = asynctest.CoroutineMock(
                side_effect=self._n2vc_CreateExecutionEnvironment)
            self.my_ns.n2vc.install_configuration_sw = asynctest.CoroutineMock(return_value=pub_key)
            self.my_ns.n2vc.get_ee_ssh_public__key = asynctest.CoroutineMock(return_value=pub_key)
            self.my_ns.n2vc.exec_primitive = asynctest.CoroutineMock(side_effect=self._return_uuid)
            self.my_ns.n2vc.GetPrimitiveStatus = asynctest.CoroutineMock(return_value="completed")
            self.my_ns.n2vc.GetPrimitiveOutput = asynctest.CoroutineMock(return_value={"result": "ok",
                                                                                       "pubkey": pub_key})
            self.my_ns.n2vc.get_public_key = asynctest.CoroutineMock(
                return_value=getenv("OSMLCM_VCA_PUBKEY", "public_key"))

        # Mock RO
        if not getenv("OSMLCMTEST_RO_NOMOCK"):
            # self.my_ns.RO = asynctest.Mock(ROclient.ROClient(self.loop, **ro_config))
            # TODO first time should be empty list, following should return a dict
            self.my_ns.RO.get_list = asynctest.CoroutineMock(self.my_ns.RO.get_list, return_value=[])
            self.my_ns.RO.create = asynctest.CoroutineMock(self.my_ns.RO.create, side_effect=self._ro_create())
            self.my_ns.RO.show = asynctest.CoroutineMock(self.my_ns.RO.show, side_effect=self._ro_show())
            self.my_ns.RO.create_action = asynctest.CoroutineMock(self.my_ns.RO.create_action,
                                                                  return_value={"vm-id": {"vim_result": 200,
                                                                                          "description": "done"}})

    @asynctest.fail_on(active_handles=True)   # all async tasks must be completed
    async def test_instantiate(self):
        nsr_id = self.db_content["nsrs"][0]["_id"]
        nslcmop_id = self.db_content["nslcmops"][0]["_id"]
        print("Test instantiate started")

        # delete deployed information of database
        if not getenv("OSMLCMTEST_DB_NOMOCK"):
            if self.db_content["nsrs"][0]["_admin"].get("deployed"):
                del self.db_content["nsrs"][0]["_admin"]["deployed"]
            for db_vnfr in self.db_content["vnfrs"]:
                db_vnfr.pop("ip_address", None)
                for db_vdur in db_vnfr["vdur"]:
                    db_vdur.pop("ip_address", None)
                    db_vdur.pop("mac_address", None)
            if getenv("OSMLCMTEST_RO_VIMID"):
                self.db_content["vim_accounts"][0]["_admin"]["deployed"]["RO"] = getenv("OSMLCMTEST_RO_VIMID")
            if getenv("OSMLCMTEST_RO_VIMID"):
                self.db_content["nsrs"][0]["_admin"]["deployed"]["RO"] = getenv("OSMLCMTEST_RO_VIMID")

        await self.my_ns.instantiate(nsr_id, nslcmop_id)

        print("instantiate_result: {}".format(self._db_get_one("nslcmops", {"_id": nslcmop_id}).get("detailed-status")))

        self.msg.aiowrite.assert_called_once_with("ns", "instantiated",
                                                  {"nsr_id": nsr_id, "nslcmop_id": nslcmop_id,
                                                   "operationState": "COMPLETED"},
                                                  loop=self.loop)
        self.lcm_tasks.lock_HA.assert_called_once_with('ns', 'nslcmops', nslcmop_id)
        if not getenv("OSMLCMTEST_LOGGING_NOMOCK"):
            self.assertTrue(self.my_ns.logger.debug.called, "Debug method not called")
            self.my_ns.logger.error.assert_not_called()
            self.my_ns.logger.exception().assert_not_called()

        if not getenv("OSMLCMTEST_DB_NOMOCK"):
            self.assertTrue(self.db.set_one.called, "db.set_one not called")

        # TODO add more checks of called methods
        # TODO add a terminate

    def test_ns_params_2_RO(self):
        vim = self._db_get_list("vim_accounts")[0]
        vim_id = vim["_id"]
        ro_vim_id = vim["_admin"]["deployed"]["RO"]
        ns_params = {"vimAccountId": vim_id}
        mgmt_interface = {"cp": "cp"}
        vdu = [{"id": "vdu_id", "interface": [{"external-connection-point-ref": "cp"}]}]
        vnfd_dict = {
            "1": {"vdu": vdu, "mgmt-interface": mgmt_interface},
            "2": {"vdu": vdu, "mgmt-interface": mgmt_interface, "vnf-configuration": None},
            "3": {"vdu": vdu, "mgmt-interface": mgmt_interface, "vnf-configuration": {"config-access": None}},
            "4": {"vdu": vdu, "mgmt-interface": mgmt_interface,
                  "vnf-configuration": {"config-access": {"ssh-access": None}}},
            "5": {"vdu": vdu, "mgmt-interface": mgmt_interface,
                  "vnf-configuration": {"config-access": {"ssh-access": {"required": True, "default_user": "U"}}}},
        }
        nsd = {"constituent-vnfd": []}
        for k in vnfd_dict.keys():
            nsd["constituent-vnfd"].append({"vnfd-id-ref": k, "member-vnf-index": k})

        n2vc_key_list = ["key"]
        ro_ns_params = self.my_ns.ns_params_2_RO(ns_params, nsd, vnfd_dict, n2vc_key_list)
        ro_params_expected = {'wim_account': None, "datacenter": ro_vim_id,
                              "vnfs": {"5": {"vdus": {"vdu_id": {"mgmt_keys": n2vc_key_list}}}}}
        self.assertEqual(ro_ns_params, ro_params_expected)

    # Test scale() and related methods
    @asynctest.fail_on(active_handles=True)   # all async tasks must be completed
    async def test_scale(self):
        # print("Test scale started")

        # TODO: Add more higher-lever tests here, for example:
        # scale-out/scale-in operations with success/error result

        # Test scale() with missing 'scaleVnfData', should return operationState = 'FAILED'
        nsr_id = self.db_content["nsrs"][0]["_id"]
        nslcmop_id = self.db_content["nslcmops"][0]["_id"]
        await self.my_ns.scale(nsr_id, nslcmop_id)
        expected_value = 'FAILED'
        return_value = self._db_get_one("nslcmops", {"_id": nslcmop_id}).get("operationState")
        self.assertEqual(return_value, expected_value)
        # print("scale_result: {}".format(self._db_get_one("nslcmops", {"_id": nslcmop_id}).get("detailed-status")))

    # Test _reintent_or_skip_suboperation()
    # Expected result:
    # - if a suboperation's 'operationState' is marked as 'COMPLETED', SUBOPERATION_STATUS_SKIP is expected
    # - if marked as anything but 'COMPLETED', the suboperation index is expected
    def test_scale_reintent_or_skip_suboperation(self):
        # Load an alternative 'nslcmops' YAML for this test
        self.db_content['nslcmops'] = yaml.load(descriptors.db_nslcmops_scale_text, Loader=yaml.Loader)
        db_nslcmop = self.db_content['nslcmops'][0]
        op_index = 2
        # Test when 'operationState' is 'COMPLETED'
        db_nslcmop['_admin']['operations'][op_index]['operationState'] = 'COMPLETED'
        return_value = self.my_ns._reintent_or_skip_suboperation(db_nslcmop, op_index)
        expected_value = self.my_ns.SUBOPERATION_STATUS_SKIP
        self.assertEqual(return_value, expected_value)
        # Test when 'operationState' is not 'COMPLETED'
        db_nslcmop['_admin']['operations'][op_index]['operationState'] = None
        return_value = self.my_ns._reintent_or_skip_suboperation(db_nslcmop, op_index)
        expected_value = op_index
        self.assertEqual(return_value, expected_value)

    # Test _find_suboperation()
    # Expected result: index of the found sub-operation, or SUBOPERATION_STATUS_NOT_FOUND if not found
    def test_scale_find_suboperation(self):
        # Load an alternative 'nslcmops' YAML for this test
        self.db_content['nslcmops'] = yaml.load(descriptors.db_nslcmops_scale_text, Loader=yaml.Loader)
        db_nslcmop = self.db_content['nslcmops'][0]
        # Find this sub-operation
        op_index = 2
        vnf_index = db_nslcmop['_admin']['operations'][op_index]['member_vnf_index']
        primitive = db_nslcmop['_admin']['operations'][op_index]['primitive']
        primitive_params = db_nslcmop['_admin']['operations'][op_index]['primitive_params']
        match = {
            'member_vnf_index': vnf_index,
            'primitive': primitive,
            'primitive_params': primitive_params,
        }
        found_op_index = self.my_ns._find_suboperation(db_nslcmop, match)
        self.assertEqual(found_op_index, op_index)
        # Test with not-matching params
        match = {
            'member_vnf_index': vnf_index,
            'primitive': '',
            'primitive_params': primitive_params,
        }
        found_op_index = self.my_ns._find_suboperation(db_nslcmop, match)
        self.assertEqual(found_op_index, self.my_ns.SUBOPERATION_STATUS_NOT_FOUND)
        # Test with None
        match = None
        found_op_index = self.my_ns._find_suboperation(db_nslcmop, match)
        self.assertEqual(found_op_index, self.my_ns.SUBOPERATION_STATUS_NOT_FOUND)

    # Test _update_suboperation_status()
    def test_scale_update_suboperation_status(self):
        db_nslcmop = self.db_content['nslcmops'][0]
        op_index = 0
        # Force the initial values to be distinct from the updated ones
        db_nslcmop['_admin']['operations'][op_index]['operationState'] = 'PROCESSING'
        db_nslcmop['_admin']['operations'][op_index]['detailed-status'] = 'In progress'
        # Test to change 'operationState' and 'detailed-status'
        operationState = 'COMPLETED'
        detailed_status = 'Done'
        self.my_ns._update_suboperation_status(
            db_nslcmop, op_index, operationState, detailed_status)
        operationState_new = db_nslcmop['_admin']['operations'][op_index]['operationState']
        detailed_status_new = db_nslcmop['_admin']['operations'][op_index]['detailed-status']
        # print("DEBUG: operationState_new={}, detailed_status_new={}".format(operationState_new, detailed_status_new))
        self.assertEqual(operationState, operationState_new)
        self.assertEqual(detailed_status, detailed_status_new)

    # Test _add_suboperation()
    def test_scale_add_suboperation(self):
        db_nslcmop = self.db_content['nslcmops'][0]
        vnf_index = '1'
        num_ops_before = len(db_nslcmop.get('_admin', {}).get('operations', [])) - 1
        vdu_id = None
        vdu_count_index = None
        vdu_name = None
        primitive = 'touch'
        mapped_primitive_params = {'parameter':
                                   [{'data-type': 'STRING',
                                     'name': 'filename',
                                     'default-value': '<touch_filename2>'}],
                                   'name': 'touch'}
        operationState = 'PROCESSING'
        detailed_status = 'In progress'
        operationType = 'PRE-SCALE'
        # Add a 'pre-scale' suboperation
        op_index_after = self.my_ns._add_suboperation(db_nslcmop, vnf_index, vdu_id, vdu_count_index,
                                                      vdu_name, primitive, mapped_primitive_params,
                                                      operationState, detailed_status, operationType)
        self.assertEqual(op_index_after, num_ops_before + 1)

        # Delete all suboperations and add the same operation again
        del db_nslcmop['_admin']['operations']
        op_index_zero = self.my_ns._add_suboperation(db_nslcmop, vnf_index, vdu_id, vdu_count_index,
                                                     vdu_name, primitive, mapped_primitive_params,
                                                     operationState, detailed_status, operationType)
        self.assertEqual(op_index_zero, 0)

        # Add a 'RO' suboperation
        RO_nsr_id = '1234567890'
        RO_scaling_info = [{'type': 'create', 'count': 1, 'member-vnf-index': '1', 'osm_vdu_id': 'dataVM'}]
        op_index = self.my_ns._add_suboperation(db_nslcmop, vnf_index, vdu_id, vdu_count_index,
                                                vdu_name, primitive, mapped_primitive_params,
                                                operationState, detailed_status, operationType,
                                                RO_nsr_id, RO_scaling_info)
        db_RO_nsr_id = db_nslcmop['_admin']['operations'][op_index]['RO_nsr_id']
        self.assertEqual(op_index, 1)
        self.assertEqual(RO_nsr_id, db_RO_nsr_id)

        # Try to add an invalid suboperation, should return SUBOPERATION_STATUS_NOT_FOUND
        op_index_invalid = self.my_ns._add_suboperation(None, None, None, None, None,
                                                        None, None, None,
                                                        None, None, None)
        self.assertEqual(op_index_invalid, self.my_ns.SUBOPERATION_STATUS_NOT_FOUND)

    # Test _check_or_add_scale_suboperation() and _check_or_add_scale_suboperation_RO()
    # check the possible return values:
    # - SUBOPERATION_STATUS_NEW: This is a new sub-operation
    # - op_index (non-negative number): This is an existing sub-operation, operationState != 'COMPLETED'
    # - SUBOPERATION_STATUS_SKIP: This is an existing sub-operation, operationState == 'COMPLETED'
    def test_scale_check_or_add_scale_suboperation(self):
        db_nslcmop = self.db_content['nslcmops'][0]
        operationType = 'PRE-SCALE'
        vnf_index = '1'
        primitive = 'touch'
        primitive_params = {'parameter':
                            [{'data-type': 'STRING',
                              'name': 'filename',
                              'default-value': '<touch_filename2>'}],
                            'name': 'touch'}

        # Delete all sub-operations to be sure this is a new sub-operation
        del db_nslcmop['_admin']['operations']

        # Add a new sub-operation
        # For new sub-operations, operationState is set to 'PROCESSING' by default
        op_index_new = self.my_ns._check_or_add_scale_suboperation(
            db_nslcmop, vnf_index, primitive, primitive_params, operationType)
        self.assertEqual(op_index_new, self.my_ns.SUBOPERATION_STATUS_NEW)

        # Use the same parameters again to match the already added sub-operation
        # which has status 'PROCESSING' (!= 'COMPLETED') by default
        # The expected return value is a non-negative number
        op_index_existing = self.my_ns._check_or_add_scale_suboperation(
            db_nslcmop, vnf_index, primitive, primitive_params, operationType)
        self.assertTrue(op_index_existing >= 0)

        # Change operationState 'manually' for this sub-operation
        db_nslcmop['_admin']['operations'][op_index_existing]['operationState'] = 'COMPLETED'
        # Then use the same parameters again to match the already added sub-operation,
        # which now has status 'COMPLETED'
        # The expected return value is SUBOPERATION_STATUS_SKIP
        op_index_skip = self.my_ns._check_or_add_scale_suboperation(
            db_nslcmop, vnf_index, primitive, primitive_params, operationType)
        self.assertEqual(op_index_skip, self.my_ns.SUBOPERATION_STATUS_SKIP)

        # RO sub-operation test:
        # Repeat tests for the very similar _check_or_add_scale_suboperation_RO(),
        RO_nsr_id = '1234567890'
        RO_scaling_info = [{'type': 'create', 'count': 1, 'member-vnf-index': '1', 'osm_vdu_id': 'dataVM'}]
        op_index_new_RO = self.my_ns._check_or_add_scale_suboperation(
            db_nslcmop, vnf_index, None, None, 'SCALE-RO', RO_nsr_id, RO_scaling_info)
        self.assertEqual(op_index_new_RO, self.my_ns.SUBOPERATION_STATUS_NEW)

        # Use the same parameters again to match the already added RO sub-operation
        op_index_existing_RO = self.my_ns._check_or_add_scale_suboperation(
            db_nslcmop, vnf_index, None, None, 'SCALE-RO', RO_nsr_id, RO_scaling_info)
        self.assertTrue(op_index_existing_RO >= 0)

        # Change operationState 'manually' for this RO sub-operation
        db_nslcmop['_admin']['operations'][op_index_existing_RO]['operationState'] = 'COMPLETED'
        # Then use the same parameters again to match the already added sub-operation,
        # which now has status 'COMPLETED'
        # The expected return value is SUBOPERATION_STATUS_SKIP
        op_index_skip_RO = self.my_ns._check_or_add_scale_suboperation(
            db_nslcmop, vnf_index, None, None, 'SCALE-RO', RO_nsr_id, RO_scaling_info)
        self.assertEqual(op_index_skip_RO, self.my_ns.SUBOPERATION_STATUS_SKIP)


if __name__ == '__main__':
    asynctest.main()
