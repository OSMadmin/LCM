##
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
# contact: alfonso.tiernosepulveda@telefonica.com
##

import asynctest
import logging

from osm_lcm import lcm_helm_conn
from osm_lcm.lcm_helm_conn import LCMHelmConn
from osm_common.fslocal import FsLocal
from asynctest.mock import Mock
from osm_common.dbmemory import DbMemory

__author__ = "Isabel Lloret <illoret@indra.es>"


class TestLcmHelmConn(asynctest.TestCase):
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    async def setUp(self):
        self.db = Mock(DbMemory())
        self.fs = asynctest.Mock(FsLocal())
        self.fs.path = "/app/storage"
        vca_config = {}
        lcm_helm_conn.K8sHelmConnector = asynctest.Mock(lcm_helm_conn.K8sHelmConnector)
        self.helm_conn = LCMHelmConn(self.db, self.fs, loop=self.loop, vca_config=vca_config, log=self.logger)

    @asynctest.fail_on(active_handles=True)
    async def test_create_execution_environment(self):
        namespace = "testnamespace"
        db_dict = {}
        artifact_path = "helm_sample_charm"
        helm_chart_id = "helm_sample_charm_0001"
        self.helm_conn._k8sclusterhelm.install = asynctest.CoroutineMock(return_value=helm_chart_id)
        self.db.get_one.return_value = {"_admin": {"helm-chart": {"id": "myk8s_id"}}}
        ee_id, _ = await self.helm_conn.create_execution_environment(namespace, db_dict, artifact_path=artifact_path)
        self.assertEqual(ee_id, "{}.{}".format("osm", helm_chart_id),
                         "Check ee_id format: <default namespace>.<helm_chart-id>")
        self.helm_conn._k8sclusterhelm.install.assert_called_once_with("myk8s_id",
                                                                       kdu_model="/app/storage/helm_sample_charm",
                                                                       namespace="osm", db_dict=db_dict,
                                                                       params=None, timeout=None)

    @asynctest.fail_on(active_handles=True)
    async def test_get_ee_ssh_public__key(self):
        ee_id = "osm.helm_sample_charm_0001"
        db_dict = {}
        lcm_helm_conn.socket.gethostbyname = asynctest.Mock()
        mock_pub_key = "ssh-rsapubkey"
        self.db.get_one.return_value = {"_admin": {"helm-chart": {"id": "myk8s_id"}}}
        self.helm_conn._get_ssh_key = asynctest.CoroutineMock(return_value=mock_pub_key)
        pub_key = await self.helm_conn.get_ee_ssh_public__key(ee_id=ee_id, db_dict=db_dict)
        self.assertEqual(pub_key, mock_pub_key)

    @asynctest.fail_on(active_handles=True)
    async def test_execute_primitive(self):
        lcm_helm_conn.socket.gethostbyname = asynctest.Mock()
        ee_id = "osm.helm_sample_charm_0001"
        primitive_name = "sleep"
        params = {}
        self.db.get_one.return_value = {"_admin": {"helm-chart": {"id": "myk8s_id"}}}
        self.helm_conn._execute_primitive_internal = asynctest.CoroutineMock(return_value=("OK", "test-ok"))
        message = await self.helm_conn.exec_primitive(ee_id, primitive_name, params)
        self.assertEqual(message, "test-ok")

    @asynctest.fail_on(active_handles=True)
    async def test_execute_config_primitive(self):
        self.logger.debug("Execute config primitive")
        lcm_helm_conn.socket.gethostbyname = asynctest.Mock()
        ee_id = "osm.helm_sample_charm_0001"
        primitive_name = "config"
        params = {"ssh-host-name": "host1"}
        self.db.get_one.return_value = {"_admin": {"helm-chart": {"id": "myk8s_id"}}}
        self.helm_conn._execute_primitive_internal = asynctest.CoroutineMock(return_value=("OK", "CONFIG OK"))
        message = await self.helm_conn.exec_primitive(ee_id, primitive_name, params)
        self.assertEqual(message, "CONFIG OK")

    @asynctest.fail_on(active_handles=True)
    async def test_delete_execution_environment(self):
        ee_id = "osm.helm_sample_charm_0001"
        self.db.get_one.return_value = {"_admin": {"helm-chart": {"id": "myk8s_id"}}}
        self.helm_conn._k8sclusterhelm.uninstall = asynctest.CoroutineMock()
        await self.helm_conn.delete_execution_environment(ee_id)
        self.helm_conn._k8sclusterhelm.uninstall.assert_called_once_with("myk8s_id", "helm_sample_charm_0001")


if __name__ == '__main__':
    asynctest.main()
