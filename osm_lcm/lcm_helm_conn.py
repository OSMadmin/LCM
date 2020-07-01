##
# Copyright 2020 Telefonica Investigacion y Desarrollo, S.A.U.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
##
import functools
import yaml
import asyncio
import socket
import uuid

from grpclib.client import Channel

from osm_lcm.frontend_pb2 import PrimitiveRequest
from osm_lcm.frontend_pb2 import SshKeyRequest, SshKeyReply
from osm_lcm.frontend_grpc import FrontendExecutorStub

from n2vc.n2vc_conn import N2VCConnector
from n2vc.k8s_helm_conn import K8sHelmConnector
from n2vc.exceptions import N2VCBadArgumentsException, N2VCException, N2VCExecutionException

from osm_lcm.lcm_utils import deep_get


def retryer(max_wait_time=60, delay_time=10):
    def wrapper(func):
        retry_exceptions = (
            ConnectionRefusedError
        )

        @functools.wraps(func)
        async def wrapped(*args, **kwargs):
            wait_time = max_wait_time
            while wait_time > 0:
                try:
                    return await func(*args, **kwargs)
                except retry_exceptions:
                    wait_time = wait_time - delay_time
                    await asyncio.sleep(delay_time)
                    continue
            else:
                return ConnectionRefusedError
        return wrapped
    return wrapper


class LCMHelmConn(N2VCConnector):
    _KUBECTL_OSM_NAMESPACE = "osm"
    _KUBECTL_OSM_CLUSTER_NAME = "_system-osm-k8s"
    _EE_SERVICE_PORT = 50050

    # Time beetween retries
    _EE_RETRY_DELAY = 10
    # Initial max retry time
    _MAX_INITIAL_RETRY_TIME = 300
    # Other retry time
    _MAX_RETRY_TIME = 30

    def __init__(self,
                 db: object,
                 fs: object,
                 log: object = None,
                 loop: object = None,
                 url: str = None,
                 username: str = None,
                 vca_config: dict = None,
                 on_update_db=None, ):
        """
        Initialize EE helm connector.
        """

        # parent class constructor
        N2VCConnector.__init__(
            self,
            db=db,
            fs=fs,
            log=log,
            loop=loop,
            url=url,
            username=username,
            vca_config=vca_config,
            on_update_db=on_update_db,
        )

        self.log.debug("Initialize helm N2VC connector")

        # TODO - Obtain data from configuration
        self._ee_service_port = self._EE_SERVICE_PORT

        self._retry_delay = self._EE_RETRY_DELAY
        self._max_retry_time = self._MAX_RETRY_TIME
        self._initial_retry_time = self._MAX_INITIAL_RETRY_TIME

        # initialize helm connector
        self._k8sclusterhelm = K8sHelmConnector(
            kubectl_command=self.vca_config.get("kubectlpath"),
            helm_command=self.vca_config.get("helmpath"),
            fs=self.fs,
            log=self.log,
            db=self.db,
            on_update_db=None,
        )

        self._system_cluster_id = None
        self.log.info("Helm N2VC connector initialized")

    # TODO - ¿reuse_ee_id?
    async def create_execution_environment(self,
                                           namespace: str,
                                           db_dict: dict,
                                           reuse_ee_id: str = None,
                                           progress_timeout: float = None,
                                           total_timeout: float = None,
                                           artifact_path: str = None,
                                           vca_type: str = None) -> (str, dict):
        """
        Creates a new helm execution environment deploying the helm-chat indicated in the
        attifact_path
        :param str namespace: This param is not used, all helm charts are deployed in the osm
        system namespace
        :param dict db_dict: where to write to database when the status changes.
            It contains a dictionary with {collection: str, filter: {},  path: str},
                e.g. {collection: "nsrs", filter: {_id: <nsd-id>, path:
                "_admin.deployed.VCA.3"}
        :param str reuse_ee_id: ee id from an older execution. TODO - right now this params is not used
        :param float progress_timeout:
        :param float total_timeout:
        :param str artifact_path  path of package content
        :param str vca_type  Type of vca, not used as assumed of type helm
        :returns str, dict: id of the new execution environment including namespace.helm_id
        and credentials object set to None as all credentials should be osm kubernetes .kubeconfig
        """

        self.log.info(
            "create_execution_environment: namespace: {}, artifact_path: {}, db_dict: {}, "
            "reuse_ee_id: {}".format(
                namespace, artifact_path, db_dict, reuse_ee_id)
        )

        # Validate artifact-path is provided
        if artifact_path is None or len(artifact_path) == 0:
            raise N2VCBadArgumentsException(
                message="artifact_path is mandatory", bad_args=["artifact_path"]
            )

        # Validate artifact-path exists

        # remove / in charm path
        while artifact_path.find("//") >= 0:
            artifact_path = artifact_path.replace("//", "/")

        # check charm path
        if self.fs.file_exists(artifact_path):
            helm_chart_path = artifact_path
        else:
            msg = "artifact path does not exist: {}".format(artifact_path)
            raise N2VCBadArgumentsException(message=msg, bad_args=["artifact_path"])

        if artifact_path.startswith("/"):
            full_path = self.fs.path + helm_chart_path
        else:
            full_path = self.fs.path + "/" + helm_chart_path

        try:
            # Call helm conn install
            # Obtain system cluster id from database
            system_cluster_uuid = self._get_system_cluster_id()

            self.log.debug("install helm chart: {}".format(full_path))
            helm_id = await self._k8sclusterhelm.install(system_cluster_uuid, kdu_model=full_path,
                                                         namespace=self._KUBECTL_OSM_NAMESPACE,
                                                         db_dict=db_dict,
                                                         timeout=progress_timeout)

            ee_id = "{}.{}".format(self._KUBECTL_OSM_NAMESPACE, helm_id)
            return ee_id, None
        except Exception as e:
            self.log.error("Error deploying chart ee: {}".format(e), exc_info=True)
            raise N2VCException("Error deploying chart ee: {}".format(e))

    async def register_execution_environment(self, namespace: str, credentials: dict, db_dict: dict,
                                             progress_timeout: float = None, total_timeout: float = None) -> str:
        # nothing to do
        pass

    async def install_configuration_sw(self,
                                       ee_id: str,
                                       artifact_path: str,
                                       db_dict: dict,
                                       progress_timeout: float = None,
                                       total_timeout: float = None,
                                       config: dict = None,
                                       num_units: int = 1,
                                       vca_type: str = None
                                       ):
        # nothing to do
        pass

    async def add_relation(self, ee_id_1: str, ee_id_2: str, endpoint_1: str, endpoint_2: str):
        # nothing to do
        pass

    async def remove_relation(self):
        # nothing to to
        pass

    async def get_status(self, namespace: str, yaml_format: bool = True):
        # not used for this connector
        pass

    async def get_ee_ssh_public__key(self, ee_id: str, db_dict: dict, progress_timeout: float = None,
                                     total_timeout: float = None) -> str:
        """
        Obtains ssh-public key from ee executing GetSShKey method from the ee.

        :param str ee_id: the id of the execution environment returned by
            create_execution_environment or register_execution_environment
        :param dict db_dict:
        :param float progress_timeout:
        :param float total_timeout:
        :returns: public key of the execution environment
        """

        self.log.info(
            "get_ee_ssh_public_key: ee_id: {}, db_dict: {}".format(
                ee_id, db_dict)
        )

        # check arguments
        if ee_id is None or len(ee_id) == 0:
            raise N2VCBadArgumentsException(
                message="ee_id is mandatory", bad_args=["ee_id"]
            )

        try:
            # Obtain ip_addr for the ee service, it is resolved by dns from the ee name by kubernetes
            namespace, helm_id = self._get_ee_id_parts(ee_id)
            ip_addr = socket.gethostbyname(helm_id)

            # Obtain ssh_key from the ee, this method will implement retries to allow the ee
            # install libraries and start successfully
            ssh_key = await self._get_ssh_key(ip_addr)
            return ssh_key
        except Exception as e:
            self.log.error("Error obtaining ee ssh_key: {}".format(e), exc_info=True)
            raise N2VCException("Error obtaining ee ssh_ke: {}".format(e))

    async def exec_primitive(self, ee_id: str, primitive_name: str, params_dict: dict, db_dict: dict = None,
                             progress_timeout: float = None, total_timeout: float = None) -> str:
        """
        Execute a primitive in the execution environment

        :param str ee_id: the one returned by create_execution_environment or
            register_execution_environment with the format namespace.helm_id
        :param str primitive_name: must be one defined in the software. There is one
            called 'config', where, for the proxy case, the 'credentials' of VM are
            provided
        :param dict params_dict: parameters of the action
        :param dict db_dict: where to write into database when the status changes.
                        It contains a dict with
                            {collection: <str>, filter: {},  path: <str>},
                            e.g. {collection: "nslcmops", filter:
                                {_id: <nslcmop_id>, path: "_admin.VCA"}
                        It will be used to store information about intermediate notifications
        :param float progress_timeout:
        :param float total_timeout:
        :returns str: primitive result, if ok. It raises exceptions in case of fail
        """

        self.log.info("exec primitive for ee_id : {}, primitive_name: {}, params_dict: {}, db_dict: {}".format(
            ee_id, primitive_name, params_dict, db_dict
        ))

        # check arguments
        if ee_id is None or len(ee_id) == 0:
            raise N2VCBadArgumentsException(
                message="ee_id is mandatory", bad_args=["ee_id"]
            )
        if primitive_name is None or len(primitive_name) == 0:
            raise N2VCBadArgumentsException(
                message="action_name is mandatory", bad_args=["action_name"]
            )
        if params_dict is None:
            params_dict = dict()

        try:
            namespace, helm_id = self._get_ee_id_parts(ee_id)
            ip_addr = socket.gethostbyname(helm_id)
        except Exception as e:
            self.log.error("Error getting ee ip ee: {}".format(e))
            raise N2VCException("Error getting ee ip ee: {}".format(e))

        if primitive_name == "config":
            try:
                # Execute config primitive, higher timeout to check the case ee is starting
                status, detailed_message = await self._execute_config_primitive(ip_addr, params_dict, db_dict=db_dict)
                self.log.debug("Executed config primitive ee_id_ {}, status: {}, message: {}".format(
                    ee_id, status, detailed_message))
                if status != "OK":
                    self.log.error("Error configuring helm ee, status: {}, message: {}".format(
                        status, detailed_message))
                    raise N2VCExecutionException(
                        message="Error configuring helm ee_id: {}, status: {}, message: {}: ".format(
                            ee_id, status, detailed_message
                        ),
                        primitive_name=primitive_name,
                    )
            except Exception as e:
                self.log.error("Error configuring helm ee: {}".format(e))
                raise N2VCExecutionException(
                    message="Error configuring helm ee_id: {}, {}".format(
                        ee_id, e
                    ),
                    primitive_name=primitive_name,
                )
            return "CONFIG OK"
        else:
            try:
                # Execute primitive
                status, detailed_message = await self._execute_primitive(ip_addr, primitive_name,
                                                                         params_dict, db_dict=db_dict)
                self.log.debug("Executed primitive {} ee_id_ {}, status: {}, message: {}".format(
                    primitive_name, ee_id, status, detailed_message))
                if status != "OK" and status != "PROCESSING":
                    self.log.error(
                        "Execute primitive {} returned not ok status: {}, message: {}".format(
                            primitive_name, status, detailed_message)
                    )
                    raise N2VCExecutionException(
                        message="Execute primitive {} returned not ok status: {}, message: {}".format(
                            primitive_name, status, detailed_message
                        ),
                        primitive_name=primitive_name,
                    )
            except Exception as e:
                self.log.error(
                    "Error executing primitive {}: {}".format(primitive_name, e)
                )
                raise N2VCExecutionException(
                    message="Error executing primitive {} into ee={} : {}".format(
                        primitive_name, ee_id, e
                    ),
                    primitive_name=primitive_name,
                )
            return detailed_message

    async def deregister_execution_environments(self):
        # nothing to be done
        pass

    async def delete_execution_environment(self, ee_id: str, db_dict: dict = None, total_timeout: float = None):
        """
        Delete an execution environment
        :param str ee_id: id of the execution environment to delete, included namespace.helm_id
        :param dict db_dict: where to write into database when the status changes.
                        It contains a dict with
                            {collection: <str>, filter: {},  path: <str>},
                            e.g. {collection: "nsrs", filter:
                                {_id: <nsd-id>, path: "_admin.deployed.VCA.3"}
        :param float total_timeout:
        """

        self.log.info("ee_id: {}".format(ee_id))

        # check arguments
        if ee_id is None:
            raise N2VCBadArgumentsException(
                message="ee_id is mandatory", bad_args=["ee_id"]
            )

        try:

            # Obtain cluster_uuid
            system_cluster_uuid = self._get_system_cluster_id()

            # Get helm_id
            namespace, helm_id = self._get_ee_id_parts(ee_id)

            # Uninstall chart
            await self._k8sclusterhelm.uninstall(system_cluster_uuid, helm_id)
            self.log.info("ee_id: {} deleted".format(ee_id))
        except Exception as e:
            self.log.error("Error deleting ee id: {}: {}".format(ee_id, e), exc_info=True)
            raise N2VCException("Error deleting ee id {}: {}".format(ee_id, e))

    async def delete_namespace(self, namespace: str, db_dict: dict = None, total_timeout: float = None):
        # method not implemented for this connector, execution environments must be deleted individually
        pass

    async def install_k8s_proxy_charm(
        self,
        charm_name: str,
        namespace: str,
        artifact_path: str,
        db_dict: dict,
        progress_timeout: float = None,
        total_timeout: float = None,
        config: dict = None,
    ) -> str:
        pass

    @retryer(max_wait_time=_MAX_INITIAL_RETRY_TIME, delay_time=_EE_RETRY_DELAY)
    async def _get_ssh_key(self, ip_addr):
        channel = Channel(ip_addr, self._ee_service_port)
        try:
            stub = FrontendExecutorStub(channel)
            self.log.debug("get ssh key, ip_addr: {}".format(ip_addr))
            reply: SshKeyReply = await stub.GetSshKey(SshKeyRequest())
            return reply.message
        finally:
            channel.close()

    @retryer(max_wait_time=_MAX_INITIAL_RETRY_TIME, delay_time=_EE_RETRY_DELAY)
    async def _execute_config_primitive(self, ip_addr, params, db_dict=None):
        return await self._execute_primitive_internal(ip_addr, "config", params, db_dict=db_dict)

    @retryer(max_wait_time=_MAX_RETRY_TIME, delay_time=_EE_RETRY_DELAY)
    async def _execute_primitive(self, ip_addr, primitive_name, params, db_dict=None):
        return await  self._execute_primitive_internal(ip_addr, primitive_name, params, db_dict=db_dict)

    async def _execute_primitive_internal(self, ip_addr, primitive_name, params, db_dict=None):

        channel = Channel(ip_addr, self._ee_service_port)
        try:
            stub = FrontendExecutorStub(channel)
            async with stub.RunPrimitive.open() as stream:
                primitive_id = str(uuid.uuid1())
                result = None
                self.log.debug("Execute primitive internal: id:{}, name:{}, params: {}".
                               format(primitive_id, primitive_name, params))
                await stream.send_message(
                    PrimitiveRequest(id=primitive_id, name=primitive_name, params=yaml.dump(params)), end=True)
                async for reply in stream:
                    self.log.debug("Received reply: {}".format(reply))
                    result = reply
                    # If db_dict provided write notifs in database
                    if db_dict:
                        self._write_op_detailed_status(db_dict, reply.status, reply.detailed_message)
                if result:
                    return reply.status, reply.detailed_message
                else:
                    return "ERROR", "No result received"
        finally:
            channel.close()

    def _write_op_detailed_status(self, db_dict, status, detailed_message):

        # write ee_id to database: _admin.deployed.VCA.x
        try:
            the_table = db_dict["collection"]
            the_filter = db_dict["filter"]
            update_dict = {"detailed-status": "{}: {}".format(status, detailed_message)}
            # self.log.debug('Writing ee_id to database: {}'.format(the_path))
            self.db.set_one(
                table=the_table,
                q_filter=the_filter,
                update_dict=update_dict,
                fail_on_empty=True,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.log.error("Error writing detailedStatus to database: {}".format(e))

    def _get_system_cluster_id(self):
        if not self._system_cluster_id:
            db_k8cluster = self.db.get_one("k8sclusters", {"name": self._KUBECTL_OSM_CLUSTER_NAME})
            k8s_hc_id = deep_get(db_k8cluster, ("_admin", "helm-chart", "id"))
            self._system_cluster_id = k8s_hc_id
        return self._system_cluster_id

    def _get_ee_id_parts(self, ee_id):
        namespace, _, helm_id = ee_id.partition('.')
        return namespace, helm_id
