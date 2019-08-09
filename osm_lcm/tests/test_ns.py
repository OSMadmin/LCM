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

db_vim_accounts_text = """
---
-   _admin:
        created: 1566818150.3024442
        current_operation: 0
        deployed:
            RO: dc51ce6c-c7f2-11e9-b9c0-02420aff0004
            RO-account: dc5c67fa-c7f2-11e9-b9c0-02420aff0004
        detailed-status: Done
        modified: 1566818150.3024442
        operationalState: ENABLED
        operations:
        -   detailed-status: Done
            lcmOperationType: create
            operationParams: null
            operationState: COMPLETED
            startTime: 1566818150.3025382
            statusEnteredTime: 1566818150.3025382
            worker: 86434c2948e2
        projects_read:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        projects_write:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
    _id: ea958ba5-4e58-4405-bf42-6e3be15d4c3a
    description: Openstack site 2, based on Mirantis, also called DSS9000-1, with
        tenant tid
    name: ost2-mrt-tid
    schema_version: '1.1'
    vim_password: 5g0yGX86qIhprX86YTMcpg==
    vim_tenant_name: osm
    vim_type: openstack
    vim_url: http://10.95.87.162:5000/v2.0
    vim_user: osm
"""

db_vnfds_text = """
---
-   _admin:
        created: 1566823352.7154346
        modified: 1566823353.9295402
        onboardingState: ONBOARDED
        operationalState: ENABLED
        projects_read:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        projects_write:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        storage:
            descriptor: hackfest_3charmed_vnfd/hackfest_3charmed_vnfd.yaml
            folder: 7637bcf8-cf14-42dc-ad70-c66fcf1e6e77
            fs: local
            path: /app/storage/
            pkg-dir: hackfest_3charmed_vnfd
            zipfile: package.tar.gz
        type: vnfd
        usageState: NOT_IN_USE
        userDefinedData: {}
    _id: 7637bcf8-cf14-42dc-ad70-c66fcf1e6e77
    connection-point:
    -   id: vnf-mgmt
        name: vnf-mgmt
        short-name: vnf-mgmt
        type: VPORT
    -   id: vnf-data
        name: vnf-data
        short-name: vnf-data
        type: VPORT
    description: A VNF consisting of 2 VDUs connected to an internal VL, and one VDU
        with cloud-init
    id: hackfest3charmed-vnf
    internal-vld:
    -   id: internal
        internal-connection-point:
        -   id-ref: mgmtVM-internal
        -   id-ref: dataVM-internal
        name: internal
        short-name: internal
        type: ELAN
    logo: osm.png
    mgmt-interface:
        cp: vnf-mgmt
    monitoring-param:
    -   aggregation-type: AVERAGE
        id: monitor1
        name: monitor1
        vdu-monitoring-param:
            vdu-monitoring-param-ref: dataVM_cpu_util
            vdu-ref: dataVM
    name: hackfest3charmed-vnf
    scaling-group-descriptor:
    -   max-instance-count: 10
        name: scale_dataVM
        scaling-config-action:
        -   trigger: post-scale-out
            vnf-config-primitive-name-ref: touch
        -   trigger: pre-scale-in
            vnf-config-primitive-name-ref: touch
        scaling-policy:
        -   cooldown-time: 60
            name: auto_cpu_util_above_threshold
            scaling-criteria:
            -   name: cpu_util_above_threshold
                scale-in-relational-operation: LE
                scale-in-threshold: '15.0000000000'
                scale-out-relational-operation: GE
                scale-out-threshold: '60.0000000000'
                vnf-monitoring-param-ref: monitor1
            scaling-type: automatic
            threshold-time: 0
        vdu:
        -   count: 1
            vdu-id-ref: dataVM
    short-name: hackfest3charmed-vnf
    vdu:
    -   count: '1'
        cloud-init-file: cloud-config.txt
        id: mgmtVM
        image: hackfest3-mgmt
        interface:
        -   external-connection-point-ref: vnf-mgmt
            name: mgmtVM-eth0
            position: 1
            type: EXTERNAL
            virtual-interface:
                type: VIRTIO
        -   internal-connection-point-ref: mgmtVM-internal
            name: mgmtVM-eth1
            position: 2
            type: INTERNAL
            virtual-interface:
                type: VIRTIO
        internal-connection-point:
        -   id: mgmtVM-internal
            name: mgmtVM-internal
            short-name: mgmtVM-internal
            type: VPORT
        name: mgmtVM
        vm-flavor:
            memory-mb: '1024'
            storage-gb: '10'
            vcpu-count: 1
    -   count: '1'
        id: dataVM
        image: hackfest3-mgmt
        interface:
        -   internal-connection-point-ref: dataVM-internal
            name: dataVM-eth0
            position: 1
            type: INTERNAL
            virtual-interface:
                type: VIRTIO
        -   external-connection-point-ref: vnf-data
            name: dataVM-xe0
            position: 2
            type: EXTERNAL
            virtual-interface:
                type: VIRTIO
        internal-connection-point:
        -   id: dataVM-internal
            name: dataVM-internal
            short-name: dataVM-internal
            type: VPORT
        monitoring-param:
        -   id: dataVM_cpu_util
            nfvi-metric: cpu_utilization
        name: dataVM
        vm-flavor:
            memory-mb: '1024'
            storage-gb: '10'
            vcpu-count: 1
    version: '1.0'
    vnf-configuration:
        config-access:
            ssh-access:
                required: True
                default-user: ubuntu
        config-primitive:
        -   name: touch
            parameter:
            -   data-type: STRING
                default-value: <touch_filename2>
                name: filename
        initial-config-primitive:
        -   name: config
            parameter:
            -   name: ssh-hostname
                value: <rw_mgmt_ip>
            -   name: ssh-username
                value: ubuntu
            -   name: ssh-password
                value: osm4u
            seq: '1'
        -   name: touch
            parameter:
            -   name: filename
                value: <touch_filename>
            seq: '2'
        juju:
            charm: simple
"""

db_nsds_text = """
---
-   _admin:
        created: 1566823353.971486
        modified: 1566823353.971486
        onboardingState: ONBOARDED
        operationalState: ENABLED
        projects_read:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        projects_write:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        storage:
            descriptor: hackfest_3charmed_nsd/hackfest_3charmed_nsd.yaml
            folder: 8c2f8b95-bb1b-47ee-8001-36dc090678da
            fs: local
            path: /app/storage/
            pkg-dir: hackfest_3charmed_nsd
            zipfile: package.tar.gz
        usageState: NOT_IN_USE
        userDefinedData: {}
    _id: 8c2f8b95-bb1b-47ee-8001-36dc090678da
    constituent-vnfd:
    -   member-vnf-index: '1'
        vnfd-id-ref: hackfest3charmed-vnf
    -   member-vnf-index: '2'
        vnfd-id-ref: hackfest3charmed-vnf
    description: NS with 2 VNFs hackfest3charmed-vnf connected by datanet and mgmtnet
        VLs
    id: hackfest3charmed-ns
    logo: osm.png
    name: hackfest3charmed-ns
    short-name: hackfest3charmed-ns
    version: '1.0'
    vld:
    -   id: mgmt
        mgmt-network: true
        name: mgmt
        short-name: mgmt
        type: ELAN
        vim-network-name: mgmt
        vnfd-connection-point-ref:
        -   member-vnf-index-ref: '1'
            vnfd-connection-point-ref: vnf-mgmt
            vnfd-id-ref: hackfest3charmed-vnf
        -   member-vnf-index-ref: '2'
            vnfd-connection-point-ref: vnf-mgmt
            vnfd-id-ref: hackfest3charmed-vnf
    -   id: datanet
        name: datanet
        short-name: datanet
        type: ELAN
        vnfd-connection-point-ref:
        -   member-vnf-index-ref: '1'
            vnfd-connection-point-ref: vnf-data
            vnfd-id-ref: hackfest3charmed-vnf
        -   member-vnf-index-ref: '2'
            vnfd-connection-point-ref: vnf-data
            vnfd-id-ref: hackfest3charmed-vnf
"""

db_nsrs_text = """
---
-   _admin:
        created: 1566823354.3716335
        deployed:
            RO:
                nsd_id: 876573b5-968d-40b9-b52b-91bf5c5844f7
                nsr_id: c9fe9908-3180-430d-b633-fca2f68db008
                nsr_status: ACTIVE
                vnfd:
                -   id: 1ab2a418-9fe3-4358-bf17-411e5155535f
                    member-vnf-index: '1'
                -   id: 0de348e3-c201-4f6a-91cc-7f957e2d5504
                    member-vnf-index: '2'
            VCA:
            -   application: alf-b-aa
                detailed-status: Ready!
                member-vnf-index: '1'
                model: f48163a6-c807-47bc-9682-f72caef5af85
                operational-status: active
                primitive_id: null
                ssh-public-key: ssh-rsa pub-key root@juju-145d3e-0
                step: ssh-public-key-obtained
                vdu_count_index: null
                vdu_id: null
                vdu_name: null
                vnfd_id: hackfest3charmed-vnf
            -   application: alf-c-ab
                detailed-status: Ready!
                member-vnf-index: '2'
                model: f48163a6-c807-47bc-9682-f72caef5af85
                operational-status: active
                primitive_id: null
                ssh-public-key: ssh-rsa pub-key root@juju-145d3e-0
                step: ssh-public-key-obtained
                vdu_count_index: null
                vdu_id: null
                vdu_name: null
                vnfd_id: hackfest3charmed-vnf
            VCA-model-name: f48163a6-c807-47bc-9682-f72caef5af85
        modified: 1566823354.3716335
        nsState: INSTANTIATED
        nslcmop: null
        projects_read:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        projects_write:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
    _id: f48163a6-c807-47bc-9682-f72caef5af85
    additionalParamsForNs: null
    admin-status: ENABLED
    config-status: init
    constituent-vnfr-ref:
    - 88d90b0c-faff-4b9f-bccd-017f33985984
    - 1ca3bb1a-b29b-49fe-bed6-5f3076d77434
    create-time: 1566823354.36234
    datacenter: ea958ba5-4e58-4405-bf42-6e3be15d4c3a
    description: default description
    detailed-status: 'ERROR executing proxy charm initial primitives for member_vnf_index=1
        vdu_id=None: charm error executing primitive verify-ssh-credentials for member_vnf_index=1
        vdu_id=None: ''timeout after 600 seconds'''
    id: f48163a6-c807-47bc-9682-f72caef5af85
    instantiate_params:
        nsDescription: default description
        nsName: ALF
        nsdId: 8c2f8b95-bb1b-47ee-8001-36dc090678da
        vimAccountId: ea958ba5-4e58-4405-bf42-6e3be15d4c3a
    name: ALF
    name-ref: ALF
    ns-instance-config-ref: f48163a6-c807-47bc-9682-f72caef5af85
    nsd:
        _admin:
            created: 1566823353.971486
            modified: 1566823353.971486
            onboardingState: ONBOARDED
            operationalState: ENABLED
            projects_read:
            - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
            projects_write:
            - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
            storage:
                descriptor: hackfest_3charmed_nsd/hackfest_3charmed_nsd.yaml
                folder: 8c2f8b95-bb1b-47ee-8001-36dc090678da
                fs: local
                path: /app/storage/
                pkg-dir: hackfest_3charmed_nsd
                zipfile: package.tar.gz
            usageState: NOT_IN_USE
            userDefinedData: {}
        _id: 8c2f8b95-bb1b-47ee-8001-36dc090678da
        constituent-vnfd:
        -   member-vnf-index: '1'
            vnfd-id-ref: hackfest3charmed-vnf
        -   member-vnf-index: '2'
            vnfd-id-ref: hackfest3charmed-vnf
        description: NS with 2 VNFs hackfest3charmed-vnf connected by datanet and
            mgmtnet VLs
        id: hackfest3charmed-ns
        logo: osm.png
        name: hackfest3charmed-ns
        short-name: hackfest3charmed-ns
        version: '1.0'
        vld:
        -   id: mgmt
            mgmt-network: true
            name: mgmt
            short-name: mgmt
            type: ELAN
            vim-network-name: mgmt
            vnfd-connection-point-ref:
            -   member-vnf-index-ref: '1'
                vnfd-connection-point-ref: vnf-mgmt
                vnfd-id-ref: hackfest3charmed-vnf
            -   member-vnf-index-ref: '2'
                vnfd-connection-point-ref: vnf-mgmt
                vnfd-id-ref: hackfest3charmed-vnf
        -   id: datanet
            name: datanet
            short-name: datanet
            type: ELAN
            vnfd-connection-point-ref:
            -   member-vnf-index-ref: '1'
                vnfd-connection-point-ref: vnf-data
                vnfd-id-ref: hackfest3charmed-vnf
            -   member-vnf-index-ref: '2'
                vnfd-connection-point-ref: vnf-data
                vnfd-id-ref: hackfest3charmed-vnf
    nsd-id: 8c2f8b95-bb1b-47ee-8001-36dc090678da
    nsd-name-ref: hackfest3charmed-ns
    nsd-ref: hackfest3charmed-ns
    operational-events: []
    operational-status: failed
    orchestration-progress: {}
    resource-orchestrator: osmopenmano
    short-name: ALF
    ssh-authorized-key: null
    vld:
    -   id: mgmt
        name: null
        status: ACTIVE
        status-detailed: null
        vim-id: f99ae780-0e2f-4985-af41-574eae6919c0
        vim-network-name: mgmt
    -   id: datanet
        name: ALF-datanet
        status: ACTIVE
        status-detailed: null
        vim-id: c31364ba-f573-4ab6-bf1a-fed30ede39a8
    vnfd-id:
    - 7637bcf8-cf14-42dc-ad70-c66fcf1e6e77
"""

db_nslcmops_text = """
---
-   _admin:
        created: 1566823354.4148262
        modified: 1566823354.4148262
        projects_read:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        projects_write:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        worker: 86434c2948e2
    _id: a639fac7-e0bb-4225-8ecb-c1f8efcc125e
    detailed-status: 'FAILED executing proxy charm initial primitives for member_vnf_index=1
        vdu_id=None: charm error executing primitive verify-ssh-credentials for member_vnf_index=1
        vdu_id=None: ''timeout after 600 seconds'''
    id: a639fac7-e0bb-4225-8ecb-c1f8efcc125e
    isAutomaticInvocation: false
    isCancelPending: false
    lcmOperationType: instantiate
    links:
        nsInstance: /osm/nslcm/v1/ns_instances/f48163a6-c807-47bc-9682-f72caef5af85
        self: /osm/nslcm/v1/ns_lcm_op_occs/a639fac7-e0bb-4225-8ecb-c1f8efcc125e
    nsInstanceId: f48163a6-c807-47bc-9682-f72caef5af85
    operationParams:
        additionalParamsForVnf:
        -   additionalParams:
                touch_filename: /home/ubuntu/first-touch-1
                touch_filename2: /home/ubuntu/second-touch-1
            member-vnf-index: '1'
        -   additionalParams:
                touch_filename: /home/ubuntu/first-touch-2
                touch_filename2: /home/ubuntu/second-touch-2
            member-vnf-index: '2'
        lcmOperationType: instantiate
        nsDescription: default description
        nsInstanceId: f48163a6-c807-47bc-9682-f72caef5af85
        nsName: ALF
        nsdId: 8c2f8b95-bb1b-47ee-8001-36dc090678da
        vimAccountId: ea958ba5-4e58-4405-bf42-6e3be15d4c3a
    operationState: FAILED
    startTime: 1566823354.414689
    statusEnteredTime: 1566824534.5112448
"""

db_vnfrs_text = """
---
-   _admin:
        created: 1566823354.3668208
        modified: 1566823354.3668208
        nsState: NOT_INSTANTIATED
        projects_read:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        projects_write:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
    _id: 88d90b0c-faff-4b9f-bccd-017f33985984
    additionalParamsForVnf:
        touch_filename: /home/ubuntu/first-touch-1
        touch_filename2: /home/ubuntu/second-touch-1
    connection-point:
    -   connection-point-id: vnf-mgmt
        id: vnf-mgmt
        name: vnf-mgmt
    -   connection-point-id: vnf-data
        id: vnf-data
        name: vnf-data
    created-time: 1566823354.36234
    id: 88d90b0c-faff-4b9f-bccd-017f33985984
    ip-address: 10.205.1.46
    member-vnf-index-ref: '1'
    nsr-id-ref: f48163a6-c807-47bc-9682-f72caef5af85
    vdur:
    -   _id: f0e7d7ce-2443-4dcb-ad0b-5ab9f3b13d37
        count-index: 0
        interfaces:
        -   ip-address: 10.205.1.46
            mac-address: fa:16:3e:b4:3e:b1
            mgmt-vnf: true
            name: mgmtVM-eth0
            ns-vld-id: mgmt
        -   ip-address: 192.168.54.2
            mac-address: fa:16:3e:6e:7e:78
            name: mgmtVM-eth1
            vnf-vld-id: internal
        internal-connection-point:
        -   connection-point-id: mgmtVM-internal
            id: mgmtVM-internal
            name: mgmtVM-internal
        ip-address: 10.205.1.46
        name: ALF-1-mgmtVM-1
        status: ACTIVE
        status-detailed: null
        vdu-id-ref: mgmtVM
        vim-id: c2538499-4c30-41c0-acd5-80cb92f48061
    -   _id: ab453219-2d9a-45c2-864d-2c0788385028
        count-index: 0
        interfaces:
        -   ip-address: 192.168.54.3
            mac-address: fa:16:3e:d9:7a:5d
            name: dataVM-eth0
            vnf-vld-id: internal
        -   ip-address: 192.168.24.3
            mac-address: fa:16:3e:d1:6c:0d
            name: dataVM-xe0
            ns-vld-id: datanet
        internal-connection-point:
        -   connection-point-id: dataVM-internal
            id: dataVM-internal
            name: dataVM-internal
        ip-address: null
        name: ALF-1-dataVM-1
        status: ACTIVE
        status-detailed: null
        vdu-id-ref: dataVM
        vim-id: 87973c3f-365d-4227-95c2-7a8abc74349c
    vim-account-id: ea958ba5-4e58-4405-bf42-6e3be15d4c3a
    vld:
    -   id: internal
        name: ALF-internal
        status: ACTIVE
        status-detailed: null
        vim-id: ff181e6d-2597-4244-b40b-bb0174bdfeb6
    vnfd-id: 7637bcf8-cf14-42dc-ad70-c66fcf1e6e77
    vnfd-ref: hackfest3charmed-vnf
-   _admin:
        created: 1566823354.3703845
        modified: 1566823354.3703845
        nsState: NOT_INSTANTIATED
        projects_read:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        projects_write:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
    _id: 1ca3bb1a-b29b-49fe-bed6-5f3076d77434
    additionalParamsForVnf:
        touch_filename: /home/ubuntu/first-touch-2
        touch_filename2: /home/ubuntu/second-touch-2
    connection-point:
    -   connection-point-id: vnf-mgmt
        id: vnf-mgmt
        name: vnf-mgmt
    -   connection-point-id: vnf-data
        id: vnf-data
        name: vnf-data
    created-time: 1566823354.36234
    id: 1ca3bb1a-b29b-49fe-bed6-5f3076d77434
    ip-address: 10.205.1.47
    member-vnf-index-ref: '2'
    nsr-id-ref: f48163a6-c807-47bc-9682-f72caef5af85
    vdur:
    -   _id: 190b4a2c-4f85-4cfe-9406-4cef7ffb1e67
        count-index: 0
        interfaces:
        -   ip-address: 10.205.1.47
            mac-address: fa:16:3e:cb:9f:c7
            mgmt-vnf: true
            name: mgmtVM-eth0
            ns-vld-id: mgmt
        -   ip-address: 192.168.231.1
            mac-address: fa:16:3e:1a:89:24
            name: mgmtVM-eth1
            vnf-vld-id: internal
        internal-connection-point:
        -   connection-point-id: mgmtVM-internal
            id: mgmtVM-internal
            name: mgmtVM-internal
        ip-address: 10.205.1.47
        name: ALF-2-mgmtVM-1
        status: ACTIVE
        status-detailed: null
        vdu-id-ref: mgmtVM
        vim-id: 248077b2-e3b8-4a37-8b72-575abb8ed912
    -   _id: 889b874d-e1c3-4e75-aa45-53a9b0ddabd9
        count-index: 0
        interfaces:
        -   ip-address: 192.168.231.3
            mac-address: fa:16:3e:7e:ba:8c
            name: dataVM-eth0
            vnf-vld-id: internal
        -   ip-address: 192.168.24.4
            mac-address: fa:16:3e:d2:e1:f5
            name: dataVM-xe0
            ns-vld-id: datanet
        internal-connection-point:
        -   connection-point-id: dataVM-internal
            id: dataVM-internal
            name: dataVM-internal
        ip-address: null
        name: ALF-2-dataVM-1
        status: ACTIVE
        status-detailed: null
        vdu-id-ref: dataVM
        vim-id: a4ce4372-e0ad-4ae3-8f9f-1c969f32e77b
    vim-account-id: ea958ba5-4e58-4405-bf42-6e3be15d4c3a
    vld:
    -   id: internal
        name: ALF-internal
        status: ACTIVE
        status-detailed: null
        vim-id: ff181e6d-2597-4244-b40b-bb0174bdfeb6
    vnfd-id: 7637bcf8-cf14-42dc-ad70-c66fcf1e6e77
    vnfd-ref: hackfest3charmed-vnf
"""

ro_ns_text = """
datacenter_tenant_id: dc5c67fa-c7f2-11e9-b9c0-02420aff0004
description: null
name: ALF
nets:
-   created: false
    datacenter_id: dc51ce6c-c7f2-11e9-b9c0-02420aff0004
    datacenter_tenant_id: dc5c67fa-c7f2-11e9-b9c0-02420aff0004
    error_msg: null
    ns_net_osm_id: mgmt
    related: c6bac394-fa27-4c43-bb34-42f621a9d343
    sce_net_id: 8f215bab-c35e-41e6-a035-42bfaa07af9f
    sdn_net_id: null
    status: ACTIVE
    uuid: c6bac394-fa27-4c43-bb34-42f621a9d343
    vim_info: "{vim_info: null}"
    vim_name: null
    vim_net_id: f99ae780-0e2f-4985-af41-574eae6919c0
    vnf_net_id: null
    vnf_net_osm_id: null
-   created: true
    datacenter_id: dc51ce6c-c7f2-11e9-b9c0-02420aff0004
    datacenter_tenant_id: dc5c67fa-c7f2-11e9-b9c0-02420aff0004
    error_msg: null
    ns_net_osm_id: datanet
    related: 509d576c-120f-493a-99a1-5fea99dfe041
    sce_net_id: 3d766bbc-33a8-41aa-a986-2f35e8d25c16
    sdn_net_id: null
    status: ACTIVE
    uuid: 509d576c-120f-493a-99a1-5fea99dfe041
    vim_info: "{vim_info: null}"
    vim_name: ALF-datanet
    vim_net_id: c31364ba-f573-4ab6-bf1a-fed30ede39a8
    vnf_net_id: null
    vnf_net_osm_id: null
-   created: true
    datacenter_id: dc51ce6c-c7f2-11e9-b9c0-02420aff0004
    datacenter_tenant_id: dc5c67fa-c7f2-11e9-b9c0-02420aff0004
    error_msg: null
    ns_net_osm_id: null
    related: 277fed09-3220-4bfd-9052-b96b21a32daf
    sce_net_id: null
    sdn_net_id: null
    status: ACTIVE
    uuid: 277fed09-3220-4bfd-9052-b96b21a32daf
    vim_info: "{vim_info: null}"
    vim_name: ALF-internal
    vim_net_id: ff181e6d-2597-4244-b40b-bb0174bdfeb6
    vnf_net_id: 62e62fae-c12b-4ebc-9a9b-30031c6c16fa
    vnf_net_osm_id: internal
-   created: true
    datacenter_id: dc51ce6c-c7f2-11e9-b9c0-02420aff0004
    datacenter_tenant_id: dc5c67fa-c7f2-11e9-b9c0-02420aff0004
    error_msg: null
    ns_net_osm_id: null
    related: 92534d1a-e697-4372-a84d-aa0aa643b68a
    sce_net_id: null
    sdn_net_id: null
    status: ACTIVE
    uuid: 92534d1a-e697-4372-a84d-aa0aa643b68a
    vim_info: "{vim_info: null}"
    vim_name: ALF-internal
    vim_net_id: 09655387-b639-421a-b5f6-72b26d685fb4
    vnf_net_id: 13c6c77d-86a5-4914-832c-990d4ec7b54e
    vnf_net_osm_id: internal
nsd_osm_id: f48163a6-c807-47bc-9682-f72caef5af85.2.hackfest3charmed-ns
scenario_id: 876573b5-968d-40b9-b52b-91bf5c5844f7
scenario_name: hackfest3charmed-ns
sfis: []
sfps: []
sfs: []
tenant_id: 0ea38bd0-2729-47a9-ae07-c6ce76115eb2
uuid: c9fe9908-3180-430d-b633-fca2f68db008
vnfs:
-   datacenter_id: dc51ce6c-c7f2-11e9-b9c0-02420aff0004
    datacenter_tenant_id: dc5c67fa-c7f2-11e9-b9c0-02420aff0004
    ip_address: 10.205.1.46
    member_vnf_index: '1'
    mgmt_access: '{interface_id: 61549ee3-cd6c-4930-8b90-eaad97fe345b, required: ''False'',
        vm_id: 6cf4a48f-3b6c-4395-8221-119fa37de24a}

        '
    sce_vnf_id: 83be04a8-c513-42ba-9908-22728f686d31
    uuid: 94724042-7576-4fb0-82ec-6a7ab642741c
    vms:
    -   created_at: '2019-08-26T12:50:38'
        error_msg: null
        interfaces:
        -   external_name: vnf-mgmt
            instance_net_id: c6bac394-fa27-4c43-bb34-42f621a9d343
            internal_name: mgmtVM-eth0
            ip_address: 10.205.1.46
            mac_address: fa:16:3e:b4:3e:b1
            sdn_port_id: null
            type: mgmt
            vim_info: "{vim_info: null}"
            vim_interface_id: 4d3cb8fd-7040-4169-a0ad-2486d2b006a1
        -   external_name: null
            instance_net_id: 277fed09-3220-4bfd-9052-b96b21a32daf
            internal_name: mgmtVM-eth1
            ip_address: 192.168.54.2
            mac_address: fa:16:3e:6e:7e:78
            sdn_port_id: null
            type: bridge
            vim_info: "{vim_info: null}"
            vim_interface_id: 54ed68e2-9802-4dfe-b68a-280b3fc6e02d
        ip_address: 10.205.1.46
        name: mgmtVM
        related: d0b91293-a91d-4f08-b15f-0bf841216dfe
        status: ACTIVE
        uuid: d0b91293-a91d-4f08-b15f-0bf841216dfe
        vdu_osm_id: mgmtVM
        vim_info: "{vim_info: null}"
        vim_name: ALF-1-mgmtVM-1
        vim_vm_id: c2538499-4c30-41c0-acd5-80cb92f48061
    -   created_at: '2019-08-26T12:50:38'
        error_msg: null
        interfaces:
        -   external_name: null
            instance_net_id: 277fed09-3220-4bfd-9052-b96b21a32daf
            internal_name: dataVM-eth0
            ip_address: 192.168.54.3
            mac_address: fa:16:3e:d9:7a:5d
            sdn_port_id: null
            type: bridge
            vim_info: "{vim_info: null}"
            vim_interface_id: 1637f350-8840-4241-8ed0-4616bdcecfcf
        -   external_name: vnf-data
            instance_net_id: 509d576c-120f-493a-99a1-5fea99dfe041
            internal_name: dataVM-xe0
            ip_address: 192.168.24.3
            mac_address: fa:16:3e:d1:6c:0d
            sdn_port_id: null
            type: bridge
            vim_info: "{vim_info: null}"
            vim_interface_id: 54c73e83-7059-41fe-83a9-4c4ae997b481
        name: dataVM
        related: 5c08253d-8a35-474f-b0d3-c5297d174c13
        status: ACTIVE
        uuid: 5c08253d-8a35-474f-b0d3-c5297d174c13
        vdu_osm_id: dataVM
        vim_info: "{vim_info: null}"
        vim_name: ALF-1-dataVM-1
        vim_vm_id: 87973c3f-365d-4227-95c2-7a8abc74349c
    -   created_at: '2019-08-26T13:40:54'
        error_msg: null
        interfaces:
        -   external_name: null
            instance_net_id: 277fed09-3220-4bfd-9052-b96b21a32daf
            internal_name: dataVM-eth0
            ip_address: 192.168.54.5
            mac_address: fa:16:3e:e4:17:45
            sdn_port_id: null
            type: bridge
            vim_info: "{vim_info: null}"
            vim_interface_id: 7e246e40-8710-4c33-9c95-78fc3c02bc5b
        -   external_name: vnf-data
            instance_net_id: 509d576c-120f-493a-99a1-5fea99dfe041
            internal_name: dataVM-xe0
            ip_address: 192.168.24.5
            mac_address: fa:16:3e:29:6f:a6
            sdn_port_id: null
            type: bridge
            vim_info: "{vim_info: null}"
            vim_interface_id: ce81af7a-9adf-494b-950e-6581fd04ecc4
        name: dataVM
        related: 1ae5a0a2-c15a-49a4-a77c-2991d97f6dbe
        status: ACTIVE
        uuid: 1ae5a0a2-c15a-49a4-a77c-2991d97f6dbe
        vdu_osm_id: dataVM
        vim_info: "{vim_info: null}"
        vim_name: ALF-1-dataVM-2
        vim_vm_id: 4916533e-36c6-4861-9fe3-366a8fb0a5f8
    vnf_id: 1ab2a418-9fe3-4358-bf17-411e5155535f
    vnf_name: hackfest3charmed-vnf.1
    vnfd_osm_id: f48163a6-c807-47bc-9682-f72caef5af85.0.1
-   datacenter_id: dc51ce6c-c7f2-11e9-b9c0-02420aff0004
    datacenter_tenant_id: dc5c67fa-c7f2-11e9-b9c0-02420aff0004
    ip_address: 10.205.1.47
    member_vnf_index: '2'
    mgmt_access: '{interface_id: 538604c3-5c5e-41eb-8f84-c0239c7fabcd, required: ''False'',
        vm_id: dd04d792-05c9-4ecc-bf28-f77384d00311}

        '
    sce_vnf_id: c4f3607a-08ff-4f75-893c-fce507e2f240
    uuid: 00020403-e80f-4ef2-bb7e-b29669643035
    vms:
    -   created_at: '2019-08-26T12:50:38'
        error_msg: null
        interfaces:
        -   external_name: vnf-mgmt
            instance_net_id: c6bac394-fa27-4c43-bb34-42f621a9d343
            internal_name: mgmtVM-eth0
            ip_address: 10.205.1.47
            mac_address: fa:16:3e:cb:9f:c7
            sdn_port_id: null
            type: mgmt
            vim_info: "{vim_info: null}"
            vim_interface_id: dcd6d2de-3c68-481c-883e-e9d38c671dc4
        -   external_name: null
            instance_net_id: 92534d1a-e697-4372-a84d-aa0aa643b68a
            internal_name: mgmtVM-eth1
            ip_address: 192.168.231.1
            mac_address: fa:16:3e:1a:89:24
            sdn_port_id: null
            type: bridge
            vim_info: "{vim_info: null}"
            vim_interface_id: 50e538e3-aba0-4652-93bb-20487f3f28e1
        ip_address: 10.205.1.47
        name: mgmtVM
        related: 4543ab5d-578c-427c-9df2-affd17e21b66
        status: ACTIVE
        uuid: 4543ab5d-578c-427c-9df2-affd17e21b66
        vdu_osm_id: mgmtVM
        vim_info: "{vim_info: null}"
        vim_name: ALF-2-mgmtVM-1
        vim_vm_id: 248077b2-e3b8-4a37-8b72-575abb8ed912
    -   created_at: '2019-08-26T12:50:38'
        error_msg: null
        interfaces:
        -   external_name: null
            instance_net_id: 92534d1a-e697-4372-a84d-aa0aa643b68a
            internal_name: dataVM-eth0
            ip_address: 192.168.231.3
            mac_address: fa:16:3e:7e:ba:8c
            sdn_port_id: null
            type: bridge
            vim_info: "{vim_info: null}"
            vim_interface_id: 15274862-14ea-4527-b405-101cae8bc1a0
        -   external_name: vnf-data
            instance_net_id: 509d576c-120f-493a-99a1-5fea99dfe041
            internal_name: dataVM-xe0
            ip_address: 192.168.24.4
            mac_address: fa:16:3e:d2:e1:f5
            sdn_port_id: null
            type: bridge
            vim_info: "{vim_info: null}"
            vim_interface_id: 253ebe4e-38d5-46be-8777-dbb57510a2ec
        name: dataVM
        related: 6f03f16b-295a-47a1-9a69-2d069d574a33
        status: ACTIVE
        uuid: 6f03f16b-295a-47a1-9a69-2d069d574a33
        vdu_osm_id: dataVM
        vim_info: "{vim_info: null}"
        vim_name: ALF-2-dataVM-1
        vim_vm_id: a4ce4372-e0ad-4ae3-8f9f-1c969f32e77b
    vnf_id: 0de348e3-c201-4f6a-91cc-7f957e2d5504
    vnf_name: hackfest3charmed-vnf.2
    vnfd_osm_id: f48163a6-c807-47bc-9682-f72caef5af85.1.2
"""


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

    def _n2vc_CreateExecutionEnvironment(self, namespace):
        k_list = namespace.split(".")
        ee_id = k_list[1] + "."
        if len(k_list) >= 2:
            for k in k_list[2:4]:
                ee_id += k[:8]
        else:
            ee_id += "_NS_"
        return ee_id

    def _ro_show(self, *args, **kwargs):
        ro_ns_desc = yaml.load(ro_ns_text)

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
                "nsrs": yaml.load(db_nsrs_text),
                "nslcmops": yaml.load(db_nslcmops_text),
                "vnfrs": yaml.load(db_vnfrs_text),
                "vnfds": yaml.load(db_vnfds_text),
                "vim_accounts": yaml.load(db_vim_accounts_text),
            }
            self.db_vim_accounts = yaml.load(db_vim_accounts_text)

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
            self.my_ns.n2vc.CreateExecutionEnvironment = asynctest.CoroutineMock(
                side_effect=self._n2vc_CreateExecutionEnvironment)
            self.my_ns.n2vc.InstallConfigurationSW = asynctest.CoroutineMock(return_value=pub_key)
            self.my_ns.n2vc.ExecutePrimitive = asynctest.CoroutineMock(side_effect=self._return_uuid)
            self.my_ns.n2vc.GetPrimitiveStatus = asynctest.CoroutineMock(return_value="completed")
            self.my_ns.n2vc.GetPrimitiveOutput = asynctest.CoroutineMock(return_value={"result": "ok",
                                                                                       "pubkey": pub_key})

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

    @asynctest.fail_on(active_handles=True)   # all async tasks must be completed
    async def test_scale(self):
        pass


if __name__ == '__main__':
    asynctest.main()
