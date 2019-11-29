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
-   _admin:
        created: 1575031728.9257665
        modified: 1575031728.9257665
        onboardingState: ONBOARDED
        operationalState: ENABLED
        projects_read:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        projects_write:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        storage:
            descriptor: multikdu_ns/multikdu_nsd.yaml
            folder: d0f63683-9032-4c6f-8928-ffd4674b9f69
            fs: local
            path: /app/storage/
            pkg-dir: multikdu_ns
            zipfile: multikdu_ns.tar.gz
        usageState: NOT_IN_USE
        userDefinedData: {}
    _id: d0f63683-9032-4c6f-8928-ffd4674b9f69
    constituent-vnfd:
    -   member-vnf-index: multikdu
        vnfd-id-ref: multikdu_knf
    description: NS consisting of a single KNF multikdu_knf connected to mgmt network
    id: multikdu_ns
    logo: osm.png
    name: multikdu_ns
    short-name: multikdu_ns
    vendor: OSM
    version: '1.0'
    vld:
    -   id: mgmtnet
        mgmt-network: true
        name: mgmtnet
        type: ELAN
        vim-network-name: mgmt
        vnfd-connection-point-ref:
        -   member-vnf-index-ref: multikdu
            vnfd-connection-point-ref: mgmt
            vnfd-id-ref: multikdu_knf
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
        operations:
        -   member_vnf_index: '1'
            primitive: touch
            primitive_params: /home/ubuntu/last-touch-1
            operationState: COMPLETED
            detailed-status: Done
        -   member_vnf_index: '1'
            primitive: touch
            primitive_params: /home/ubuntu/last-touch-2
            operationState: COMPLETED
            detailed-status: Done
        -   member_vnf_index: '2'
            primitive: touch
            primitive_params: /home/ubuntu/last-touch-3
            operationState: FAILED
            detailed-status: Unknown error
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
-   _admin:
        created: 1575034637.044651
        modified: 1575034637.044651
        projects_read:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        projects_write:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
    _id: cf3aa178-7640-4174-b921-2330e6f2aad6
    detailed-status: done
    id: cf3aa178-7640-4174-b921-2330e6f2aad6
    isAutomaticInvocation: false
    isCancelPending: false
    lcmOperationType: instantiate
    links:
        nsInstance: /osm/nslcm/v1/ns_instances/0bcb701c-ee4d-41ab-8ee6-f4156f7f114d
        self: /osm/nslcm/v1/ns_lcm_op_occs/cf3aa178-7640-4174-b921-2330e6f2aad6
    nsInstanceId: 0bcb701c-ee4d-41ab-8ee6-f4156f7f114d
    operationParams:
        lcmOperationType: instantiate
        nsDescription: default description
        nsInstanceId: 0bcb701c-ee4d-41ab-8ee6-f4156f7f114d
        nsName: multikdu
        nsdId: d0f63683-9032-4c6f-8928-ffd4674b9f69
        nsr_id: 0bcb701c-ee4d-41ab-8ee6-f4156f7f114d
        vimAccountId: 74337dcb-ef54-41e7-bd2d-8c0d7fcd326f
        vld:
        -   name: mgmtnet
            vim-network-name: internal
    operationState: COMPLETED
    startTime: 1575034637.0445576
    statusEnteredTime: 1575034663.8484545
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
-   _admin:
        created: 1575034637.011233
        current-operation: null
        deployed:
            K8s:
            -   k8scluster-uuid: 73d96432-d692-40d2-8440-e0c73aee209c
                kdu-instance: stable-mongodb-0086856106
                kdu-model: stable/mongodb
                kdu-name: mongo
                vnfr-id: 5ac34899-a23a-4b3c-918a-cd77acadbea6
            -   k8scluster-uuid: 73d96432-d692-40d2-8440-e0c73aee209c
                kdu-instance: stable-openldap-0092830263
                kdu-model: stable/mongodb
                kdu-name: mongo
                vnfr-id: 5ac34899-a23a-4b3c-918a-cd77acadbea6
            RO:
                detailed-status: Deployed at VIM
                nsd_id: b03a8de8-1898-4142-bc6d-3b0787df567d
                nsr_id: b5ce3e00-8647-415d-afaa-d5a612cf3074
                nsr_status: ACTIVE
                operational-status: running
                vnfd:
                -   id: b9493dae-a4c9-4b96-8965-329581efb0a1
                    member-vnf-index: multikdu
            VCA: []
        modified: 1575034637.011233
        nsState: INSTANTIATED
        nslcmop: null
        operation-type: null
        projects_read:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        projects_write:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
    _id: 0bcb701c-ee4d-41ab-8ee6-f4156f7f114d
    additionalParamsForNs: null
    admin-status: ENABLED
    config-status: configured
    constituent-vnfr-ref:
    - 5ac34899-a23a-4b3c-918a-cd77acadbea6
    create-time: 1575034636.9990137
    datacenter: ea958ba5-4e58-4405-bf42-6e3be15d4c3a
    description: default description
    detailed-status: done
    id: 0bcb701c-ee4d-41ab-8ee6-f4156f7f114d
    instantiate_params:
        nsDescription: default description
        nsName: multikdu
        nsdId: d0f63683-9032-4c6f-8928-ffd4674b9f69
        vimAccountId: 74337dcb-ef54-41e7-bd2d-8c0d7fcd326f
    name: multikdu
    name-ref: multikdu
    ns-instance-config-ref: 0bcb701c-ee4d-41ab-8ee6-f4156f7f114d
    nsd-id: d0f63683-9032-4c6f-8928-ffd4674b9f69
    nsd-name-ref: multikdu_ns
    nsd-ref: multikdu_ns
    operational-events: []
    operational-status: init
    orchestration-progress: {}
    resource-orchestrator: osmopenmano
    short-name: multikdu
    ssh-authorized-key: null
    vld:
    -   id: mgmtnet
        name: null
        status: ACTIVE
        status-detailed: null
        vim-id: 9b6a2ac4-767e-4ec9-9497-8ba63084c77f
        vim-network-name: mgmt
    vnfd-id:
    - 7ab0d10d-8ce2-4c68-aef6-cc5a437a9c62
"""

db_ro_ns_text = """
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

db_k8sclusters_text = """
-   _admin:
        created: 1575031378.9268339
        current_operation: 0
        modified: 1575031378.9268339
        operationalState: ENABLED
        operations:
        -   detailed-status: ''
            lcmOperationType: create
            operationParams: null
            operationState: ''
            startTime: 1575031378.926895
            statusEnteredTime: 1575031378.926895
            worker: 36681ccf7f32
        projects_read:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        projects_write:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        helm-chart:
            id: 73d96432-d692-40d2-8440-e0c73aee209c
            created: True
    _id: e7169dab-f71a-4f1f-b82b-432605e8c4b3
    credentials:
        apiVersion: v1
        users:
        -   name: admin
            user:
                password: qhpdogJXhBLG+JiYyyE0LeNsJXHkCSMy+sGVzlnJqes=
                username: admin
    description: Cluster3
    k8s_version: '1.15'
    name: cluster3
    namespace: kube-system
    nets:
        net1: None
    schema_version: '1.11'
    vim_account: ea958ba5-4e58-4405-bf42-6e3be15d4c3a
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
-   _admin:
        created: 1575031727.5383403
        modified: 1575031727.5383403
        onboardingState: ONBOARDED
        operationalState: ENABLED
        projects_read:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        projects_write:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        storage:
            descriptor: multikdu_knf/multikdu_vnfd.yaml
            folder: 7ab0d10d-8ce2-4c68-aef6-cc5a437a9c62
            fs: local
            path: /app/storage/
            pkg-dir: multikdu_knf
            zipfile: multikdu_knf.tar.gz
        usageState: NOT_IN_USE
        userDefinedData: {}
    _id: 7ab0d10d-8ce2-4c68-aef6-cc5a437a9c62
    connection-point:
    -   name: mgmt
    description: KNF with two KDU using helm-charts
    id: multikdu_knf
    k8s-cluster:
        nets:
        -   external-connection-point-ref: mgmt
            id: mgmtnet
    kdu:
    -   helm-chart: stable/openldap:1.2.1
        name: ldap
    -   helm-chart: stable/mongodb
        name: mongo
    mgmt-interface:
        cp: mgmt
    name: multikdu_knf
    short-name: multikdu_knf
    vendor: Telefonica
    version: '1.0'
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
-   _admin:
        created: 1575034637.009597
        modified: 1575034637.009597
        nsState: NOT_INSTANTIATED
        projects_read:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
        projects_write:
        - 25b5aebf-3da1-49ed-99de-1d2b4a86d6e4
    _id: 5ac34899-a23a-4b3c-918a-cd77acadbea6
    additionalParamsForVnf: null
    connection-point:
    -   connection-point-id: null
        id: null
        name: mgmt
    created-time: 1575034636.9990137
    id: 5ac34899-a23a-4b3c-918a-cd77acadbea6
    ip-address: null
    k8s-cluster:
        nets:
        -   external-connection-point-ref: mgmt
            id: mgmtnet
            ns-vld-id: mgmtnet
            vim_net: internal
    kdur:
    -   ip-address: null
        k8s-cluster:
            id: e7169dab-f71a-4f1f-b82b-432605e8c4b3
        kdu-name: ldap
        helm-chart: stable/openldap:1.2.1
    -   ip-address: null
        k8s-cluster:
            id: e7169dab-f71a-4f1f-b82b-432605e8c4b3
        kdu-name: mongo
        helm-chart: stable/mongodb
    member-vnf-index-ref: multikdu
    nsr-id-ref: 0bcb701c-ee4d-41ab-8ee6-f4156f7f114d
    vdur: []
    vim-account-id: 74337dcb-ef54-41e7-bd2d-8c0d7fcd326f
    vnfd-id: 7ab0d10d-8ce2-4c68-aef6-cc5a437a9c62
    vnfd-ref: multikdu_knf
"""

db_nslcmops_scale_text = """
---
-   _admin:
      created: 1565250912.2643092
      modified: 1570026174.83263
      projects_read:
      - d3581c99-31e3-45f9-b45c-49a290faedbc
      current_operation: '5'
      deployed:
        RO: d9aea288-b9b1-11e9-b19e-02420aff0006
        RO-account: d9bb2f1c-b9b1-11e9-b19e-02420aff0006
      detailed-status: Done
      modified: 1565250912.2643092
      operationalState: ENABLED
      operations:
      - member_vnf_index: '1'
        primitive: touch
        primitive_params: /home/ubuntu/last-touch-1
        operationState: COMPLETED
        detailed-status: Done
      - member_vnf_index: '1'
        primitive: touch
        primitive_params: /home/ubuntu/last-touch-2
        operationState: COMPLETED
        detailed-status: Done
      - member_vnf_index: '2'
        primitive: touch
        primitive_params: /home/ubuntu/last-touch-3
        operationState: COMPLETED
        detailed-status: Done
      projects_read:
      - b2d2ce4b-a1a0-4c01-847e-048632c43b40
      projects_write:
      - b2d2ce4b-a1a0-4c01-847e-048632c43b40
      worker: c4055a07655b
      deploy:
        RO: ACTION-1570026232.061742
    _id: 053967e8-7c1c-400f-ae82-3d45b291374b
    lcmOperationType: scale
    nsInstanceId: 90d9ebb7-2b5a-4b7c-bc34-a51fd7ef7b7b
    statusEnteredTime: 1570026243.09784
    startTime: 1570026174.8326
    operationParams:
      lcmOperationType: scale
      nsInstanceId: 90d9ebb7-2b5a-4b7c-bc34-a51fd7ef7b7b
      scaleVnfData:
        scaleByStepData:
          member-vnf-index: '1'
          scaling-group-descriptor: scale_scaling_group
        scaleVnfType: SCALE_IN
      scaleType: SCALE_VNF
    isAutomaticInvocation: false
    isCancelPending: false
    id: 053967e8-7c1c-400f-ae82-3d45b291374b
    links:
      nsInstance: "/osm/nslcm/v1/ns_instances/90d9ebb7-2b5a-4b7c-bc34-a51fd7ef7b7b"
      self: "/osm/nslcm/v1/ns_lcm_op_occs/053967e8-7c1c-400f-ae82-3d45b291374b"
    operationState: COMPLETED
    detailed-status: done
"""
