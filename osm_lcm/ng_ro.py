#!/usr/bin/env python3
# -*- coding: utf-8 -*-

##
# Copyright 2020 Telefónica Investigación y Desarrollo, S.A.U.
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
##

"""
asyncio RO python client to interact with New Generation RO server
"""

import asyncio
import aiohttp
import yaml
import logging

__author__ = "Alfonso Tierno <alfonso.tiernosepulveda@telefonica.com"
__date__ = "$09-Jan-2018 09:09:48$"
__version__ = "0.1.2"
version_date = "2020-05-08"


class NgRoException(Exception):
    def __init__(self, message, http_code=400):
        """Common Exception for all RO client exceptions"""
        self.http_code = http_code
        Exception.__init__(self, message)


class NgRoClient:
    headers_req = {'Accept': 'application/yaml', 'content-type': 'application/yaml'}
    client_to_RO = {'tenant': 'tenants', 'vim': 'datacenters', 'vim_account': 'datacenters', 'sdn': 'sdn_controllers',
                    'vnfd': 'vnfs', 'nsd': 'scenarios', 'wim': 'wims', 'wim_account': 'wims',
                    'ns': 'instances'}
    mandatory_for_create = {
        'tenant': ("name", ),
        'vnfd': ("name", "id"),
        'nsd': ("name", "id"),
        'ns': ("name", "scenario", "datacenter"),
        'vim': ("name", "vim_url"),
        'wim': ("name", "wim_url"),
        'vim_account': (),
        'wim_account': (),
        'sdn': ("name", 'type'),
    }
    timeout_large = 120
    timeout_short = 30

    def __init__(self, loop, uri, **kwargs):
        self.loop = loop
        self.endpoint_url = uri
        if not self.endpoint_url.endswith("/"):
            self.endpoint_url += "/"
        if not self.endpoint_url.startswith("http"):
            self.endpoint_url = "http://" + self.endpoint_url

        self.username = kwargs.get("username")
        self.password = kwargs.get("password")
        self.tenant_id_name = kwargs.get("tenant")
        self.tenant = None
        self.datacenter_id_name = kwargs.get("datacenter")
        self.datacenter = None
        logger_name = kwargs.get('logger_name', 'lcm.ro')
        self.logger = logging.getLogger(logger_name)
        if kwargs.get("loglevel"):
            self.logger.setLevel(kwargs["loglevel"])

    async def deploy(self, nsr_id, target):
        """
        Performs an action over an item
        :param item: can be 'tenant', 'vnfd', 'nsd', 'ns', 'vim', 'vim_account', 'sdn'
        :param item_id_name: RO id or name of the item. Raise and exception if more than one found
        :param descriptor: can be a dict, or a yaml/json text. Autodetect unless descriptor_format is provided
        :param descriptor_format: Can be 'json' or 'yaml'
        :param kwargs: Overrides descriptor with values as name, description, vim_url, vim_url_admin, vim_type
               keys can be a dot separated list to specify elements inside dict
        :return: dictionary with the information or raises NgRoException on Error
        """
        try:
            if isinstance(target, str):
                target = self._parse_yaml(target)
            payload_req = yaml.safe_dump(target)

            url = "{}/ns/v1/deploy/{nsr_id}".format(self.endpoint_url, nsr_id=nsr_id)
            async with aiohttp.ClientSession(loop=self.loop) as session:
                self.logger.debug("NG-RO POST %s %s", url, payload_req)
                # timeout = aiohttp.ClientTimeout(total=self.timeout_large)
                async with session.post(url, headers=self.headers_req, data=payload_req) as response:
                    response_text = await response.read()
                    self.logger.debug("POST {} [{}] {}".format(url, response.status, response_text[:100]))
                    if response.status >= 300:
                        raise NgRoException(response_text, http_code=response.status)
                    return self._parse_yaml(response_text, response=True)
        except (aiohttp.ClientOSError, aiohttp.ClientError) as e:
            raise NgRoException(e, http_code=504)
        except asyncio.TimeoutError:
            raise NgRoException("Timeout", http_code=504)

    async def status(self, nsr_id, action_id):
        try:
            url = "{}/ns/v1/deploy/{nsr_id}/{action_id}".format(self.endpoint_url, nsr_id=nsr_id, action_id=action_id)
            async with aiohttp.ClientSession(loop=self.loop) as session:
                self.logger.debug("GET %s", url)
                # timeout = aiohttp.ClientTimeout(total=self.timeout_short)
                async with session.get(url, headers=self.headers_req) as response:
                    response_text = await response.read()
                    self.logger.debug("GET {} [{}] {}".format(url, response.status, response_text[:100]))
                    if response.status >= 300:
                        raise NgRoException(response_text, http_code=response.status)
                    return self._parse_yaml(response_text, response=True)

        except (aiohttp.ClientOSError, aiohttp.ClientError) as e:
            raise NgRoException(e, http_code=504)
        except asyncio.TimeoutError:
            raise NgRoException("Timeout", http_code=504)

    async def delete(self, nsr_id):
        try:
            url = "{}/ns/v1/deploy/{nsr_id}".format(self.endpoint_url, nsr_id=nsr_id)
            async with aiohttp.ClientSession(loop=self.loop) as session:
                self.logger.debug("DELETE %s", url)
                # timeout = aiohttp.ClientTimeout(total=self.timeout_short)
                async with session.delete(url, headers=self.headers_req) as response:
                    self.logger.debug("DELETE {} [{}]".format(url, response.status))
                    if response.status >= 300:
                        raise NgRoException("Delete {}".format(nsr_id), http_code=response.status)
                    return

        except (aiohttp.ClientOSError, aiohttp.ClientError) as e:
            raise NgRoException(e, http_code=504)
        except asyncio.TimeoutError:
            raise NgRoException("Timeout", http_code=504)

    async def get_version(self):
        """
        Obtain RO server version.
        :return: a list with integers ["major", "minor", "release"]. Raises NgRoException on Error,
        """
        try:
            response_text = ""
            async with aiohttp.ClientSession(loop=self.loop) as session:
                url = "{}/version".format(self.endpoint_url)
                self.logger.debug("RO GET %s", url)
                # timeout = aiohttp.ClientTimeout(total=self.timeout_short)
                async with session.get(url, headers=self.headers_req) as response:
                    response_text = await response.read()
                    self.logger.debug("GET {} [{}] {}".format(url, response.status, response_text[:100]))
                    if response.status >= 300:
                        raise NgRoException(response_text, http_code=response.status)

                for word in str(response_text).split(" "):
                    if "." in word:
                        version_text, _, _ = word.partition("-")
                        return version_text
                raise NgRoException("Got invalid version text: '{}'".format(response_text), http_code=500)
        except (aiohttp.ClientOSError, aiohttp.ClientError) as e:
            raise NgRoException(e, http_code=504)
        except asyncio.TimeoutError:
            raise NgRoException("Timeout", http_code=504)
        except Exception as e:
            raise NgRoException("Got invalid version text: '{}'; causing exception {}".format(response_text, e),
                                http_code=500)

    @staticmethod
    def _parse_yaml(descriptor, response=False):
        try:
            return yaml.safe_load(descriptor)
        except yaml.YAMLError as exc:
            error_pos = ""
            if hasattr(exc, 'problem_mark'):
                mark = exc.problem_mark
                error_pos = " at line:{} column:{}s".format(mark.line + 1, mark.column + 1)
            error_text = "yaml format error" + error_pos
            if response:
                raise NgRoException("reponse with " + error_text)
            raise NgRoException(error_text)
