#!/usr/bin/python3
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

from os import path
from time import time, sleep
from sys import stderr

""" This module is used for helth check. A file called time_last_ping is used 
This contains the last time where something is received from kafka
"""


def health_check(health_check_file=None, ping_interval_pace=120):
    health_check_file = health_check_file or path.expanduser("~") + "/time_last_ping"
    retry = 2
    while retry:
        retry -= 1
        try:
            with open(health_check_file, "r") as f:
                last_received_ping = f.read()

            if time() - float(last_received_ping) < 2 * ping_interval_pace:  # allow one ping not received every two
                exit(0)
        except Exception as e:
            print(e, file=stderr)
        if retry:
            sleep(6)
    exit(1)


if __name__ == '__main__':
    health_check()
