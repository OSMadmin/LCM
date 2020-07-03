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
from osm_lcm.prometheus import Prometheus, initial_prometheus_data
from asynctest.mock import Mock
from osm_common.dbmemory import DbMemory

__author__ = 'Alfonso Tierno <alfonso.tiernosepulveda@telefonica.com>'


class TestPrometheus(asynctest.TestCase):

    async def setUp(self):
        config = {'uri': 'http:prometheus:9090',
                  'path': '/etc/prometheus'}
        self.db = Mock(DbMemory())
        self.p = Prometheus(config, worker_id='1', db=self.db, loop=self.loop)

    @asynctest.fail_on(active_handles=True)
    async def test_start(self):
        # test with database empty
        self.db.get_one.return_value = False
        self.p.update = asynctest.CoroutineMock()
        await self.p.start()
        self.db.create.assert_called_once_with('admin', initial_prometheus_data)
        self.p.update.assert_called_once_with()

        # test with database not empty
        self.db.create.reset_mock()
        self.db.get_one.return_value = initial_prometheus_data
        self.p.update.reset_mock()
        await self.p.start()
        self.db.create.assert_not_called()
        self.p.update.assert_called_once_with()

    @asynctest.fail_on(active_handles=True)
    async def test_update(self):
        self.p.PROMETHEUS_LOCKED_TIME = 1
        number_call_set_one = 0

        def _db_set_one(*args, **kwargs):
            # simulated that database is not locked at first call
            nonlocal number_call_set_one

            number_call_set_one += 1
            if number_call_set_one == 1:
                return
            else:
                return {'update': 1}

        def _check_set_one_calls(set_one_calls):
            # check the three calls to database set_one
            self.assertEqual(len(set_one_calls), 3, 'Not called three times to db.set_one, two blocks, one unblock')
            self.assertIn('admin', set_one_calls[0][0], 'db.set_one collection should be admin')
            first_used_time = set_one_calls[0][1]['update_dict']['_admin.locked_at']
            second_used_time = set_one_calls[1][1]['update_dict']['_admin.locked_at']
            third_used_time = set_one_calls[2][1]['update_dict']['_admin.locked_at']
            self.assertTrue(first_used_time != 0 and second_used_time != 0, 'blocking locked_at time must not be 0')
            self.assertGreater(second_used_time, first_used_time,
                               'Every blocking try must contain a new locked_at time')
            self.assertEqual(third_used_time, 0, 'For unblocking must be set locked_at=0')

        # check add_jobs
        number_call_set_one = 0
        self.db.get_one.return_value = initial_prometheus_data
        self.db.set_one.side_effect = _db_set_one
        self.p.send_data = asynctest.CoroutineMock(return_value=True)
        add_jobs = {'job1': {'job_name': 'job1', 'nsr_id': 'nsr_id'}}
        await self.p.update(add_jobs=add_jobs)
        set_one_calls = self.db.set_one.call_args_list
        _check_set_one_calls(set_one_calls)
        update_dict = set_one_calls[2][1]['update_dict']
        unset_dict = set_one_calls[2][1]['unset']
        expected_final_set = {
            '_admin.locked_at': 0,
            '_admin.locked_by': None,
            '_admin.modified_at': set_one_calls[1][1]['update_dict']['_admin.locked_at'],
            'scrape_configs.job1': add_jobs['job1']}
        self.assertEqual(update_dict, expected_final_set, 'invalid set and unlock values')
        self.assertEqual(unset_dict, None, 'invalid unset and unlock values')

        # check remove_jobs
        number_call_set_one = 0
        remove_jobs = ['job1']
        self.db.set_one.reset_mock()
        await self.p.update(remove_jobs=remove_jobs)
        set_one_calls = self.db.set_one.call_args_list
        _check_set_one_calls(set_one_calls)
        update_dict = set_one_calls[2][1]['update_dict']
        unset_dict = set_one_calls[2][1]['unset']
        expected_final_set = {
            '_admin.locked_at': 0,
            '_admin.locked_by': None,
            '_admin.modified_at': set_one_calls[1][1]['update_dict']['_admin.locked_at']
        }
        self.assertEqual(update_dict, expected_final_set, 'invalid set and unlock values')
        self.assertEqual(unset_dict, {'scrape_configs.job1': None}, 'invalid unset and unlock values')

    def test_parse_job(self):
        text_to_parse = """
            # yaml format with jinja2
            key1: "parsing var1='{{ var1 }}'"
            key2: "parsing var2='{{ var2 }}'"
        """
        vars = {'var1': 'VAR1', 'var2': 'VAR2', 'var3': 'VAR3'}
        expected = {
            'key1': "parsing var1='VAR1'",
            'key2': "parsing var2='VAR2'"
        }
        result = self.p.parse_job(text_to_parse, vars)
        self.assertEqual(result, expected, 'Error at jinja2 parse')


if __name__ == '__main__':
    asynctest.main()
