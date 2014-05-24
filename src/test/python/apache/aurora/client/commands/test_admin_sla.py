#
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import contextlib
from collections import defaultdict

from mock import Mock, patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client.api import AuroraClientAPI
from apache.aurora.client.api.sla import DomainUpTimeSlaVector
from apache.aurora.client.commands.admin import sla_list_safe_domain, sla_probe_hosts
from apache.aurora.client.commands.util import AuroraClientCommandTest
from apache.aurora.common.aurora_job_key import AuroraJobKey

MIN_INSTANCE_COUNT = 1


class TestAdminSlaListSafeDomainCommand(AuroraClientCommandTest):

  @classmethod
  def setup_mock_options(cls, exclude=None, include=None, override=None,
                         exclude_list=None, include_list=None, list_jobs=False):
    mock_options = Mock(spec=['exclude_filename', 'exclude_hosts', 'include_filename',
        'include_hosts', 'override_filename', 'list_jobs', 'verbosity', 'disable_all_hooks',
        'min_instance_count'])

    mock_options.exclude_filename = exclude
    mock_options.exclude_hosts = exclude_list
    mock_options.include_filename = include
    mock_options.include_hosts = include_list
    mock_options.override_filename = override
    mock_options.list_jobs = list_jobs
    mock_options.verbosity = False
    mock_options.disable_all_hooks = False
    mock_options.min_instance_count = MIN_INSTANCE_COUNT
    return mock_options

  @classmethod
  def create_hosts(cls, num_hosts, percentage, duration):
    hosts = defaultdict(list)
    for i in range(num_hosts):
      host_name = 'h%s' % i
      job = AuroraJobKey.from_path('west/role/env/job%s' % i)
      hosts[host_name].append(DomainUpTimeSlaVector.JobUpTimeLimit(job, percentage, duration))
    return hosts

  @classmethod
  def create_mock_vector(cls, result):
    mock_vector = Mock(spec=DomainUpTimeSlaVector)
    mock_vector.get_safe_hosts.return_value = result
    return mock_vector

  def test_safe_domain_no_options(self):
    """Tests successful execution of the sla_list_safe_domain command without extra options."""
    mock_options = self.setup_mock_options()
    mock_vector = self.create_mock_vector(self.create_hosts(3, 80, 100))
    with contextlib.nested(
        patch('apache.aurora.client.commands.admin.AuroraClientAPI', new=Mock(spec=AuroraClientAPI)),
        patch('apache.aurora.client.commands.admin.print_results'),
        patch('apache.aurora.client.commands.admin.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)
    ) as (
        mock_api,
        mock_print_results,
        test_clusters,
        mock_options):

      mock_api.return_value.sla_get_safe_domain_vector.return_value = mock_vector
      sla_list_safe_domain(['west', '50', '100s'])

      mock_vector.get_safe_hosts.assert_called_once_with(50.0, 100.0, {})
      mock_print_results.assert_called_once_with(['h0', 'h1', 'h2'])

  def test_safe_domain_exclude_hosts(self):
    """Test successful execution of the sla_list_safe_domain command with exclude hosts option."""
    mock_vector = self.create_mock_vector(self.create_hosts(3, 80, 100))
    with temporary_file() as fp:
      fp.write('h1')
      fp.flush()
      mock_options = self.setup_mock_options(exclude=fp.name)
      with contextlib.nested(
          patch('apache.aurora.client.commands.admin.AuroraClientAPI', new=Mock(spec=AuroraClientAPI)),
          patch('apache.aurora.client.commands.admin.print_results'),
          patch('apache.aurora.client.commands.admin.CLUSTERS', new=self.TEST_CLUSTERS),
          patch('twitter.common.app.get_options', return_value=mock_options)
      ) as (
          mock_api,
          mock_print_results,
          test_clusters,
          mock_options):

        mock_api.return_value.sla_get_safe_domain_vector.return_value = mock_vector

        sla_list_safe_domain(['west', '50', '100s'])

        mock_vector.get_safe_hosts.assert_called_once_with(50.0, 100.0, {})
        mock_print_results.assert_called_once_with(['h0', 'h2'])

  def test_safe_domain_exclude_hosts_from_list(self):
    """Test successful execution of the sla_list_safe_domain command with exclude list option."""
    mock_vector = self.create_mock_vector(self.create_hosts(3, 80, 100))
    mock_options = self.setup_mock_options(exclude_list=','.join(['h0', 'h1']))
    with contextlib.nested(
        patch('apache.aurora.client.commands.admin.AuroraClientAPI', new=Mock(spec=AuroraClientAPI)),
        patch('apache.aurora.client.commands.admin.print_results'),
        patch('apache.aurora.client.commands.admin.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)
    ) as (
        mock_api,
        mock_print_results,
        test_clusters,
        mock_options):

      mock_api.return_value.sla_get_safe_domain_vector.return_value = mock_vector

      sla_list_safe_domain(['west', '50', '100s'])

      mock_vector.get_safe_hosts.assert_called_once_with(50.0, 100.0, {})
      mock_print_results.assert_called_once_with(['h2'])

  def test_safe_domain_include_hosts(self):
    """Test successful execution of the sla_list_safe_domain command with include hosts option."""
    mock_vector = self.create_mock_vector(self.create_hosts(1, 80, 100))
    hostname = 'h0'
    with temporary_file() as fp:
      fp.write(hostname)
      fp.flush()
      mock_options = self.setup_mock_options(include=fp.name)
      with contextlib.nested(
          patch('apache.aurora.client.commands.admin.AuroraClientAPI', new=Mock(spec=AuroraClientAPI)),
          patch('apache.aurora.client.commands.admin.print_results'),
          patch('apache.aurora.client.commands.admin.CLUSTERS', new=self.TEST_CLUSTERS),
          patch('twitter.common.app.get_options', return_value=mock_options)
      ) as (
          mock_api,
          mock_print_results,
          test_clusters,
          mock_options):

        mock_api.return_value.sla_get_safe_domain_vector.return_value = mock_vector

        sla_list_safe_domain(['west', '50', '100s'])

        mock_api.return_value.sla_get_safe_domain_vector.assert_called_once_with(
            MIN_INSTANCE_COUNT, [hostname])
        mock_vector.get_safe_hosts.assert_called_once_with(50.0, 100.0, {})
        mock_print_results.assert_called_once_with([hostname])

  def test_safe_domain_include_hosts_from_list(self):
    """Test successful execution of the sla_list_safe_domain command with include list option."""
    mock_vector = self.create_mock_vector(self.create_hosts(2, 80, 100))
    hosts = ['h0', 'h1']
    mock_options = self.setup_mock_options(include_list=','.join(hosts))
    with contextlib.nested(
        patch('apache.aurora.client.commands.admin.AuroraClientAPI', new=Mock(spec=AuroraClientAPI)),
        patch('apache.aurora.client.commands.admin.print_results'),
        patch('apache.aurora.client.commands.admin.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)
    ) as (
        mock_api,
        mock_print_results,
        test_clusters,
        mock_options):

      mock_api.return_value.sla_get_safe_domain_vector.return_value = mock_vector

      sla_list_safe_domain(['west', '50', '100s'])

      mock_api.return_value.sla_get_safe_domain_vector.assert_called_once_with(
          MIN_INSTANCE_COUNT, hosts)
      mock_vector.get_safe_hosts.assert_called_once_with(50.0, 100.0, {})
      mock_print_results.assert_called_once_with(hosts)

  def test_safe_domain_override_jobs(self):
    """Test successful execution of the sla_list_safe_domain command with override_jobs option."""
    mock_vector = self.create_mock_vector(self.create_hosts(3, 80, 100))
    with temporary_file() as fp:
      fp.write('west/role/env/job1 30 200s')
      fp.flush()
      mock_options = self.setup_mock_options(override=fp.name)
      with contextlib.nested(
          patch('apache.aurora.client.commands.admin.AuroraClientAPI', new=Mock(spec=AuroraClientAPI)),
          patch('apache.aurora.client.commands.admin.print_results'),
          patch('apache.aurora.client.commands.admin.CLUSTERS', new=self.TEST_CLUSTERS),
          patch('twitter.common.app.get_options', return_value=mock_options)
      ) as (
          mock_api,
          mock_print_results,
          test_clusters,
          mock_options):

        mock_api.return_value.sla_get_safe_domain_vector.return_value = mock_vector

        sla_list_safe_domain(['west', '50', '100s'])

        job_key = AuroraJobKey.from_path('west/role/env/job1')
        override = {job_key: DomainUpTimeSlaVector.JobUpTimeLimit(job_key, 30, 200)}
        mock_vector.get_safe_hosts.assert_called_once_with(50.0, 100.0, override)
        mock_print_results.assert_called_once_with(['h0', 'h1', 'h2'])

  def test_safe_domain_list_jobs(self):
    """Tests successful execution of the sla_list_safe_domain command with list_jobs option."""
    mock_options = self.setup_mock_options(list_jobs=True)
    mock_vector = self.create_mock_vector(self.create_hosts(3, 50, 100))
    with contextlib.nested(
        patch('apache.aurora.client.commands.admin.AuroraClientAPI', new=Mock(spec=AuroraClientAPI)),
        patch('apache.aurora.client.commands.admin.print_results'),
        patch('apache.aurora.client.commands.admin.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)
    ) as (
        mock_api,
        mock_print_results,
        test_clusters,
        mock_options):

      mock_api.return_value.sla_get_safe_domain_vector.return_value = mock_vector
      sla_list_safe_domain(['west', '50', '100s'])

      mock_vector.get_safe_hosts.assert_called_once_with(50.0, 100.0, {})
      mock_print_results.assert_called_once_with([
          'h0\twest/role/env/job0\t50.00\t100',
          'h1\twest/role/env/job1\t50.00\t100',
          'h2\twest/role/env/job2\t50.00\t100'])

  def test_safe_domain_invalid_percentage(self):
    """Tests execution of the sla_list_safe_domain command with invalid percentage"""
    mock_options = self.setup_mock_options()
    with patch('twitter.common.app.get_options', return_value=mock_options) as (mock_options):

      try:
        sla_list_safe_domain(['west', '0', '100s'])
      except SystemExit:
        pass
      else:
        assert 'Expected error is not raised.'

  def test_safe_domain_malformed_job_override(self):
    """Tests execution of the sla_list_safe_domain command with invalid job_override file"""
    with temporary_file() as fp:
      fp.write('30 200s')
      fp.flush()
      mock_options = self.setup_mock_options(override=fp.name)
      with patch('twitter.common.app.get_options', return_value=mock_options) as (mock_options):

        try:
          sla_list_safe_domain(['west', '50', '100s'])
        except SystemExit:
          pass
        else:
          assert 'Expected error is not raised.'

  def test_safe_domain_hosts_error(self):
    """Tests execution of the sla_list_safe_domain command with both include file and list"""
    mock_options = self.setup_mock_options(include='file', include_list='list')
    with patch('twitter.common.app.get_options', return_value=mock_options) as (mock_options):

      try:
        sla_list_safe_domain(['west', '50', '100s'])
      except SystemExit:
        pass
      else:
        assert 'Expected error is not raised.'


class TestAdminSlaProbeHostsCommand(AuroraClientCommandTest):

  @classmethod
  def setup_mock_options(cls, hosts=None, filename=None):
    mock_options = Mock()
    mock_options.hosts = hosts
    mock_options.filename = filename
    mock_options.verbosity = False
    return mock_options

  @classmethod
  def create_mock_vector(cls, result):
    mock_vector = Mock(spec=DomainUpTimeSlaVector)
    mock_vector.probe_hosts.return_value = result
    return mock_vector

  @classmethod
  def create_probe_hosts(cls, num_hosts, predicted, safe, safe_in):
    hosts = defaultdict(list)
    for i in range(num_hosts):
      host_name = 'h%s' % i
      job = AuroraJobKey.from_path('west/role/env/job%s' % i)
      hosts[host_name].append(DomainUpTimeSlaVector.JobUpTimeDetails(job, predicted, safe, safe_in))
    return hosts

  def test_probe_hosts_with_list(self):
    """Tests successful execution of the sla_probe_hosts command with host list."""
    hosts = ['h0', 'h1']
    mock_options = self.setup_mock_options(hosts=','.join(hosts))
    mock_vector = self.create_mock_vector(self.create_probe_hosts(2, 80, True, 0))
    with contextlib.nested(
        patch('apache.aurora.client.commands.admin.AuroraClientAPI', new=Mock(spec=AuroraClientAPI)),
        patch('apache.aurora.client.commands.admin.print_results'),
        patch('apache.aurora.client.commands.admin.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)
    ) as (
        mock_api,
        mock_print_results,
        test_clusters,
        options):

      mock_api.return_value.sla_get_safe_domain_vector.return_value = mock_vector
      sla_probe_hosts(['west', '90', '200s'])

      mock_api.return_value.sla_get_safe_domain_vector.assert_called_once_with(
          mock_options.min_instance_count, hosts)
      mock_vector.probe_hosts.assert_called_once_with(90.0, 200.0)
      mock_print_results.assert_called_once_with([
          'h0\twest/role/env/job0\t80.00\tTrue\t0',
          'h1\twest/role/env/job1\t80.00\tTrue\t0'
      ])

  def test_probe_hosts_with_file(self):
    """Tests successful execution of the sla_probe_hosts command with host filename."""
    mock_vector = self.create_mock_vector(self.create_probe_hosts(1, 80, False, None))
    with temporary_file() as fp:
      fp.write('h0')
      fp.flush()
      mock_options = self.setup_mock_options(filename=fp.name)
      with contextlib.nested(
          patch('apache.aurora.client.commands.admin.AuroraClientAPI', new=Mock(spec=AuroraClientAPI)),
          patch('apache.aurora.client.commands.admin.print_results'),
          patch('apache.aurora.client.commands.admin.CLUSTERS', new=self.TEST_CLUSTERS),
          patch('twitter.common.app.get_options', return_value=mock_options)
      ) as (
          mock_api,
          mock_print_results,
          test_clusters,
          options):

        mock_api.return_value.sla_get_safe_domain_vector.return_value = mock_vector
        sla_probe_hosts(['west', '90', '200s'])

        mock_api.return_value.sla_get_safe_domain_vector.assert_called_once_with(
          mock_options.min_instance_count, ['h0'])
        mock_vector.probe_hosts.assert_called_once_with(90.0, 200.0)
        mock_print_results.assert_called_once_with([
            'h0\twest/role/env/job0\t80.00\tFalse\tn/a'
        ])

  def test_probe_hosts_error(self):
    """Tests execution of the sla_probe_hosts command with both host and filename provided."""
    with temporary_file() as fp:
      fp.write('h0')
      fp.flush()
      mock_options = self.setup_mock_options(hosts='h0', filename=fp.name)
      with patch('twitter.common.app.get_options', return_value=mock_options) as (mock_options):

        try:
          sla_probe_hosts(['west', '50', '100s'])
        except SystemExit:
          pass
        else:
          assert 'Expected error is not raised.'
