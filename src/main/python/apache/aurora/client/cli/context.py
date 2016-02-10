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

from __future__ import print_function

import functools
import logging
from fnmatch import fnmatch

from apache.aurora.client.api import AuroraClientAPI, SchedulerProxy
from apache.aurora.client.base import AURORA_V2_USER_AGENT_NAME, combine_messages
from apache.aurora.client.cli import (
    EXIT_API_ERROR,
    EXIT_AUTH_ERROR,
    EXIT_COMMAND_FAILURE,
    EXIT_INVALID_CONFIGURATION,
    EXIT_INVALID_PARAMETER,
    Context
)
from apache.aurora.client.config import get_config
from apache.aurora.client.hooks.hooked_api import HookedAuroraClientAPI
from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.common.clusters import CLUSTERS

from gen.apache.aurora.api import AuroraAdmin
from gen.apache.aurora.api.constants import ACTIVE_STATES
from gen.apache.aurora.api.ttypes import ResponseCode


class AuthErrorHandlingScheduler(object):
  """A decorator that can be applied on a AuroraClientAPI instance to add handling of
  auth-related errors, terminating the client."""

  def __init__(self, delegate):
    self._delegate = delegate

  def __getattr__(self, method_name):
    try:
      method = getattr(AuroraAdmin.Client, method_name)
    except AttributeError:
      # Don't interfere with the non-public API.
      return getattr(self._delegate, method_name)
    if not callable(method):
      return method

    @functools.wraps(method)
    def method_wrapper(*args, **kwargs):
      try:
        return getattr(self._delegate, method_name)(*args, **kwargs)
      except SchedulerProxy.AuthError as e:
        raise Context.CommandError(EXIT_AUTH_ERROR, str(e))

    return method_wrapper


def add_auth_error_handler(api):
  api._scheduler_proxy = AuthErrorHandlingScheduler(api._scheduler_proxy)
  return api


class AuroraCommandContext(Context):

  LOCK_ERROR_MSG = """Error: job is locked by an active update.
      Run 'aurora update abort' or wait for the active update to finish."""

  """A context object used by Aurora commands to manage command processing state
  and common operations.
  """

  def __init__(self):
    super(AuroraCommandContext, self).__init__()
    self.apis = {}
    self.unhooked_apis = {}

  def get_api(self, cluster, enable_hooks=True, clusters=CLUSTERS):
    """Gets an API object for a specified cluster
    Keeps the API handle cached, so that only one handle for each cluster will be created in a
    session.
    """
    if cluster not in clusters:
      raise self.CommandError(EXIT_INVALID_CONFIGURATION, "Unknown cluster: %s" % cluster)

    apis = self.apis if enable_hooks else self.unhooked_apis
    base_class = HookedAuroraClientAPI if enable_hooks else AuroraClientAPI

    if cluster not in apis:
      api = base_class(clusters[cluster], AURORA_V2_USER_AGENT_NAME, verbose=False)
      apis[cluster] = api
    return add_auth_error_handler(apis[cluster])

  def get_job_config_optional(self, jobkey, config_file):
    """Loads a job configuration if provided."""
    return self.get_job_config(jobkey, config_file) if config_file is not None else None

  def get_job_config(self, jobkey, config_file):
    """Loads a job configuration from a config file."""
    jobname = jobkey.name
    try:
      # TODO(mchucarroll): pull request to pystachio, to make it possible to log the loaded
      # file without double-reading.
      with open(config_file, "r") as fp:
        logging.debug("Config: %s" % fp.readlines())
      result = get_config(
          jobname,
          config_file,
          self.options.read_json,
          self.options.bindings,
          select_cluster=jobkey.cluster,
          select_role=jobkey.role,
          select_env=jobkey.env)
      check_result = result.raw().check()
      if not check_result.ok():
        raise self.CommandError(EXIT_INVALID_CONFIGURATION, check_result)
      return result
    except Exception as e:
      raise self.CommandError(EXIT_INVALID_CONFIGURATION, 'Error loading configuration: %s' % e)

  def log_response_and_raise(self, resp, err_code=EXIT_API_ERROR, err_msg="Command failure:"):
    if resp.responseCode == ResponseCode.OK:
      msg = combine_messages(resp)
      if msg:
        logging.info(msg)
    else:
      self.print_err(err_msg)
      self.print_err("\t%s" % combine_messages(resp))
      if resp.responseCode == ResponseCode.LOCK_ERROR:
        self.print_err("\t%s" % self.LOCK_ERROR_MSG)
      raise self.CommandErrorLogged(err_code, err_msg)

  def get_job_list(self, clusters, role=None):
    """Get a list of jobs from a group of clusters.
    :param clusters: the clusters to query for jobs
    :param role: if specified, only return jobs for the role; otherwise, return all jobs.
    """
    result = []
    if '*' in role:
      role = None
    for cluster in clusters:
      api = self.get_api(cluster)
      resp = api.get_jobs(role)
      self.log_response_and_raise(resp, err_code=EXIT_COMMAND_FAILURE)
      result.extend([AuroraJobKey(cluster, job.key.role, job.key.environment, job.key.name)
          for job in resp.result.getJobsResult.configs])
    return result

  def get_jobs_matching_key(self, key):
    """Finds all jobs matching a key containing wildcard segments.
    This is potentially slow!
    TODO(mchucarroll): insert a warning to users about slowness if the key contains wildcards!
    """
    def is_fully_bound(key):
      """Helper that checks if a key contains wildcards."""
      return not any('*' in component for component in [key.cluster, key.role, key.env, key.name])

    def filter_job_list(jobs, role, env, name):
      """Filter a list of jobs to get just the jobs that match the pattern from a key"""
      return [job for job in jobs if fnmatch(job.role, role) and fnmatch(job.env, env)
          and fnmatch(job.name, name)]

    # For cluster, we can expand the list of things we're looking for directly.
    # For other key elements, we need to just get a list of the jobs on the clusters, and filter
    # it for things that match.
    if key.cluster == '*':
      clusters_to_search = CLUSTERS
    else:
      clusters_to_search = [key.cluster]
    if is_fully_bound(key):
      return [AuroraJobKey(key.cluster, key.role, key.env, key.name)]
    else:
      jobs = filter_job_list(self.get_job_list(clusters_to_search, key.role),
          key.role, key.env, key.name)
      return jobs

  def get_active_tasks(self, key):
    """Returns a list of the currently active tasks of a job

    :param key: Job key
    :type key: AuroraJobKey
    :return: set of active tasks
    """
    api = self.get_api(key.cluster)
    resp = api.query_no_configs(
        api.build_query(key.role, key.name, env=key.env, statuses=ACTIVE_STATES))
    self.log_response_and_raise(resp, err_code=EXIT_INVALID_PARAMETER)
    return resp.result.scheduleStatusResult.tasks

  def get_active_instances_or_raise(self, key, instances):
    """Same as get_active_instances but raises error if
       any of the requested instances are not active.

    :param key: Job key
    :type key: AuroraJobKey
    :param instances: instances to verify
    :type instances: list of int
    :return: set of all currently active instances
    """
    active = set(task.assignedTask.instanceId for task in self.get_active_tasks(key) or [])
    unrecognized = set(instances) - active
    if unrecognized:
      raise self.CommandError(EXIT_INVALID_PARAMETER,
          "Invalid instance parameter: %s" % (list(unrecognized)))
    return active
