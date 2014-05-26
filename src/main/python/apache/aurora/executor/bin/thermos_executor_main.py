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

"""Command-line entry point to the Thermos Executor

This module wraps the Thermos Executor into an executable suitable for launching by a Mesos
slave.

"""

import os

import mesos
from twitter.common import app, log
from twitter.common.log.options import LogOptions

from apache.aurora.executor.aurora_executor import AuroraExecutor
from apache.aurora.executor.common.executor_timeout import ExecutorTimeout
from apache.aurora.executor.common.health_checker import HealthCheckerProvider
from apache.aurora.executor.thermos_task_runner import DefaultThermosTaskRunnerProvider

app.configure(debug=True)
LogOptions.set_simple(True)
LogOptions.set_disk_log_level('DEBUG')
LogOptions.set_log_dir('.')


# TODO(wickman) Consider just having the OSS version require pip installed
# thermos_runner binaries on every machine and instead of embedding the pex
# as a resource, shell out to one on the PATH.
def dump_runner_pex():
  import pkg_resources
  import apache.aurora.executor.resources
  pex_name = 'thermos_runner.pex'
  runner_pex = os.path.join(os.path.realpath('.'), pex_name)
  with open(runner_pex, 'w') as fp:
    # TODO(wickman) Use shutil.copyfileobj to reduce memory footprint here.
    fp.write(pkg_resources.resource_stream(
        apache.aurora.executor.resources.__name__, pex_name).read())
  return runner_pex


def proxy_main():
  def main():
    thermos_runner_provider = DefaultThermosTaskRunnerProvider(
        dump_runner_pex(),
        artifact_dir=os.path.realpath('.'),
    )

    # Create executor stub
    thermos_executor = AuroraExecutor(
        runner_provider=thermos_runner_provider,
        status_providers=(HealthCheckerProvider(),),
    )

    # Create driver stub
    driver = mesos.MesosExecutorDriver(thermos_executor)

    # This is an ephemeral executor -- shutdown if we receive no tasks within a certain
    # time period
    ExecutorTimeout(thermos_executor.launched, driver).start()

    # Start executor
    driver.run()

    log.info('MesosExecutorDriver.run() has finished.')

  app.main()
