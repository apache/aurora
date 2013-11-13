"""Command-line entry point to the Thermos Executor

This module wraps the Thermos Executor into an executable suitable for launching by a Mesos
slave.

"""

import os

from twitter.common import app, log
from twitter.common.log.options import LogOptions

from twitter.aurora.executor.common.executor_timeout import ExecutorTimeout
from twitter.aurora.executor.thermos_task_runner import ThermosTaskRunner
from twitter.aurora.executor.thermos_executor import ThermosExecutor

import mesos


# TODO(wickman) Consider just having the OSS version require pip installed
# thermos_runner binaries on every machine and instead of embedding the pex
# as a resource, shell out to one on the PATH.

class ProductionTaskRunner(ThermosTaskRunner):
  _RUNNER_PEX_DUMPED = False

  def __init__(self, *args, **kwargs):
    kwargs.update(artifact_dir=os.path.realpath('.'))
    super(ProductionTaskRunner, self).__init__(*args, **kwargs)

  @property
  def runner_pex(self):
    if not self._RUNNER_PEX_DUMPED:
      import pkg_resources
      import twitter.aurora.executor.resources
      runner_pex = os.path.join(os.path.realpath('.'), self.PEX_NAME)
      with open(runner_pex, 'w') as fp:
        fp.write(pkg_resources.resource_stream(twitter.aurora.executor.resources.__name__,
          self.PEX_NAME).read())
      self.__runner_pex = runner_pex
      self._RUNNER_PEX_DUMPED = True
    return self.__runner_pex


def main():
  # Create executor stub
  thermos_executor = ThermosExecutor(runner_class=ProductionTaskRunner)

  # Create driver stub
  driver = mesos.MesosExecutorDriver(thermos_executor)

  # This is an ephemeral executor -- shutdown if we receive no tasks within a certain
  # time period
  ExecutorTimeout(thermos_executor.launched, driver).start()

  # Start executor
  driver.run()

  log.info('MesosExecutorDriver.run() has finished.')


app.configure(debug=True)
LogOptions.set_simple(True)
LogOptions.set_disk_log_level('DEBUG')
LogOptions.set_log_dir('.')

app.main()
