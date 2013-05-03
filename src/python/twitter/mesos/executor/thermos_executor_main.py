import os

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.common.metrics import CompoundMetrics
from twitter.common.metrics.sampler import DiskMetricWriter

from twitter.mesos.executor.executor_detector import ExecutorDetector
from twitter.mesos.executor.executor_vars import ExecutorVars
from twitter.mesos.executor.task_runner_wrapper import (
    ProductionTaskRunner,
    AngrybirdTaskRunner)
from twitter.mesos.executor.thermos_executor import (
    ThermosExecutor,
    ThermosExecutorTimer)

import mesos


app.configure(module='twitter.common_internal.app.modules.chickadee_handler',
    service_name='thermos_executor')
app.configure(debug=True)


LogOptions.set_simple(True)
LogOptions.set_disk_log_level('DEBUG')

if 'ANGRYBIRD_THERMOS' in os.environ:
  LogOptions.set_log_dir(os.path.join(os.environ['ANGRYBIRD_THERMOS'], 'thermos/log'))
else:
  # locate logs locally in executor sandbox
  LogOptions.set_log_dir(ExecutorDetector.LOG_PATH)


def main():
  # Create executor stub
  thermos_executor = ThermosExecutor()
  executor_vars = ExecutorVars()

  compound_collector = CompoundMetrics(thermos_executor.metrics, executor_vars.metrics)

  # Start metrics collection
  metric_writer = DiskMetricWriter(compound_collector, ExecutorDetector.VARS_PATH)
  metric_writer.start()

  # Create driver stub
  driver = mesos.MesosExecutorDriver(thermos_executor)

  # This is an ephemeral executor -- shutdown if we receive no tasks within a certain
  # time period
  ThermosExecutorTimer(thermos_executor, driver).start()

  # Start executor.
  driver.run()

  log.info('MesosExecutorDriver.run() has finished.')


app.main()
