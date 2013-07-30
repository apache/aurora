"""Command-line entry point to the Thermos GC executor

This module wraps the Thermos GC executor into an executable suitable for launching by a Mesos
slave.

"""

import os

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.common_internal.app.modules import chickadee_handler
from twitter.common.metrics.sampler import DiskMetricWriter
from twitter.thermos.base.path import TaskPath

from .executor_detector import ExecutorDetector
from .executor_vars import ExecutorVars
from .gc_executor import ThermosGCExecutor

import mesos


app.configure(module='twitter.common_internal.app.modules.chickadee_handler',
    service_name='thermos_gc_executor')
app.configure(debug=True)

LogOptions.set_disk_log_level('DEBUG')

# locate logs locally in executor sandbox
LogOptions.set_log_dir(ExecutorDetector.LOG_PATH)


def main():
  checkpoint_root, mesos_root = TaskPath.DEFAULT_CHECKPOINT_ROOT, None
  if 'META_THERMOS_ROOT' in os.environ:
    mesos_root = os.environ['META_THERMOS_ROOT']
    checkpoint_root = os.path.join(mesos_root, 'checkpoints')

  # Create executor stub
  thermos_gc_executor = ThermosGCExecutor(checkpoint_root=checkpoint_root, mesos_root=mesos_root)
  thermos_gc_executor.start()

  # Start metrics collection
  metric_writer = DiskMetricWriter(thermos_gc_executor.metrics, ExecutorDetector.VARS_PATH)
  metric_writer.start()

  # Create driver stub
  driver = mesos.MesosExecutorDriver(thermos_gc_executor)

  #Start GC executor
  driver.run()

  log.info('MesosExecutorDriver.run() has finished.')


app.main()
