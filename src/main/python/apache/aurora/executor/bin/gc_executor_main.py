"""Command-line entry point to the Thermos GC executor

This module wraps the Thermos GC executor into an executable suitable for launching by a Mesos
slave.

"""

from twitter.aurora.executor.executor_detector import ExecutorDetector
from twitter.aurora.executor.gc_executor import ThermosGCExecutor
from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.common.metrics.sampler import DiskMetricWriter
from twitter.thermos.common.path import TaskPath

import mesos


app.configure(debug=True)


# locate logs locally in executor sandbox
LogOptions.set_simple(True)
LogOptions.set_disk_log_level('DEBUG')
LogOptions.set_log_dir(ExecutorDetector.LOG_PATH)


def proxy_main():
  def main():
    # Create executor stub
    thermos_gc_executor = ThermosGCExecutor(checkpoint_root=TaskPath.DEFAULT_CHECKPOINT_ROOT)
    thermos_gc_executor.start()

    # Start metrics collection
    metric_writer = DiskMetricWriter(thermos_gc_executor.metrics, ExecutorDetector.VARS_PATH)
    metric_writer.start()

    # Create driver stub
    driver = mesos.MesosExecutorDriver(thermos_gc_executor)

    # Start GC executor
    driver.run()

    log.info('MesosExecutorDriver.run() has finished.')

  app.main()
