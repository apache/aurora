"""Command-line entry point to the Thermos GC executor

This module wraps the Thermos GC executor to allow it to be compiled into an executable, so that it
can be launched by Mesos.

"""

import os

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.common_internal.app.modules import chickadee_handler
from twitter.thermos.base.path import TaskPath

from .gc_executor import ThermosGCExecutor

import mesos


LogOptions.set_log_dir('executor_logs')
LogOptions.set_disk_log_level('DEBUG')
app.configure(module='twitter.common_internal.app.modules.chickadee_handler',
    service_name='thermos_gc_executor')
app.configure(debug=True)


def main():
  checkpoint_root, mesos_root = TaskPath.DEFAULT_CHECKPOINT_ROOT, None
  if 'META_THERMOS_ROOT' in os.environ:
    mesos_root = os.environ['META_THERMOS_ROOT']
    checkpoint_root = os.path.join(mesos_root, 'checkpoints')
  thermos_gc_executor = ThermosGCExecutor(checkpoint_root=checkpoint_root, mesos_root=mesos_root)
  drv = mesos.MesosExecutorDriver(thermos_gc_executor)
  drv.run()
  log.info('MesosExecutorDriver.run() has finished.')


app.main()
