from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.common_internal.app.modules import chickadee_handler

from .gc_executor import ThermosGCExecutor

import mesos


LogOptions.set_log_dir('executor_logs')
LogOptions.set_disk_log_level('DEBUG')
app.configure(module='twitter.common_internal.app.modules.chickadee_handler',
    service_name='thermos_gc_executor')
app.configure(debug=True)


def main():
  thermos_gc_executor = ThermosGCExecutor(checkpoint_root='/var/run/thermos')
  drv = mesos.MesosExecutorDriver(thermos_gc_executor)
  drv.run()
  log.info('MesosExecutorDriver.run() has finished.')


app.main()
