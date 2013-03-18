import os
from twitter.common import app, log
from twitter.common.log.options import LogOptions

# thermos
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

if 'ANGRYBIRD_THERMOS' in os.environ:
  LogOptions.set_log_dir(os.path.join(os.environ['ANGRYBIRD_THERMOS'], 'thermos/log'))
else:
  # locate logs locally in executor sandbox
  LogOptions.set_log_dir('executor_logs')


def main():
  LogOptions.set_disk_log_level('DEBUG')
  thermos_executor = ThermosExecutor()
  driver = mesos.MesosExecutorDriver(thermos_executor)
  ThermosExecutorTimer(thermos_executor, driver).start()
  driver.run()
  log.info('MesosExecutorDriver.run() has finished.')


app.main()
