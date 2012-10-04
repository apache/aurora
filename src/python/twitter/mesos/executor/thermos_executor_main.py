# mesos
import mesos

from twitter.common import app, log
from twitter.common.log.options import LogOptions

# thermos
from twitter.mesos.executor.task_runner_wrapper import (
    ProductionTaskRunner,
    AngrybirdTaskRunner)
from twitter.mesos.executor.thermos_executor import (
    ThermosExecutor,
    ThermosExecutorTimer)


app.configure(module='twitter.common_internal.app.modules.chickadee_handler',
    service_name='thermos_executor')
app.configure(debug=True)


if 'ANGRYBIRD_THERMOS' in os.environ:
  LogOptions.set_log_dir(os.path.join(os.environ['ANGRYBIRD_THERMOS'], 'thermos/log'))
else:
  LogOptions.set_log_dir('/var/log/mesos')


def main():
  LogOptions.set_disk_log_level('DEBUG')
  thermos_executor = ThermosExecutor()
  driver = mesos.MesosExecutorDriver(thermos_executor)
  ThermosExecutorTimer(thermos_executor, driver).start()
  driver.run()
  log.info('MesosExecutorDriver.run() has finished.')


app.main()
