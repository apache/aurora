import os
import time

from twitter.common import app
from twitter.common.app.modules.http import RootServer

from twitter.mesos.clusters import Cluster
from twitter.mesos.executor.resource_checkpoints import CheckpointResourceMonitor
from twitter.mesos.observer.mesos_vars import MesosObserverVars
from twitter.thermos.base.path import TaskPath
from twitter.thermos.observer.observer import TaskObserver
from twitter.thermos.observer.http import BottleObserver


app.add_option("--root",
               dest="root",
               metavar="DIR",
               default=TaskPath.DEFAULT_CHECKPOINT_ROOT,
               help="root checkpoint directory for thermos task runners")


app.configure(module='twitter.common.app.modules.http',
    port=1338, host='0.0.0.0', enable=True, framework='cherrypy')
app.configure(module='twitter.common_internal.app.modules.chickadee_handler',
    service_name='thermos_observer')


def main(_, opts):
  mesos_root = os.environ.get('META_THERMOS_ROOT', Cluster.DEFAULT_MESOS_ROOT)

  # TODO(jon): either fully implement or remove the MesosCheckpointResourceMonitor
  class MesosCheckpointResourceMonitor(CheckpointResourceMonitor):
    MESOS_ROOT = mesos_root

  task_observer = TaskObserver(opts.root)
  task_observer.start()

  observer_vars = MesosObserverVars(task_observer, mesos_root)
  observer_vars.start()

  bottle_wrapper = BottleObserver(task_observer)
  RootServer().mount_routes(bottle_wrapper)

  while True:
    time.sleep(10)


app.main()
