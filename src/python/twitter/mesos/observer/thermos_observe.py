import os
import time

from twitter.common import app
from twitter.common.app.modules.http import RootServer
from twitter.common.metrics import RootMetrics
from twitter.mesos.common_internal.clusters import TwitterCluster
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

app.add_option("--show-meta-slaves",
               dest='show_meta_slaves',
               default=False,
               action='store_true',
               help="Export stats of meta-slaves as well.")


app.configure(module='twitter.common.app.modules.http',
    port=1338, host='0.0.0.0', enable=True, framework='cherrypy')
app.configure(module='twitter.common_internal.app.modules.chickadee_handler',
    service_name='thermos_observer')


# TODO(wickman) This is purely to prevent killing the keytracker.  Once the
# keytracker work has been put in place, remove this.
class MetaIgnoringMesosObserverVars(MesosObserverVars):
  @classmethod
  def executor_filter(cls, executor):
    return 'mesos-meta_slave' in executor.executor_id


def main(_, opts):
  mesos_root = os.environ.get('META_THERMOS_ROOT', TwitterCluster.DEFAULT_MESOS_ROOT)

  # TODO(jon): either fully implement or remove the MesosCheckpointResourceMonitor
  class MesosCheckpointResourceMonitor(CheckpointResourceMonitor):
    MESOS_ROOT = mesos_root

  task_observer = TaskObserver(opts.root)
  task_observer.start()

  observer_class = MesosObserverVars if opts.show_meta_slaves else MetaIgnoringMesosObserverVars
  observer_vars = observer_class(task_observer, mesos_root)
  observer_vars.start()
  RootMetrics().register_observable('observer', observer_vars)

  bottle_wrapper = BottleObserver(task_observer)
  RootServer().mount_routes(bottle_wrapper)

  while True:
    time.sleep(10)


app.main()
