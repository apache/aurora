#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A Mesos-customized entry point to the thermos_observer webserver."""

import time

from twitter.common import app
from twitter.common.exceptions import ExceptionalThread
from twitter.common.log.options import LogOptions
from twitter.common.quantity import Amount, Time

from apache.aurora.executor.common.path_detector import MesosPathDetector
from apache.thermos.common.excepthook import ExceptionTerminationHandler
from apache.thermos.monitoring.disk import DiskCollectorSettings
from apache.thermos.monitoring.resource import TaskResourceMonitor
from apache.thermos.observer.http.configure import configure_server
from apache.thermos.observer.task_observer import TaskObserver

app.add_option(
    '--mesos-root',
    dest='mesos_root',
    type='string',
    default=MesosPathDetector.DEFAULT_MESOS_ROOT,
    help='The mesos root directory to search for Thermos executor sandboxes [default: %default]')


app.add_option(
    '--ip',
    dest='ip',
    type='string',
    default='0.0.0.0',
    help='The IP address the observer will bind to.')


app.add_option(
    '--port',
    dest='port',
    type='int',
    default=1338,
    help='The port on which the observer should listen.')


app.add_option(
    '--polling_interval_secs',
    dest='polling_interval_secs',
    type='int',
    default=int(TaskObserver.POLLING_INTERVAL.as_(Time.SECONDS)),
    help='The number of seconds between observer refresh attempts.')


app.add_option(
    '--disable_task_resource_collection',
    dest='disable_task_resource_collection',
    default=False,
    action='store_true',
    help="Disable collection of CPU and memory statistics for each active task. Those can be "
         "expensive to collect if there are hundreds of active tasks per host.")


app.add_option(
    '--task_process_collection_interval_secs',
    dest='task_process_collection_interval_secs',
    type='int',
    default=int(TaskResourceMonitor.PROCESS_COLLECTION_INTERVAL.as_(Time.SECONDS)),
    help='The number of seconds between per task process resource collections.')


app.add_option(
    '--task_disk_collection_interval_secs',
    dest='task_disk_collection_interval_secs',
    type='int',
    default=int(DiskCollectorSettings.DISK_COLLECTION_INTERVAL.as_(Time.SECONDS)),
    help='The number of seconds between per task disk resource collections.')


app.add_option(
    '--enable_mesos_disk_collector',
    dest='enable_mesos_disk_collector',
    default=False,
    action='store_true',
    help="Delegate per task disk usage collection to agent. Should be enabled in conjunction to "
         "disk isolation in Mesos-agent. This is not compatible with an authenticated agent API.")


app.add_option(
    '--agent_api_url',
    dest='agent_api_url',
    type='string',
    default=DiskCollectorSettings.DEFAULT_AGENT_CONTAINERS_ENDPOINT,
    help='Mesos Agent API url.')


app.add_option(
    '--executor_id_json_path',
    dest='executor_id_json_path',
    type='string',
    default=DiskCollectorSettings.DEFAULT_EXECUTOR_ID_PATH,
    help='`jmespath` to executor_id key in agent response json object.')


app.add_option(
    '--disk_usage_json_path',
    dest='disk_usage_json_path',
    type='string',
    default=DiskCollectorSettings.DEFAULT_DISK_USAGE_PATH,
    help='`jmespath` to disk usage bytes value in agent response json object.')


# Allow an interruptible sleep so that ^C works.
def sleep_forever():
  while True:
    time.sleep(1)


def initialize(options):
  path_detector = MesosPathDetector(options.mesos_root)
  disk_collector_settings = DiskCollectorSettings(
      options.agent_api_url,
      options.executor_id_json_path,
      options.disk_usage_json_path,
      Amount(options.task_disk_collection_interval_secs, Time.SECONDS))

  return TaskObserver(
      path_detector,
      Amount(options.polling_interval_secs, Time.SECONDS),
      Amount(options.task_process_collection_interval_secs, Time.SECONDS),
      disable_task_resource_collection=options.disable_task_resource_collection,
      enable_mesos_disk_collector=options.enable_mesos_disk_collector,
      disk_collector_settings=disk_collector_settings)


def main(_, options):
  observer = initialize(options)
  observer.start()
  root_server = configure_server(observer)

  server = ExceptionalThread(target=lambda: root_server.run(options.ip, options.port, 'cherrypy'))
  server.daemon = True
  server.start()

  sleep_forever()


LogOptions.set_stderr_log_level('google:INFO')
app.register_module(ExceptionTerminationHandler())
app.main()
