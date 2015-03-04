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

import pprint

from pystachio.naming import frozendict
from twitter.common import app

from apache.thermos.cli.common import get_task_from_options
from apache.thermos.common.options import add_binding_to, add_port_to


def inspect_unwrap(obj):
  if isinstance(obj, frozendict):
    return dict((key, inspect_unwrap(val)) for (key, val) in obj.items())
  if isinstance(obj, (list, tuple, set)):
    return tuple(inspect_unwrap(val) for val in obj)
  return obj


@app.command
@app.command_option("--task", metavar="TASKNAME", default=None, dest='task',
                    help="The thermos task within the config that should be inspected. Only "
                    "required if there are multiple tasks exported from the thermos "
                    "configuration.")
@app.command_option("--json", default=False, action='store_true', dest='json',
                    help="Read the source file in json format instead of pystachio.")
@app.command_option("-P", "--port", type="string", nargs=1, action="callback",
                    callback=add_port_to('prebound_ports'), dest="prebound_ports", default=[],
                    metavar="NAME:PORT", help="bind named PORT to NAME.")
@app.command_option("-E", "--environment", type="string", nargs=1, action="callback",
                    callback=add_binding_to('bindings'), default=[], dest="bindings",
                    metavar="NAME=VALUE",
                    help="bind the configuration environment variable NAME to VALUE.")
def inspect(args, options):
  """Inspect a thermos config and display the evaluated task

    Usage: thermos inspect [options] config
  """
  thermos_task = get_task_from_options(args, options)
  ti, _ = thermos_task.task().interpolate()
  pprint.pprint(inspect_unwrap(ti.get()), indent=4)
