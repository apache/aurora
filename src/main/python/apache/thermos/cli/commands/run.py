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

from __future__ import print_function

import getpass

from twitter.common import app

from apache.thermos.cli.common import get_task_from_options, really_run
from apache.thermos.common.options import add_binding_to, add_port_to


@app.command
@app.command_option("--user", metavar="USER", default=getpass.getuser(), dest='user',
                    help="run as this user.  if not $USER, must have setuid privilege.")
@app.command_option("--enable_chroot", dest="chroot", default=False, action='store_true',
                    help="chroot tasks to the sandbox before executing them, requires "
                    "root privileges.")
@app.command_option("--task", metavar="TASKNAME", default=None, dest='task',
                    help="The thermos task within the config that should be run. Only required if "
                    "there are multiple tasks exported from the thermos configuration.")
@app.command_option("--task_id", metavar="STRING", default=None, dest='task_id',
                    help="The id to which this task should be bound, synthesized from the task "
                    "name if none provided.")
@app.command_option("--json", default=False, action='store_true', dest='json',
                    help="Read the source file in json format.")
@app.command_option("--sandbox", metavar="PATH", default="/var/lib/thermos/sandbox", dest='sandbox',
                    help="The sandbox in which to run the task.")
@app.command_option("-P", "--port", type="string", nargs=1, action="callback",
                    callback=add_port_to('prebound_ports'), dest="prebound_ports", default=[],
                    metavar="NAME:PORT", help="bind named PORT to NAME.")
@app.command_option("-E", "--environment", type="string", nargs=1, action="callback",
                    callback=add_binding_to('bindings'), default=[], dest="bindings",
                    metavar="NAME=VALUE",
                    help="bind the configuration environment variable NAME to VALUE.")
@app.command_option("--daemon", default=False, action='store_true', dest='daemon',
                    help="fork and daemonize the thermos runner.")
def run(args, options):

  """Run a thermos task.

    Usage: thermos run [options] config
  """
  thermos_task = get_task_from_options(args, options)
  really_run(thermos_task,
             options.root,
             options.sandbox,
             task_id=options.task_id,
             user=options.user,
             prebound_ports=options.prebound_ports,
             chroot=options.chroot,
             daemon=options.daemon)
