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

import ctypes
import json
import os

from twitter.common import log

from gen.apache.aurora.api.constants import TASK_FILESYSTEM_MOUNT_POINT


def wrap_with_mesos_containerizer(cmdline, user, cwd, mesos_containerizer_path):
  command = json.dumps({
    'shell': False,
    'value': '/bin/bash',  # the binary to executed
    'arguments': [
      '/bin/bash',  # the name of the called binary as passed to the launched process
      '-c',
      cmdline
    ]
  })
  return [mesos_containerizer_path,
          'launch',
          '--unshare_namespace_mnt',
          '--working_directory=%s' % cwd,
          '--rootfs=%s' % os.path.join(os.environ['MESOS_DIRECTORY'], TASK_FILESYSTEM_MOUNT_POINT),
          '--user=%s' % user,
          '--command=%s' % command]


def setup_child_subreaping():
  """
  This uses the prctl(2) syscall to set the `PR_SET_CHILD_SUBREAPER` flag. This
  means if any children processes need to be reparented, they will be reparented
  to this process.

  More documentation here: http://man7.org/linux/man-pages/man2/prctl.2.html
  and here: https://lwn.net/Articles/474787/

  Callers should reap terminal children to prevent zombies.
  """
  log.debug("Calling prctl(2) with PR_SET_CHILD_SUBREAPER")
  # This constant is taken from prctl.h
  PR_SET_CHILD_SUBREAPER = 36
  try:
    library_name = ctypes.util.find_library('c')
    if library_name is None:
      log.warning("libc is not found. Unable to call prctl!")
      log.warning("Children subreaping is disabled!")
      return
    libc = ctypes.CDLL(library_name, use_errno=True)
    # If we are on a system where prctl doesn't exist, this will throw an
    # attribute error.
    ret = libc.prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0)
    if ret != 0:
      errno = ctypes.get_errno()
      raise OSError(errno, os.strerror(errno))
  except Exception as e:
    log.error("Unable to call prctl %s" % e)
    log.error("Children subreaping is disabled!")
