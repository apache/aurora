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
import subprocess

from twitter.common import log

from gen.apache.aurora.api.constants import TASK_FILESYSTEM_MOUNT_POINT


def is_launch_info_supported(mesos_containerizer_path):
  process = subprocess.Popen(
          [mesos_containerizer_path, 'launch'],
          stdout=subprocess.PIPE,
          stderr=subprocess.PIPE)

  _, stderr = process.communicate()
  if "launch_info" in stderr:
    return True

  return False


def wrap_with_mesos_containerizer(cmdline, user, cwd, mesos_containerizer_path):
  command_info = {
    'shell': False,
    'value': '/bin/bash',  # the binary to executed
    'arguments': [
      '/bin/bash',  # the name of the called binary as passed to the launched process
      '-c',
      cmdline
    ]
  }

  rootfs = os.path.join(os.environ['MESOS_DIRECTORY'], TASK_FILESYSTEM_MOUNT_POINT)

  launch_info = {
    'rootfs': rootfs,
    'user': str(user),
    'working_directory': cwd,
    'command': command_info
  }

  # TODO(santhk) it is unfortunate that we will need to check the
  # interface of mesos-containerizer like this. Clean this up when we have
  # migrated to Mesos-1.2.0.
  if is_launch_info_supported(mesos_containerizer_path):
    return [mesos_containerizer_path,
      'launch',
      '--unshare_namespace_mnt',
      '--launch_info=%s' % json.dumps(launch_info)]
  else:
    return [mesos_containerizer_path,
      'launch',
      '--unshare_namespace_mnt',
      '--working_directory=%s' % cwd,
      '--rootfs=%s' % rootfs,
      '--user=%s' % user,
      '--command=%s' % json.dumps(command_info)]


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
