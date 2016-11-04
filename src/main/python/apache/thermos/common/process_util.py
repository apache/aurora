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
import os

from twitter.common import log

from gen.apache.aurora.api.constants import TASK_FILESYSTEM_MOUNT_POINT


def wrap_with_mesos_containerizer(cmdline, user, cwd, mesos_containerizer_path):
  # We're going to embed this in JSON, so we must escape quotes and newlines.
  cmdline = cmdline.replace('"', '\\"').replace('\n', '\\n')

  # We must wrap the command in single quotes otherwise the shell that executes
  # mesos-containerizer will expand any bash variables in the cmdline. Escaping single quotes in
  # bash is hard: https://github.com/koalaman/shellcheck/wiki/SC1003.
  bash_wrapper = "/bin/bash -c '\\''%s'\\''"

  # The shell: true below shouldn't be necessary. Since we're just invoking bash anyway, using it
  # results in a process like: `sh -c /bin/bash -c ...`, however in my testing no combination of
  # shell: false and splitting the bash/cmdline args across value/arguments produced an invocation
  # that actually worked. That said, it *should* be possbie.
  # TODO(jcohen): Investigate setting shell:false further.
  return ('%s launch '
          '--unshare_namespace_mnt '
          '--working_directory=%s '
          '--rootfs=%s '
          '--user=%s '
          '--command=\'{"shell":true,"value":"%s"}\'' % (
              mesos_containerizer_path,
              cwd,
              os.path.join(os.environ['MESOS_DIRECTORY'], TASK_FILESYSTEM_MOUNT_POINT),
              user,
              bash_wrapper % cmdline))


def setup_child_subreaping():
  """
  This uses the prctl(2) syscall to set the `PR_SET_CHILD_SUBREAPER` flag. This
  means if any children processes need to be reparented, they will be reparented
  to this process.

  More documentation here: http://man7.org/linux/man-pages/man2/prctl.2.html
  and here: https://lwn.net/Articles/474787/

  Callers should reap terminal children to prevent zombies.

  raises OSError if the underlying prctl call fails.
  raises RuntimeError if libc cannot be found.
  """
  log.debug("Calling prctl(2) with PR_SET_CHILD_SUBREAPER")
  # This constant is taken from prctl.h
  PR_SET_CHILD_SUBREAPER = 36
  library_name = ctypes.util.find_library('c')
  if library_name is None:
    raise RuntimeError("libc not found")
  libc = ctypes.CDLL(library_name, use_errno=True)
  ret = libc.prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0)
  if ret != 0:
    errno = ctypes.get_errno()
    raise OSError(errno, os.strerror(errno))
