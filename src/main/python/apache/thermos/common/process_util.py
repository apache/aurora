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

import os

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
