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


class TaskPath(object):
  """
    Handle the resolution / detection of the path structure for thermos tasks.

    This is used by the runner to determine where it should be dumping checkpoints and writing
    stderr/stdout, and by the observer to determine how to detect the running tasks on the system.

    Examples:
      pathspec = TaskPath(root = "/var/run/thermos")
                           ^ substitution dictionary for DIR_TEMPLATE


                                           which template to acquire
                                                    v
      pathspec.given(task_id = "12345-thermos-wickman-23", state='active').getpath("task_path")
                         ^
            further substitutions DIR_TEMPLATE


    As a detection mechanism:
      path_glob = pathspec.given(task_id = "*").getpath(task_type)
      matching_paths = glob.glob(path_glob)

      path_re = pathspec.given(task_id = "(\\S+)").getpath(task_type)
      path_re = re.compile(path_re)

      ids = []
      for path in matching_paths:
        matched_blobs = path_re.match(path).groups()
        ids.append(int(matched_blobs[0]))
      return ids
  """

  class UnknownPath(Exception): pass
  class UnderspecifiedPath(Exception): pass

  KNOWN_KEYS = ['root', 'task_id', 'state', 'process', 'run', 'log_dir']

  DIR_TEMPLATE = {
      'task_path': ['%(root)s', 'tasks', '%(state)s', '%(task_id)s'],
      'checkpoint_path': ['%(root)s', 'checkpoints', '%(task_id)s'],
      'runner_checkpoint': ['%(root)s', 'checkpoints', '%(task_id)s', 'runner'],
      'process_checkpoint': ['%(root)s', 'checkpoints', '%(task_id)s', 'coordinator.%(process)s'],
      'process_logbase': ['%(log_dir)s'],
      'process_logdir': ['%(log_dir)s', '%(process)s', '%(run)s']
  }

  def __init__(self, **kw):
    self._filename = None
    # initialize with self-interpolating values
    # Before log_dir was added explicitly to RunnerHeader, it resolved to %(root)s/logs
    self._template, keys = self.DIR_TEMPLATE, self.KNOWN_KEYS
    for k, v in kw.items():
      if v is None:
        raise ValueError("Key %s is None" % k)
    self._data = dict((key, '%%(%s)s' % key) for key in keys)
    self._data.update(kw)

  def __hash__(self):
    return hash(tuple(self._data.items()))

  def given(self, **kw):
    """ Perform further interpolation of the templates given the kwargs """
    eval_dict = dict(self._data)
    eval_dict.update(kw)
    tp = TaskPath(**eval_dict)
    tp._filename = self._filename
    return tp

  def with_filename(self, filename):
    """ Return a TaskPath with the specific filename appended to the end of the path """
    wp = TaskPath(**self._data)
    wp._filename = filename
    return wp

  def getpath(self, pathname):
    if pathname not in self._template:
      raise self.UnknownPath("Internal error, unknown id: %s" % pathname)
    path = self._template[pathname][:]

    if self._filename:
      path += [self._filename]
    path = os.path.join(*path)
    interpolated_path = path % self._data
    try:
      interpolated_path % {}
    except KeyError:
      raise self.UnderspecifiedPath(
        "Tried to interpolate path with insufficient variables: %s as %s" % (
        pathname, interpolated_path))
    return interpolated_path
