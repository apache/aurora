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

      path_re = pathspec.given(task_id = "(\S+)").getpath(task_type)
      path_re = re.compile(path_re)

      ids = []
      for path in matching_paths:
        matched_blobs = path_re.match(path).groups()
        ids.append(int(matched_blobs[0]))
      return ids
  """

  class UnknownPath(Exception): pass
  class UnderspecifiedPath(Exception): pass

  DEFAULT_CHECKPOINT_ROOT = "/var/run/thermos"
  KNOWN_KEYS = [ 'root', 'task_id', 'state', 'process', 'run', 'log_dir' ]

  DIR_TEMPLATE = {
            'task_path': ['%(root)s',       'tasks',   '%(state)s', '%(task_id)s'],
      'checkpoint_path': ['%(root)s', 'checkpoints', '%(task_id)s'],
    'runner_checkpoint': ['%(root)s', 'checkpoints', '%(task_id)s', 'runner'],
   'process_checkpoint': ['%(root)s', 'checkpoints', '%(task_id)s', 'coordinator.%(process)s'],
      'process_logbase': ['%(log_dir)s', '%(task_id)s'],
       'process_logdir': ['%(log_dir)s', '%(task_id)s', '%(process)s', '%(run)s']
  }

  def __init__(self, **kw):
    self._filename = None
    # initialize with self-interpolating values
    if kw.get('root') is None:
      kw['root'] = self.DEFAULT_CHECKPOINT_ROOT
    # Before log_dir was added explicitly to RunnerHeader, it resolved to %(root)s/logs
    if kw.get('log_dir') is None:
      kw['log_dir'] = os.path.join(kw['root'], 'logs')
    self._data = dict((key, '%%(%s)s' % key) for key in TaskPath.KNOWN_KEYS)
    self._data.update(kw)

  def given(self, **kw):
    """ Perform further interpolation of the templates given the kwargs """
    eval_dict = dict(self._data) # copy
    eval_dict.update(kw)
    return TaskPath(**eval_dict)

  def with_filename(self, filename):
    """ Return a TaskPath with the specific filename appended to the end of the path """
    wp = TaskPath(**self._data)
    wp._filename = filename
    return wp

  def getpath(self, pathname):
    if pathname not in TaskPath.DIR_TEMPLATE:
      raise self.UnknownPath("Internal error, unknown id: %s" % pathname)
    path = list(TaskPath.DIR_TEMPLATE[pathname]) # copy
    if self._filename: path += [self._filename]
    path = os.path.join(*path)
    interpolated_path = path % self._data
    try:
      _ = interpolated_path % {}
    except KeyError:
      raise TaskPath.UnderspecifiedPath(
        "Tried to interpolate path with insufficient variables: %s as %s" % (
        pathname, interpolated_path))
    return interpolated_path
