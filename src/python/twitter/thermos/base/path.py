import os

class TaskPath(object):
  """
    Handle the resolution / detection of the path structure for thermos tasks.

    This is used by the runner to determine where it should be dumping checkpoints,
    and by the observer to determine how to detect the running tasks on the system.

    Examples:
      pathspec = TaskPath(root = "/var/run/thermos")
                           ^ substitution dictionary for DIR_TEMPLATE


                                           which template to acquire
                                                    v
      pathspec.given(job_uid = 12345).getpath("active_job_path")
                         ^
            further substitutions DIR_TEMPLATE

      path_glob = pathspec.given(job_uid = "*").getpath(job_type)
      path_re   = pathspec.given(job_uid = "(\S+)").getpath(job_type)

      matching_paths = glob.glob(path_glob)
      path_re        = re.compile(path_re)

      uids = []
      for path in matching_paths:
        matched_blobs = path_re.match(path).groups()
        uids.append(int(matched_blobs[0]))
      return uids
  """

  class UnknownPath(Exception): pass
  class UnderspecifiedPath(Exception): pass

  # all keys: root job_uid pid process run
  DIR_TEMPLATE = {
      'active_job_path': ['%(root)s',        'jobs',      'active', '%(job_uid)s'],
    'finished_job_path': ['%(root)s',        'jobs',    'finished', '%(job_uid)s'],
    'runner_checkpoint': ['%(root)s', 'checkpoints', '%(job_uid)s', 'runner'],
   'process_checkpoint': ['%(root)s', 'checkpoints', '%(job_uid)s', '%(pid)s'],
       'process_logdir': ['%(root)s',        'logs', '%(job_uid)s', '%(process)s', '%(run)s']
  }

  def __init__(self, **kw):
    self._filename = None
    # this is somewhat of a hack to do self-interpolation
    self._data = { 'root':    '%(root)s',
                   'job_uid': '%(job_uid)s',
                   'pid':     '%(pid)s',
                   'process': '%(process)s',
                   'run':     '%(run)s' }
    self._data.update(kw)

  def given(self, **kw):
    """ Perform further interpolation of the templates given the kwargs """
    eval_dict = dict(self._data) # copy
    eval_dict.update(kw)
    return TaskPath(**eval_dict)

  def with_filename(self, filename):
    """ Return a WorkingPath with the specific filename appended to the end of paths """
    wp = TaskPath(**self._data)
    wp._filename = filename
    return wp

  def getpath(self, pathname):
    if pathname not in TaskPath.DIR_TEMPLATE:
      raise TaskPath.UnknownPath("Internal error, unknown id: %s" % pathname)
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
