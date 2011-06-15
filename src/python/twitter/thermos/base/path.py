import os

class WorkflowPath_UnknownPath(Exception): pass
class WorkflowPath_UnderspecifiedPath(Exception): pass

class WorkflowPath:
  """
    Handle the resolution / detection of the path structure for thermos workflows.

    This is used by the runner to determine where it should be dumping checkpoints,
    and by the observer to determine how to detect the running workflows on the system.

    Examples:
      pathspec = WorkflowPath(root = "/var/run/thermos")
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

  # all keys: root job_uid pid task run
  DIR_TEMPLATE = {
      'active_job_path': ['%(root)s',        'jobs',      'active', '%(job_uid)s'],
    'finished_job_path': ['%(root)s',        'jobs',    'finished', '%(job_uid)s'],
    'runner_checkpoint': ['%(root)s', 'checkpoints', '%(job_uid)s', 'runner'],
      'task_checkpoint': ['%(root)s', 'checkpoints', '%(job_uid)s', '%(pid)s'],
          'task_logdir': ['%(root)s',        'logs', '%(job_uid)s', '%(task)s', '%(run)s']
  }

  def __init__(self, **kw):
    self._filename = None
    # this is somewhat of a hack to do self-interpolation
    self._data = { 'root':    '%(root)s',
                   'job_uid': '%(job_uid)s',
                   'pid':     '%(pid)s',
                   'task':    '%(task)s',
                   'run':     '%(run)s' }
    self._data.update(kw)

  def given(self, **kw):
    """ Perform further interpolation of the templates given the kwargs """
    eval_dict = dict(self._data) # copy
    eval_dict.update(kw)
    return WorkflowPath(**eval_dict)

  def with_filename(self, filename):
    """ Return a WorkingPath with the specific filename appended to the end of paths """
    wp = WorkflowPath(**self._data)
    wp._filename = filename
    return wp

  def getpath(self, pathname):
    if pathname not in WorkflowPath.DIR_TEMPLATE:
      raise WorkflowPath_UnknownPath("Internal error, unknown id: %s" % pathname)
    path = list(WorkflowPath.DIR_TEMPLATE[pathname]) # copy
    if self._filename: path += [self._filename]
    path = os.path.join(*path)
    interpolated_path = path % self._data
    try:
      _ = interpolated_path % {}
    except KeyError:
      raise WorkflowPath_UnderspecifiedPath(
        "Tried to interpolate path with insufficient variables: %s as %s" % (
        pathname, interpolated_path))
    return interpolated_path
