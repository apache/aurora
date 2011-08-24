import os
import errno

# TODO(wickman)  Use the twitter.common.dirutils module.
class Helper:
  @staticmethod
  def safe_create_file(filename, args):
    try:
      os.makedirs(os.path.dirname(filename))
    except OSError, e:
      if e.errno != errno.EEXIST:
        raise
    return file(filename, args)

  @staticmethod
  def safe_create_dir(dirname):
    try:
      os.makedirs(dirname)
    except OSError, e:
      if e.errno != errno.EEXIST:
        raise

  @staticmethod
  def task_from_job(thermosjob, task_name):
    for w in thermosjob.tasks:
      if w.name == task_name:
        return w
    return None

  @staticmethod
  def process_from_task(task, process_name):
    for t in task.processes:
      if t.name == process_name:
        return t
    return None

  @staticmethod
  def process_from_name(task, process_name):
    for process in task.processes:
      if process.name == process_name:
        return process
    return None

  # TODO(wickman)  Change wts to something actually descriptive.
  @staticmethod
  def process_sequence_number(task_state, process_name):
    if process_name not in task_state.processes:
      return 0
    else:
      wts = task_state.processes[process_name].runs[-1]
      return wts.seq

  @staticmethod
  def process_run_number(task_state, process_name):
    if process_name not in task_state.processes:
      return 0
    else:
      return len(task_state.processes[process_name].runs) - 1
