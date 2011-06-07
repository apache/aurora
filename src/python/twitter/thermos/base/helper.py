import os
import errno

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
  def workflow_from_job(thermosjob, workflow_name):
    for w in thermosjob.workflows:
      if w.name == workflow_name:
        return w
    return None

  @staticmethod
  def task_from_workflow(workflow, task_name):
    for t in workflow.tasks:
      if t.name == task_name:
        return t
    return None

  @staticmethod
  def task_from_name(workflow, task_name):
    for task in workflow.tasks:
       if task.name == task_name:
          return task
    return None

  @staticmethod
  def task_sequence_number(workflow_state, task_name):
    if task_name not in workflow_state.tasks:
      return 0
    else:
      wts = workflow_state.tasks[task_name].runs[-1]
      return wts.seq

  @staticmethod
  def task_run_number(workflow_state, task_name):
    if task_name not in workflow_state.tasks:
      return 0
    else:
      return len(workflow_state.tasks[task_name].runs) - 1
