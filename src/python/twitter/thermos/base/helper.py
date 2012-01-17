import os
import errno

class Helper(object):
  @staticmethod
  def process_from_name(task, process_name):
    if task.has_processes():
      for process in task.processes():
        if process.name().get() == process_name:
          return process
    return None
