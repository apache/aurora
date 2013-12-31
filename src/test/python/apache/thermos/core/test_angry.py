import random

from twitter.thermos.config.schema import Task, Process
from twitter.thermos.testing.runner import Runner
from gen.twitter.thermos.ttypes import TaskState, ProcessState


def flaky_task():
  task = Task(
    name = "failing_task",
    max_failures = 2,
    processes = [
      Process(name = "a", max_failures=1, min_duration=1, cmdline="echo hello world"),
      Process(name = "b", max_failures=2, min_duration=1, cmdline="exit 1"),
      Process(name = "c", max_failures=1, min_duration=1, final=True, cmdline="echo hello world")
    ],
    constraints = [{'order': ['a', 'b']}]
  )
  return task.interpolate()[0]


def test_flaky_runner():
  runner = Runner(flaky_task(), success_rate=90, random_seed=31337)

  count = 0
  while True:
    count += 1
    print('Run #%d' % count)
    rc = runner.run()
    if rc == 0:
      break

  print('Completed in %d runs' % count)
  assert runner.state.statuses[-1].state == TaskState.SUCCESS
  assert runner.state.processes['a'][-1].state == ProcessState.SUCCESS
  assert runner.state.processes['b'][-1].state == ProcessState.FAILED
  assert runner.state.processes['c'][-1].state == ProcessState.SUCCESS
