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

import pytest
from twitter.common.testing.clock import ThreadedClock

from apache.thermos.common.planner import TaskPlanner
from apache.thermos.config.schema import *

p1 = Process(name = "p1", cmdline = "")
p2 = Process(name = "p2", cmdline = "")
p3 = Process(name = "p3", cmdline = "")

unordered_task = Task(name = "unordered", processes = [p1, p2, p3])
ordered_task = unordered_task(constraints = [{'order': ['p1', 'p2', 'p3']}])
empty_task = Task(name = "empty", processes = [])

def _(*processes):
  return set(processes)
empty = set()

def approx_equal(a, b):
  return abs(a - b) < 0.001


def test_task_construction():
  p = TaskPlanner(empty_task)
  assert p.runnable == empty
  assert p.is_complete()
  p = TaskPlanner(unordered_task)
  assert p.runnable == _('p1', 'p2', 'p3')
  assert not p.is_complete()
  p = TaskPlanner(ordered_task)
  assert p.runnable == _('p1')
  assert not p.is_complete()


def test_task_finish_with_ephemerals():
  pure_ephemeral = empty_task(processes=[p1(ephemeral=True)])
  p = TaskPlanner(pure_ephemeral)
  assert p.is_complete()
  p.set_running('p1')
  assert p.is_complete()
  p.add_failure('p1')
  assert p.is_complete()
  assert not p.failed
  assert p.finished == _('p1')

  with_ephemeral = empty_task(processes=[p1, p2(ephemeral=True)])
  p = TaskPlanner(with_ephemeral)
  assert not p.is_complete()
  assert p.runnable == _('p1', 'p2')
  p.set_running('p1')
  assert not p.is_complete()
  p.add_failure('p1')
  assert p.is_complete()
  p.set_running('p2')
  assert p.is_complete()
  p.add_failure('p2')
  assert p.is_complete()
  assert p.failed == _('p1')


def test_task_finish_with_daemons():
  # Daemon is still restricted to the failure limit
  p = TaskPlanner(empty_task(processes=[p1(daemon=True)]))
  assert not p.is_complete()
  p.set_running('p1')
  assert not p.is_complete()
  p.add_failure('p1')
  assert p.is_complete()

  # Resilient to two failures
  p = TaskPlanner(empty_task(processes=[p1(daemon=True, max_failures=2)]))
  assert not p.is_complete()
  p.set_running('p1')
  assert not p.is_complete()
  p.add_failure('p1')
  assert not p.is_complete()
  p.set_running('p1')
  assert not p.is_complete()
  p.add_failure('p1')
  assert p.is_complete()

  # Can swallow successes
  p = TaskPlanner(empty_task(processes=[p1(daemon=True, max_failures=2)]))
  assert not p.is_complete()
  p.set_running('p1')
  assert not p.is_complete()
  p.add_failure('p1')
  assert not p.is_complete()
  p.set_running('p1')
  assert not p.is_complete()
  p.add_success('p1')
  assert not p.is_complete()
  p.set_running('p1')
  assert not p.is_complete()
  p.add_failure('p1')
  assert p.is_complete()


def test_task_finish_with_daemon_ephemerals():
  p = TaskPlanner(empty_task(processes=[p1, p2(daemon=True, ephemeral=True, max_failures=2)]))
  assert not p.is_complete()
  p.set_running('p1')
  p.set_running('p2')
  assert not p.is_complete()
  p.add_success('p1')
  assert p.is_complete()


def test_task_process_cannot_depend_upon_daemon():
  with pytest.raises(TaskPlanner.InvalidSchedule):
    TaskPlanner(empty_task(processes=[p1(daemon=True), p2], constraints=[{'order': ['p1', 'p2']}]))


def test_task_non_ephemeral_process_cannot_depend_on_ephemeral_process():
  with pytest.raises(TaskPlanner.InvalidSchedule):
    TaskPlanner(empty_task(processes=[p1(ephemeral=True), p2],
                           constraints=[{'order': ['p1', 'p2']}]))


def test_task_failed_predecessor_does_not_make_process_runnable():
  p = TaskPlanner(empty_task(processes=[p1, p2], constraints=[{'order': ['p1', 'p2']}]))
  p.set_running('p1')
  p.add_success('p1')
  assert 'p2' in p.runnable
  assert not p.is_complete()

  p = TaskPlanner(empty_task(processes=[p1, p2], constraints=[{'order': ['p1', 'p2']}]))
  p.set_running('p1')
  p.add_failure('p1')
  assert 'p2' not in p.runnable
  assert not p.is_complete()


def test_task_daemon_duration():
  p = TaskPlanner(empty_task(processes=[p1(daemon=True, max_failures=2, min_duration=10)]))
  assert 'p1' in p.runnable
  p.set_running('p1')
  p.add_success('p1', timestamp=5)
  assert 'p1' not in p.runnable_at(timestamp=5)
  assert 'p1' not in p.runnable_at(timestamp=10)
  assert 'p1' in p.runnable_at(timestamp=15)
  assert 'p1' in p.runnable_at(timestamp=20)
  p.set_running('p1')
  p.add_failure('p1', timestamp=10)
  assert 'p1' not in p.runnable_at(timestamp=10)
  assert 'p1' not in p.runnable_at(timestamp=15)
  assert 'p1' in p.runnable_at(timestamp=20)
  assert 'p1' in p.runnable_at(timestamp=25)
  p.set_running('p1')
  p.add_failure('p1', timestamp=15)
  assert 'p1' not in p.runnable_at(timestamp=15)
  assert 'p1' not in p.runnable_at(timestamp=20)
  assert 'p1' not in p.runnable_at(timestamp=25)  # task past maximum failure limit
  assert 'p1' not in p.runnable_at(timestamp=30)


def test_task_waits():
  dt = p1(daemon=True, max_failures=0)
  p = TaskPlanner(empty_task(processes=[dt(name='d3', min_duration=3),
                                        dt(name='d5', min_duration=5),
                                        dt(name='d7', min_duration=7)]))
  assert p.runnable_at(timestamp=0) == _('d3', 'd5', 'd7')
  assert p.min_wait(timestamp=0) == 0

  p.set_running('d3')
  p.add_success('d3', timestamp=0)
  assert p.runnable_at(timestamp=0) == _('d5', 'd7')
  assert p.waiting_at(timestamp=0) == _('d3')
  assert approx_equal(p.get_wait('d3', timestamp=0), 3)
  assert approx_equal(p.min_wait(timestamp=0), 0)
  assert approx_equal(p.min_wait(timestamp=1), 0)
  assert p.waiting_at(timestamp=3) == empty
  assert p.runnable_at(timestamp=3) == _('d3', 'd5', 'd7')

  p.set_running('d3')
  p.set_running('d7')
  p.add_success('d7', timestamp=1)
  assert approx_equal(p.min_wait(timestamp=2), 0)
  p.set_running('d5')
  assert approx_equal(p.min_wait(timestamp=2), 6)
  p.add_success('d5', timestamp=2)
  p.add_success('d3', timestamp=2)
  assert approx_equal(p.min_wait(timestamp=3), 2)
  assert p.runnable_at(timestamp=2) == empty
  assert p.runnable_at(timestamp=5) == _('d3')
  assert p.runnable_at(timestamp=7) == _('d3', 'd5')
  assert p.runnable_at(timestamp=8) == _('d3', 'd5', 'd7')


def test_task_fails():
  dt = p1(max_failures=1, min_duration=1)
  p = TaskPlanner(empty_task(processes = [dt(name='d1'), dt(name='d2')]))
  assert p.runnable_at(timestamp=0) == _('d1', 'd2')
  p.set_running('d1')
  p.set_running('d2')
  assert p.runnable_at(timestamp=0) == empty
  p.add_failure('d1', timestamp=0)
  p.add_failure('d2', timestamp=0)
  assert p.runnable_at(timestamp=0) == empty
  assert p.min_wait(timestamp=0) == TaskPlanner.INFINITY

  p = TaskPlanner(empty_task(processes = [dt(name='d1'), dt(name='d2')]))
  assert p.runnable_at(timestamp=0) == _('d1', 'd2')
  p.set_running('d1')
  p.set_failed('d1')
  assert p.runnable_at(timestamp=0) == _('d2')
  p.set_running('d2')
  assert p.runnable_at(timestamp=0) == empty
  p.add_failure('d2', timestamp=0)
  assert p.runnable_at(timestamp=0) == empty
  assert p.min_wait(timestamp=0) == TaskPlanner.INFINITY

  # test max_failures=0 && daemon==True ==> retries forever
  p = TaskPlanner(empty_task(processes = [dt(name='d1', max_failures=0, daemon=True)]))
  for k in range(10):
    p.set_running('d1')
    assert 'd1' in p.running
    assert 'd1' not in p.failed
    p.add_failure('d1')
    assert 'd1' not in p.running
    assert 'd1' not in p.failed
    p.set_running('d1')
    assert 'd1' in p.running
    assert 'd1' not in p.failed
    p.add_success('d1')
    assert 'd1' not in p.running
    assert 'd1' not in p.failed
    assert 'd1' not in p.finished

  p = TaskPlanner(empty_task(processes = [dt(name='d1', max_failures=0)]))
  p.set_running('d1')
  assert 'd1' in p.running
  assert 'd1' not in p.failed
  p.add_failure('d1')
  assert 'd1' not in p.running
  assert 'd1' not in p.failed
  p.set_running('d1')
  assert 'd1' in p.running
  assert 'd1' not in p.failed
  p.add_success('d1')
  assert 'd1' not in p.running
  assert 'd1' not in p.failed
  assert 'd1' in p.finished


def test_task_lost():
  dt = p1(max_failures=2, min_duration=1)

  # regular success behavior
  p = TaskPlanner(empty_task(processes = [dt(name='d1')]))
  assert p.runnable_at(timestamp=0) == _('d1')
  p.set_running('d1')
  p.add_success('d1', timestamp=0)
  assert p.min_wait(timestamp=0) == TaskPlanner.INFINITY

  # regular failure behavior
  p = TaskPlanner(empty_task(processes = [dt(name='d1')]))
  assert p.runnable_at(timestamp=0) == _('d1')
  p.set_running('d1')
  p.add_failure('d1', timestamp=1)
  assert approx_equal(p.min_wait(timestamp=1), 1)
  p.set_running('d1')
  p.add_failure('d1', timestamp=3)
  assert p.min_wait(timestamp=3) == TaskPlanner.INFINITY

  # lost behavior
  p = TaskPlanner(empty_task(processes = [dt(name='d1')]))
  assert p.runnable_at(timestamp=0) == _('d1')
  p.set_running('d1')
  p.add_failure('d1', timestamp=1)
  assert approx_equal(p.min_wait(timestamp=1), 1)
  p.set_running('d1')
  p.lost('d1')
  assert approx_equal(p.min_wait(timestamp=1), 1)
  p.set_running('d1')
  p.add_failure('d1', timestamp=3)
  assert p.min_wait(timestamp=3) == TaskPlanner.INFINITY


def test_task_filters():
  t = p1
  task = empty_task(processes = [t(name='p1'), t(name='p2'), t(name='p3'),
                                 t(name='f1'), t(name='f2'), t(name='f3')],
                    constraints = [Constraint(order=['p1','p2','p3']),
                                   Constraint(order=['f1','f2','f3'])])
  assert TaskPlanner(task, process_filter=lambda proc: proc.name().get().startswith('p'))
  assert TaskPlanner(task, process_filter=lambda proc: proc.name().get().startswith('f'))

  with pytest.raises(TaskPlanner.InvalidSchedule):
    TaskPlanner(task(constraints=[Constraint(order=['p1','f1'])]),
                process_filter=lambda proc: proc.name().get().startswith('p'))
  with pytest.raises(TaskPlanner.InvalidSchedule):
    TaskPlanner(task(constraints=[Constraint(order=['p1','f1'])]),
                process_filter=lambda proc: proc.name().get().startswith('f'))


def test_task_max_runs():
  class CappedTaskPlanner(TaskPlanner):
    TOTAL_RUN_LIMIT = 2
  dt = p1(daemon=True, max_failures=0)

  p = CappedTaskPlanner(empty_task(processes = [dt(name = 'd1', max_failures=100, daemon=False)]))
  p.set_running('d1')
  p.add_failure('d1', timestamp=1)
  assert 'd1' in p.runnable
  p.set_running('d1')
  p.add_failure('d1', timestamp=2)
  assert 'd1' not in p.runnable

  p = CappedTaskPlanner(empty_task(processes = [dt(name = 'd1', max_failures=100)]))
  p.set_running('d1')
  p.add_failure('d1', timestamp=1)
  assert 'd1' in p.runnable
  p.set_running('d1')
  p.add_success('d1', timestamp=2)
  assert 'd1' not in p.runnable
