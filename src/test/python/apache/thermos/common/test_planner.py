#
# Copyright 2013 Apache Software Foundation
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
from apache.thermos.common.planner import Planner


def details(planner):
  return planner.runnable, planner.running, planner.finished

def _(*processes):
  return set(processes)

empty = set()

def test_planner_empty():
  p = Planner(set(), {})
  assert details(p) == (empty, empty, empty)


def test_planner_unordered():
  p = Planner(['p1', 'p2', 'p3'], {})
  assert details(p) == (_('p1', 'p2', 'p3'), empty, empty)
  p.set_running('p2')
  assert details(p) == (_('p1', 'p3'), _('p2'), empty)
  with pytest.raises(AssertionError):
    p.set_finished('p3')
  p.set_running('p3')
  p.set_finished('p3')
  assert details(p) == (_('p1'), _('p2'), _('p3'))
  p.set_finished('p2')
  assert details(p) == (_('p1'), empty, _('p2', 'p3'))
  with pytest.raises(AssertionError):
    p.reset('p3')
  assert not p.is_complete()
  p.set_running('p1')
  p.set_finished('p1')
  assert p.is_complete()


def test_planner_ordered():
  p = Planner(['p1', 'p2', 'p3'], {'p3': ['p2'], 'p2': ['p1']})
  assert details(p) == (_('p1'), empty, empty)
  assert not p.is_complete()
  for process in ('p2', 'p3'):
    with pytest.raises(AssertionError):
      p.set_running(process)
  p.set_running('p1')
  assert details(p) == (empty, _('p1'), empty)
  p.set_finished('p1')
  assert details(p) == (_('p2'), empty, _('p1'))
  for process in ('p1', 'p2', 'p3'):
    with pytest.raises(AssertionError):
      p.reset(process)
  p.set_running('p2')
  p.set_finished('p2')
  assert details(p) == (_('p3'), empty, _('p1', 'p2'))
  assert not p.is_complete()
  p.set_running('p3')
  p.set_finished('p3')
  assert p.is_complete()


def test_planner_mixed():
  p = Planner(['p1', 'p2', 'p3', 'd1', 'd2'], {'p3': ['p2'], 'p2': ['p1']})
  assert details(p) == (_('p1', 'd1', 'd2'), empty, empty)


def test_planner_unsatisfiables():
  with pytest.raises(Planner.InvalidSchedule):
    Planner(['p1', 'p2'], {'p1': ['p2'], 'p2': ['p1']})
  with pytest.raises(Planner.InvalidSchedule):
    Planner(['p1', 'p2', 'p3'], {'p1': ['p2'], 'p2': ['p3'], 'p3': ['p1']})
