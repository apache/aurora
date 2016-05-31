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
from twitter.common.collections import OrderedDict

from apache.thermos.config.schema import (
    Constraint,
    Process,
    Resources,
    SequentialTask,
    SimpleTask,
    Task,
    Tasks,
    Units,
    combine_tasks,
    concat_tasks,
    java_options,
    order,
    python_options
)


def test_order():
  p1 = Process(name='p1')
  p2 = Process(name='p2')

  p1p2 = [Constraint(order=['p1', 'p2'])]
  assert order(p1, p2) == p1p2
  assert order('p1', p2) == p1p2
  assert order(p1, 'p2') == p1p2
  assert order('p1', 'p2') == p1p2
  assert order(u'p1', 'p2') == p1p2

  with pytest.raises(ValueError):
    order([p1])

  with pytest.raises(ValueError):
    order(None)


def test_add_resources():
  assert Units.resources_sum(Resources(), Resources()) == Resources(cpu=0, ram=0, disk=0, gpu=0)

  r1000 = Resources(cpu=1, ram=0, disk=0, gpu=0)
  r1001 = Resources(cpu=1, ram=0, disk=0, gpu=1)
  r0100 = Resources(cpu=0, ram=1, disk=0, gpu=0)
  r0010 = Resources(cpu=0, ram=0, disk=1, gpu=0)
  r1110 = Resources(cpu=1, ram=1, disk=1, gpu=0)
  r1101 = Resources(cpu=1, ram=1, disk=0, gpu=1)
  r2220 = Resources(cpu=2, ram=2, disk=2, gpu=0)

  assert reduce(Units.resources_sum, [r1000, r0100, r0010]) == r1110
  assert Units.resources_sum(r1110, r1110) == r2220
  assert r2220 == Units.resources_sum(r1000, r0100, r0010, r1110, Resources())
  assert Units.resources_sum(r1001, r0100) == r1101


def test_max_resources():
  assert Resources(cpu=1, ram=2, disk=3, gpu=4) == Units.resources_max([
      Resources(cpu=0, ram=2, disk=1, gpu=4),
      Resources(cpu=1, ram=1, disk=2, gpu=0),
      Resources(cpu=0, ram=1, disk=3, gpu=1)
  ])


def test_combine_tasks():
  p1 = Process(name='p1')
  p2 = Process(name='p2')
  p3 = Process(name='p3')
  p4 = Process(name='p4')
  r100 = Resources(cpu=1, ram=0, disk=0)
  r010 = Resources(cpu=0, ram=1, disk=0)
  r001 = Resources(cpu=0, ram=0, disk=1)
  r111 = Units.resources_sum(r100, r010, r001)

  t1 = Task(name="p1p2", processes=[p1, p2], constraints=order(p1, p2),
            resources=Units.resources_sum(r100, r010), finalization_wait=60)
  t2 = Task(name="p3p4", processes=[p3, p4], constraints=order(p3, p4),
            resources=r001, finalization_wait=45)

  assert combine_tasks() == Task()
  assert combine_tasks(t1) == t1
  assert combine_tasks(t2) == t2

  t3 = combine_tasks(t1, t2)
  assert t3.name() == t2.name()
  assert t3.resources() == r111
  assert set(t3.processes()) == set([p1, p2, p3, p4])
  assert set(t3.constraints()) == set(order(p1, p2) + order(p3, p4))
  assert t3.finalization_wait().get() == t1.finalization_wait().get()

  t4 = concat_tasks(t1, t2)
  assert t4.name() == t2.name()
  assert t4.resources() == r111
  assert set(t4.processes()) == set([p1, p2, p3, p4])
  assert set(t4.constraints()) == set(
      order(p1, p2) + order(p3, p4) + order(p1, p3) + order(p1, p4) +
      order(p2, p3) + order(p2, p4))
  assert t4.finalization_wait().get() == t1.finalization_wait().get() + t2.finalization_wait().get()


def test_simple_task():
  name, command = 'simple_thing', 'echo hello'
  st = SimpleTask(name, command)
  assert isinstance(st, Task)
  assert st.name().get() == name
  assert len(st.constraints().get()) == 0
  assert len(st.processes().get()) == 1
  assert st.processes().get()[0]['cmdline'] == command
  assert st.resources() == Resources(cpu=Tasks.SIMPLE_CPU,
                                     ram=Tasks.SIMPLE_RAM,
                                     disk=Tasks.SIMPLE_DISK)


def test_tasklets():
  install_thermosrc = Process(name='install_thermosrc')
  setup_py3k = Process(name='setup_py3k')
  setup_ruby19 = Process(name='setup_ruby19')
  setup_php = Process(name='setup_php')
  recipe_py3k = SequentialTask(processes=[install_thermosrc, setup_py3k])
  recipe_ruby19 = SequentialTask(processes=[install_thermosrc, setup_ruby19])
  recipe_php = SequentialTask(processes=[install_thermosrc, setup_php])
  all_recipes = Tasks.combine(recipe_py3k, recipe_ruby19, recipe_php)
  my_task = Task(processes=[Process(name='my_process')])
  my_new_task = Tasks.concat(all_recipes, my_task)(name='my_task')

  # TODO(wickman) Probably should have Tasks.combine/concat do constraint
  # minimization since many constraints are redundant.
  for p in (install_thermosrc, setup_py3k, setup_ruby19, setup_php):
    assert p in my_new_task.processes()


def test_render_options():
  def eq(o1, o2):
    return set(o1.split()) == set(o2.split())

  assert java_options('a', 'b', 'cow') == '-a -b -cow'
  assert eq(java_options({'a': None, 'b': None, 'cow': None}), '-a -b -cow')
  assert eq(java_options({'a': None, 'b': 1, 'cow': 'foo'}), '-a -b 1 -cow foo')
  assert eq(java_options(**{'a': None, 'b': 1, 'cow': 'foo'}), '-a -b 1 -cow foo')
  assert java_options('a', {'b': 1}, cow='foo') == '-a -b 1 -cow foo'

  assert python_options('a', 'b', 'cow') == '-a -b --cow'
  assert eq(python_options({'a': None, 'b': None, 'cow': None}), '-a -b --cow')
  assert eq(python_options({'a': None, 'b': 1, 'cow': 'foo'}), '-a -b 1 --cow foo')
  assert eq(python_options(**{'a': None, 'b': 1, 'cow': 'foo'}), '-a -b 1 --cow foo')
  assert python_options('a', {'b': 1}, cow='foo') == '-a -b 1 --cow foo'


def test_render_ordered():
  od = OrderedDict()
  od['a'] = 1
  od['b'] = 2
  od['c'] = 3
  assert java_options(od) == '-a 1 -b 2 -c 3'

  od = OrderedDict()
  od['c'] = 3
  od['b'] = 2
  od['a'] = 1
  assert java_options(od) == '-c 3 -b 2 -a 1'
