import pytest

from twitter.common.collections import OrderedDict
from twitter.thermos.config.schema import (
  combine_tasks,
  concat_tasks,
  java_options,
  order,
  python_options,
  Units,
  List,
  Constraint,
  Process,
  Task,
  Resources,
  SequentialTask
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
  assert Units.resources_sum(Resources(), Resources()) == Resources(cpu=0, ram=0, disk=0)

  r100 = Resources(cpu=1, ram=0, disk=0)
  r010 = Resources(cpu=0, ram=1, disk=0)
  r001 = Resources(cpu=0, ram=0, disk=1)
  r111 = Resources(cpu=1, ram=1, disk=1)
  r222 = Resources(cpu=2, ram=2, disk=2)

  assert reduce(Units.resources_sum, [r100, r010, r001]) == r111
  assert Units.resources_sum(r111, r111) == r222
  assert r222 == Units.resources_sum([r100, r010, r001, r111, Resources()])


def test_combine_tasks():
  p1 = Process(name='p1')
  p2 = Process(name='p2')
  p3 = Process(name='p3')
  p4 = Process(name='p4')
  r100 = Resources(cpu=1, ram=0, disk=0)
  r010 = Resources(cpu=0, ram=1, disk=0)
  r001 = Resources(cpu=0, ram=0, disk=1)
  r111 = Units.resources_sum([r100, r010, r001])

  t1 = Task(name="p1p2", processes=[p1, p2], constraints=order(p1, p2),
            resources=Units.resources_sum([r100, r010]))
  t2 = Task(name="p3p4", processes=[p3, p4], constraints=order(p3, p4),
            resources=r001)

  assert combine_tasks() == Task()
  assert combine_tasks(t1) == t1
  assert combine_tasks(t2) == t2

  t3 = combine_tasks(t1, t2)
  assert t3.name() == t1.name()
  assert t3.resources() == r111
  assert set(t3.processes()) == set([p1, p2, p3, p4])
  assert set(t3.constraints()) == set(order(p1, p2) + order(p3, p4))

  t4 = concat_tasks(t1, t2)
  assert t4.name() == t1.name()
  assert t4.resources() == r111
  assert set(t4.processes()) == set([p1, p2, p3, p4])
  assert set(t4.constraints()) == set(
      order(p1, p2) + order(p3, p4) + order(p1, p3) + order(p1, p4) +
      order(p2, p3) + order(p2, p4))


def test_tasklets():
  install_thermosrc = Process(name='install_thermosrc')
  setup_py3k = Process(name='setup_py3k')
  setup_ruby19 = Process(name='setup_ruby19')
  setup_php = Process(name='setup_php')
  recipe_py3k = SequentialTask(processes=[install_thermosrc, setup_py3k])
  recipe_ruby19 = SequentialTask(processes=[install_thermosrc, setup_ruby19])
  recipe_php = SequentialTask(processes=[install_thermosrc, setup_php])
  all_recipes = Tasks.combine(recipe_py3k, recipe_ruby19, recipe_php)
  my_task = Task(processes = [Process(name='my_process')])
  my_new_task = Tasks.concat(all_recipes, my_task)(name = 'my_task')

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
