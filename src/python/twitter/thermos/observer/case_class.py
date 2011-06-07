__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

# E.g. TaskClass = CaseClass('name', 'owner', 'pid')
# task1 = TaskClass(name = "hello", owner = "brian", pid = 15)
# task2 = TaskClass(name = "world", owner = "brian", pid = 13)
# tasks = [task1, task2]
#
# filter(lambda task: task.where(owner = "brian"), tasks) => [task1, task2]
# filter(lambda task: task.where(owner = "brian", pid = 13), tasks) => [task2]
#
# matcher = TaskClass(pid = 13)
# filter(lambda task: task.match(matcher), tasks) => [task2]

class CaseClass:
  def __init__(self, *kw):
    self._attrs = kw

  def __call__(self, **d):
    cc = CaseClass(*self._attrs)
    cc._d = d
    return cc

  def set(self, **d):
    for attr in d:
      if attr not in self._attrs: raise Exception("Unknown attribute: %s" % attr)
      self._d[attr] = d[attr]

  def __eq__(self, other):
    if self._attrs == other._attrs:
      return self._d == other._d
    return False

  def __hash__(self):
    return self.__str__().__hash__()

  def __str__(self):
    return '-'.join('%s:%s' % (a, self._d[a]) for a in self._attrs)

  def filter(self, attr, value):
    if attr not in self._attrs: raise Exception("Unknown attribute: %s" % attr)
    if self._d[attr] == value:
      return True
    else:
      return False

  def where(self, **d):
    for attr in d:
      if not self.filter(attr, d[attr]):
        return None
    return self

  def match(self, other_class):
    if self._attrs != other_class._attrs: raise Exception("Comparing incompatible case classes")
    return self.where(**other_class._d)
