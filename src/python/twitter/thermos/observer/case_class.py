__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

# E.g. ProcessClass = CaseClass('name', 'owner', 'pid')
# process1 = ProcessClass(name = "hello", owner = "brian", pid = 15)
# process2 = ProcessClass(name = "world", owner = "brian", pid = 13)
# processes = [process1, process2]
#
# filter(lambda process: process.where(owner = "brian"), processes) => [process1, process2]
# filter(lambda process: process.where(owner = "brian", pid = 13), processes) => [process2]
#
# matcher = ProcessClass(pid = 13)
# filter(lambda process: process.match(matcher), processes) => [process2]

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
