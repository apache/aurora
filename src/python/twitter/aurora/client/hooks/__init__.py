from collections import namedtuple
from functools import partial


__all__ = [
  'Hook',
  'JoinPoint',
]


RawJoinPoint = namedtuple('RawJoinPoint', ['time', 'method'])


class JoinPoint(RawJoinPoint):
  """
    A join point (ala AOP) is an opportunity for code to be executed
    We have restricted it to just a time and a method (i.e. no * wildcards)
  """

  class Error(Exception): pass
  class Invalid(ValueError, Error): pass

  TIMES = ('pre', 'post', 'err')

  def __init__(self, *args, **kw):
    try:
      super(JoinPoint, self).__init__(*args, **kw)
    except TypeError:
      raise self.Invalid('Invalid # of args in %s' % repr(self))
    if self.time not in self.TIMES:
      raise self.Invalid('Invalid time in %s' % repr(self))

  def name(self):
    return '%s_%s' % (self.time, self.method)

  @classmethod
  def from_name(cls, name):
    return cls(*name.split('_', 1))


class Hook(object):
  """
    Hook (Pointcut in AOP) is a selection of instances of JoinPoint and associated advice
    All advice is defined as a method here and refered to as a 'hook'
    Also we have chosen to keep the (string) 'hook' name the same as the JoinPoint name
    i.e. advice for JoinPoint(time=pre, name=create_job) is defined by hook 'self.pre_create_job'

    1. Subclasses may define the advice for a specific instance JoinPoint. E.g.:
       Define pre_create_job that runs before API event create_job (in case of AuroraClientAPI)
    2. Subclasses may override the generic advice 'generic_hook' that applies to all JoinPoints
    3. Subclasses may combine behavior by doing (2) and for selected JoinPoints do (1)

    All hooks receive:
    1. job_key sent to the target API method (also when sent implicitly via the 'config' param)
    2. result_or_err from executing the target method (None for 'pre' hooks)
    3. *args and **kwargs that the target API method received
  """

  def generic_hook(self, join_point, method, result_or_err, *args, **kwargs):
    """
      Proxy for when a special JoinPoint behavior is not defined.
    """
    return True

  def __getattr__(self, attribute):
    try:
      join_point = JoinPoint.from_name(attribute)
    except JoinPoint.Invalid:
      return self.__getattribute__(attribute)
    return partial(self.generic_hook, join_point)
