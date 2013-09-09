from collections import namedtuple
from functools import partial


__all__ = [
  'Hook',
  'JoinPoint',
]


RawJoinPoint = namedtuple('RawJoinPoint', ['time', 'method'])


class JoinPoint(RawJoinPoint):
  """
    A join point is an opportunity for code to be executed
    Here we have restricted it to just a time and a method (and without wildcards)
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
    Hook is the definition of actions associated around API calls
    E.g. action before API call create_job is defined by the hook 'self.pre_create_job'

    Subclasses may:
    1. define the specific action for the specific time + API call
       E.g.: Define 'pre_create_job' that runs before API event 'create_job'
    2. define the generic action 'generic_hook' that is a fallback for all times and API calls
    3. combine: (1) for specific cases and (2) for fallback cases

    All hooks receive:
    1. job_key sent to the target API method (also when sent implicitly via the 'config' param)
    2. result_or_err from executing the target method (None for 'pre' hooks)
    3. *args and **kwargs that the target API method received

    Examples:

    class Logger(Hook):
      '''Just logs every at all point for all API calls'''
      def generic_hook(self, join_point, job_key, result_or_err, *args, **kw)
         log.info('%s: %s of %s' % (self.__class__.__name__, join_point.name(), job_key))

    class KillConfirmer(Hook):
      def confirm(self, msg):
        return True if raw_input(msg).lower() == 'yes' else False

      def pre_kill(self, job_key, shards=None):
        shards = shards if shards else 'all'
        return self.confirm('Are you sure? Kill %s (shards = %s)?' % (job_key, shards))

  """

  def generic_hook(self, join_point, job_key, result_or_err, *args, **kwargs):
    """
      Proxy hook used when a hook for a specific JoinPoint is not defined.
    """
    return True

  def __getattr__(self, attribute):
    try:
      join_point = JoinPoint.from_name(attribute)
    except JoinPoint.Invalid:
      return self.__getattribute__(attribute)
    return partial(self.generic_hook, join_point)
