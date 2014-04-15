import pipes

from twitter.common.lang import Compatibility


def shellify(dict_, export=False, prefix=""):
  """Dump a dict to a shell script."""
  if export:
    prefix = "export " + prefix
  def _recurse(k, v, prefix):
    if isinstance(v, bool):
      v = int(v)
    if isinstance(v, int):
      yield "%s=%s" % (prefix + k, + v)
    if isinstance(v, Compatibility.string):
      yield "%s=%s" % (prefix + k, pipes.quote(str(v)))
    elif isinstance(v, dict):
      for k1, v1 in v.items():
        for i in _recurse(k1.upper(), v1, prefix=prefix + k + "_"):
          yield i
    elif isinstance(v, list):
      for k1, v1 in enumerate(v):
        for i in _recurse(str(k1).upper(), v1, prefix=prefix + k + "_"):
          yield i
  for k, v in dict_.items():
    for i in _recurse(k.upper(), v, prefix):
      yield i
