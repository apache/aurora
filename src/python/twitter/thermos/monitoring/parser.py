import re
from ctypes import *

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

# Parser takes:
#
#  ATTRS       = [name1, name2, ...]
#  TYPES       = [conv1, conv2, ...]
#  HANDLER_MAP = {name1: postprocessor1, name2: postprocessor2, ...}
#
class ScanfParser(object):
  # since python doesn't have built-in scanf, we have to emulate it
  # with regular expressions
  CONVERSIONS = {
     "%c": (".", c_char),
     "%d": ("[-+]?\d+", c_int),
     "%ld": ("[-+]?\d+", c_long),
     "%lld": ("[-+]?\d+", c_longlong),
     "%f": (r"(?:\d+(?:\.\d*)?|\.\d+)", c_float),
     "%s": ("\S+", c_char_p),
     "%u": ("\d+", c_uint),
     "%lu": ("\d+", c_ulong),
     "%llu": ("\d+", c_ulonglong),
  }

  # ctypes don't do str->int conversion, so must preconvert for non-string types
  PRECONVERSIONS = {
    c_int: int,
    c_long: int,
    c_longlong: long,
    c_uint: int,
    c_ulong: int,
    c_ulonglong: long,
    c_float: float,
    c_double: float
  }

  def _construct_re(self):
    regex  = []
    re_fn  = []

    for attr, typ in zip(self._attrs, self._types):
      conv_obj = ScanfParser.CONVERSIONS[typ]
      regex.append('(%s)' % conv_obj[0])
      re_fn.append(conv_obj[1])
    regex = " ".join(regex)
    regex = re.compile(regex)
    return (regex, re_fn)

  def parse(self, line):
    k = 0
    matches = {}
    matcher = self._regex.match(' '.join(line.split()))
    if matcher is None: return None
    for match in matcher.groups():
      if self._regex_fns[k] in ScanfParser.PRECONVERSIONS:
        match = ScanfParser.PRECONVERSIONS[self._regex_fns[k]](match)
      if self._attrs[k] in self._handlers:
        matches[self._attrs[k]] = self._handlers[self._attrs[k]](self._attrs[k], self._regex_fns[k](match).value)
      else:
        matches[self._attrs[k]] = self._regex_fns[k](match).value
      k += 1
    return ScanfObject(self._attrs, matches)

  def __init__(self, attrs, types, handlers = {}):
    self._handlers  = handlers
    self._attrs     = attrs
    self._types     = types
    self._regex, self._regex_fns = self._construct_re()

class ScanfObject(object):
  def __init__(self, attrs, data):
    self._attrs = attrs
    self._data  = data

  def __getattr__(self, key):
    if key in self._attrs:
      return self._data[key]
    else:
      raise AttributeError('Could not find attribute: %s' % key)

