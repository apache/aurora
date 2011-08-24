import os
import errno
from parser import ScanfParser, ScanfObject
from ctypes import *

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False
__todo__   = """
  There is painful amount of copy-pasted code in here.  Ripe for refactor.
"""
class ProcessHandlers(object):
  @staticmethod
  def handle_time(key, value):
    return 1.0 * value / os.sysconf('SC_CLK_TCK')

  @staticmethod
  def handle_proc_mem(key, value):
    return value * os.sysconf('SC_PAGESIZE')

  @staticmethod
  def handle_ps_mem(key, value):
    return value * 1024

  @staticmethod
  def handle_elapsed(key, value):
    seconds = 0

    unpack = value.split('-')
    if len(unpack) == 2:
      seconds += int(unpack[0]) * 86400
      unpack = unpack[1]
    else:
      unpack = unpack[0]

    unpack = unpack.split(':')
    mult = 1.0
    for k in range(len(unpack), 0, -1):
      seconds += float(unpack[k-1]) * mult
      mult    *= 60

    return seconds

class ProcessHandleProcfs(object):
  ATTRS = \
    """pid comm state ppid pgrp session tty_nr tpgid flags minflt cminflt majflt cmajflt utime
       stime cutime cstime priority nice num_threads itrealvalue starttime vsize rss rsslim startcode
       endcode startstack kstkesp kstkeip signal blocked sigignore sigcatch wchan nswap cnswap
       exit_signal processor rt_priority policy""".split()

  TYPE_MAP = {
            "pid":   "%d",         "comm":   "%s",       "state":  "%c",        "ppid":  "%d",
           "pgrp":   "%d",      "session":   "%d",      "tty_nr":  "%d",       "tpgid":  "%d",
          "flags":   "%u",       "minflt":  "%lu",     "cminflt": "%lu",      "majflt": "%lu",
        "cmajflt":  "%lu",        "utime":  "%lu",       "stime": "%lu",      "cutime": "%ld",
         "cstime":  "%ld",     "priority":  "%ld",        "nice": "%ld", "num_threads": "%ld",
    "itrealvalue":  "%ld",    "starttime": "%llu",       "vsize": "%lu",         "rss": "%ld",
         "rsslim":  "%lu",    "startcode":  "%lu",     "endcode": "%lu",  "startstack": "%lu",
        "kstkesp":  "%lu",      "kstkeip":  "%lu",      "signal": "%lu",     "blocked": "%lu",
      "sigignore":  "%lu",     "sigcatch":  "%lu",       "wchan": "%lu",       "nswap": "%lu",
         "cnswap":  "%lu",  "exit_signal":   "%d",   "processor":  "%d", "rt_priority":  "%u",
         "policy":   "%u"
  }

  ALIASES = {
      'vsz': 'vsize',
     'stat': 'state',
  }

  HANDLERS = {
     'utime': ProcessHandlers.handle_time,
     'stime': ProcessHandlers.handle_time,
    'cutime': ProcessHandlers.handle_time,
    'cstime': ProcessHandlers.handle_time,
       'rss': ProcessHandlers.handle_proc_mem
  }

  SCANF_PARSER = None

  def _readline(self, line):
    self._exists = True
    self._attrs  = ProcessHandleProcfs.SCANF_PARSER.parse(line)
    self._pid    = self._attrs.pid

  def _read(self):
    try:
      data = file("/proc/%s/stat" % self._pid, "r").read() # does this leak fhs?
      self._readline(data)
    except IOError, e:
      # 2: file doesn't exist
      # 3: no such process
      if e.errno in (2, 3):
        self._exists = False
        self._attrs  = ScanfObject([], {})
      else:
        raise e
    return self._exists

  @staticmethod
  def from_line(line):
    ph = ProcessHandleProcfs()
    ph._readline(line)
    return ph

  @staticmethod
  def _init_parser():
    if ProcessHandleProcfs.SCANF_PARSER: return
    ProcessHandleProcfs.SCANF_PARSER = \
      ScanfParser(ProcessHandleProcfs.ATTRS,
                  [ProcessHandleProcfs.TYPE_MAP[attr] for attr in ProcessHandleProcfs.ATTRS],
                  ProcessHandleProcfs.HANDLERS)

  def refresh(self):
    self._read()

  def __init__(self, pid=-1):
    ProcessHandleProcfs._init_parser()

    self._exists    = False
    self._pid       = pid

    if self._pid != -1:
      self._read()

  def __getattr__(self, key):
    if key in ProcessHandleProcfs.ALIASES:
      return self._attrs.__getattr__(ProcessHandleProcfs.ALIASES[key])
    else:
      return self._attrs.__getattr__(key)

class ProcessHandlePs(object):
  LINE = [ 'USER', 'PID', 'PPID', '%CPU', 'RSS', 'VSZ', 'STAT', 'ELAPSED', 'TIME', 'COMMAND' ]
  KEY  = [ 'user', 'pid', 'ppid', 'pcpu', 'rss', 'vsz', 'stat',   'etime', 'time',    'comm' ]
  TYPE = [   '%s',  '%d',   '%d',   '%f',  '%d',  '%d',   '%s',      '%s',   '%s',     '%s'  ]

  HANDLERS = {
    'rss':    ProcessHandlers.handle_ps_mem,
    'vsz':    ProcessHandlers.handle_ps_mem,
    'etime':  ProcessHandlers.handle_elapsed, # [[dd-]hh:]mm:ss.ds
    'time':   ProcessHandlers.handle_elapsed, # [[dd-]hh:]mm:ss.ds
  }

  SCANF_PARSER = None

  def _readline(self, line):
    self._exists = True
    self._attrs  = ProcessHandlePs.SCANF_PARSER.parse(line)
    if self._attrs is None:
      print '_readline got line: %s' % line
      self._exists = False
    else:
      self._pid    = self._attrs.pid

  def _read(self):
    try:
      data = os.popen('ps -p %s -o %s' % (self._pid, ','.join(ProcessHandlePs.KEY))).readlines()
      if len(data) > 1:
        self._readline(data[-1])
      else:
        self._exists = False
        self._attrs  = ScanfObject([], {})
    except IOError, e:
      # 2: file doesn't exist
      # 3: no such process
      if e.errno in (2, 3):
        self._exists = False
        self._attrs  = ScanfObject([], {})
      else:
        raise e
    return self._exists

  @staticmethod
  def _init_parser():
    if ProcessHandlePs.SCANF_PARSER: return
    ProcessHandlePs.SCANF_PARSER = \
      ScanfParser(ProcessHandlePs.KEY, ProcessHandlePs.TYPE, ProcessHandlePs.HANDLERS)

  @staticmethod
  def from_line(line):
    ph = ProcessHandlePs()
    ph._readline(line)
    return ph

  def refresh(self):
    self._read()

  def __init__(self, pid=-1):
    ProcessHandlePs._init_parser()

    self._exists    = False
    self._pid       = pid

    if pid != -1:
      self._read()

  def __getattr__(self, key):
    return self._attrs.__getattr__(key)

class ProcessTree(object):
  CMD = 'ps ax -o user,pid,ppid'

  def __init__(self):
    self._table            = {}
    self._pid_to_children  = {}
    self.refresh()

  def refresh(self):
    self._pid_to_parent   = {}
    self._pid_to_children = {}
    lines = os.popen(ProcessTree.CMD).readlines()[1:]
    for line in lines:
      user, pid, ppid = line.split()
      pid, ppid = int(pid), int(ppid)
      self._table[pid] = (ppid, user)
      if ppid not in self._pid_to_children: self._pid_to_children[ppid] = []
      self._pid_to_children[ppid].append(pid)

  def children_of(self, pid):
    all_children = []
    for child in self._pid_to_children.get(pid, []):
      all_children.append(child)
      all_children.extend(self.children_of(child))
    return all_children

class ProcessSetRealizer(object):
  def collect(self):
    print 'zing!'

  def realize(self, pid):
    print 'zong!'

  def _reinit_pids(self, pids):
    for pid in pids:
      ph = self.get(pid)
      if ph: self._pid_to_children[ph.ppid].remove(pid)
    for pid in pids: self._lines.pop(pid, None)
    for pid in pids: self._realizations.pop(pid, None)
    self._pids = self._pids - set(pids)

  def _reinit(self):
    self._lines        = {}
    self._realizations = {}
    self._pids         = set([])
    self._pid_to_children = {}

  def __init__(self):
    self._reinit()
    self.refresh()

  def refresh(self):
    self.collect()

  def refresh_set(self, pids):
    self.refresh()

  def get(self, pid):
    if pid not in self._pids:
      return None
    if pid not in self._realizations:
      self._realizations[pid] = self.realize(pid)
    return self._realizations[pid]

  def _children_of(self, pid):
    all_children = []
    for child in self._pid_to_children.get(pid, []):
      all_children.append(child)
      all_children.extend(self._children_of(child))
    return all_children

  def get_children(self, pid):
    all_children = self._children_of(pid)
    all_realized = [self.get(p) for p in all_children]
    return all_realized

  def get_set(self, pidset):
    all_realized = [self.get(p) for p in pidset]
    return all_realized

class ProcessSetRealizer_PS(ProcessSetRealizer):
  def _crunch_data(self, lines):
    for line in lines:
      sline = line.split()
      if sline[0] == 'USER': continue
      pid, ppid = int(sline[1]), int(sline[2])
      self._pids.add(pid)
      self._lines[pid] = line
      if ppid not in self._pid_to_children: self._pid_to_children[ppid] = []
      self._pid_to_children[ppid].append(pid)

  def collect(self):
    self._reinit()
    data = os.popen('ps ax -o %s' % (','.join(ProcessHandlePs.KEY))).readlines()
    self._crunch_data(data)

  def refresh_set(self, pids):
    self._reinit_pids(pids)
    data = os.popen('ps -p %s -o %s' % (
      ','.join(list(pids)), ','.join(ProcessHandlePs.KEY))).readlines()
    self._crunch_data(data)

  def realize(self, pid):
    if pid not in self._pids: return None
    return ProcessHandlePs.from_line(self._lines[pid])

  def __init__(self):
    ProcessSetRealizer.__init__(self)

class ProcessSetRealizer_Procfs(ProcessSetRealizer):
  def _crunch_data(self, lines):
    for line in lines:
      sline = line.split()
      pid, ppid = int(sline[0]), int(sline[3])
      self._pids.add(pid)
      self._lines[pid] = line
      if ppid not in self._pid_to_children: self._pid_to_children[ppid] = []
      self._pid_to_children[ppid].append(pid)

  def collect(self):
    self._reinit()
    data = os.popen('cat /proc/[0-9]*/stat 2>/dev/null').readlines()
    self._crunch_data(data)

  def refresh_set(self, pids):
    self._reinit_pids(pids)
    data = os.popen('cat /proc/{%s}/stat 2>/dev/null' % ','.join(list(pids))).readlines()
    self._crunch_data(data)

  def realize(self, pid):
    if pid not in self._pids: return None
    return ProcessHandleProcfs.from_line(self._lines[pid])

  def __init__(self):
    ProcessSetRealizer.__init__(self)

class ProcessSetFactory(object):
  @staticmethod
  def get():
    kernel = os.uname()[0]
    if kernel == 'Darwin':
      return ProcessSetRealizer_PS()
    elif kernel == 'Linux':
      return ProcessSetRealizer_Procfs()
    else:
      return None
