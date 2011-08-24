import os
import errno
import time
import json
import urllib
import threading

from twitter.common import log
from twitter.common.recordio import ThriftRecordReader

# switch to non-relative imports
from detector import TaskDetector
from muxer    import TaskMuxer
from measure  import TaskMeasurer

from twitter.thermos.base import TaskPath
from twitter.thermos.base import Helper
from twitter.thermos.base.ckpt import AlaCarteRunnerState

from thermos_thrift.ttypes import *
from tcl_thrift.ttypes import ThermosJobHeader

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

# Support http queries:
#
# Implement these as endpoints that return JSON.
#
#    /uids?offset=X => uids(offset=X) <- list(uids)
#    /uids?type=active[&offset=X] => active_uids(offset=X) <- list(uids)
#    /uids?type=finished[&offset=X] => finished_uids(offset=X) <- list(uids)
#
#    /task?uid=UID =>
#       task(uid) <- (job_uid, job_name, task_name, task_replica_id,
#                     [active_processes], [finished_processes])
#
#    /process?uid=UID&process=TASK[&run=RUN] => task_process(uid, process, run=-1) <- list(runs)
#      run = (process_name, run number,
#             req_res, used_res, max_res,
#             start time, finish time,
#             finish state)
#      res = (cpu, ram, disk)
#
#    /logs?uid=UID[&file=RELPATH][&fmt=RAW|JSON|HTML]

# TODO(wickman)  This should go into a separate file.
class Fileset(object):
  def __init__(self, root, relative, url_args):
    self._root  = root
    self._rel   = relative
    self._files = []
    self._dirs  = []
    self._args  = url_args
    self._walk()

  # TODO(wickman): This deserves a test
  def _walk(self):
    abs_root = os.path.abspath(self._root)
    abs_path = os.path.abspath(os.path.join(self._root, self._rel))
    include_parent = abs_root != abs_path
    for path, dirs, files in os.walk(abs_path):
      self._dirs = list(dirs)
      if include_parent: self._dirs.insert(0, '..')
      for f in files:
        try:
          fstat = os.stat(os.path.join(abs_path, f))
          self._files.append([f, fstat.st_size])
        except OSError, e:
          if e.errno == errno.ENOENT:
            continue
          else:
            raise e
        except:
          log.error("Error statting file: %s (%s)" % (f, e))
      break

  # TODO(wickman):  Abstract into a helper.
  @staticmethod
  def _dump_json(obj):
    d = { 'data': obj }
    return json.dumps(d)

  def to_json(self):
    d = {
      "files": self._files,
      "dirs": self._dirs
    }
    return Fileset._dump_json(d)

  LOG_TEMPLATE = """
  <html>
  <title>Log Printer</title>
  <body>
    <h3>%(toplevel_path)s</h3>
    <table border=0 cellpadding=0 cellspacing=5 align=left>
      %(rows)s
    </table>
  </body>
  </html>
  """
  LOG_ROW_TEMPLATE = "<tr><td>%(download)s</td><td>%(link)s</td></tr>"

  @staticmethod
  def _download_link(filename, url_args):
    args = dict(url_args)
    args.update({'file': filename})
    return "<a href='/download?%s'><font size=1>DL</font></a>" % urllib.urlencode(args)

  @staticmethod
  def _file_link(filename, filesize, url_args):
    args = dict(url_args)
    args.update({'filename': filename, 'fmt': 'html'})
    return "<a href='/file?%s'>%s</a> %s" % (
      urllib.urlencode(args), os.path.basename(filename), filesize)

  @staticmethod
  def _directory_link(directory, url_args):
    args = dict(url_args)
    args.update({'path': directory, 'fmt': 'html'})
    return "<a href='/logs?%s'>%s</a>" % (
      urllib.urlencode(args), "%s/" % os.path.basename(directory))

  def to_html(self):
    out_dir_lines = []
    out_file_lines = []
    for dir in self._dirs:
      out_dir_lines.append(self._directory_link(os.path.join(self._rel, dir), self._args))
    for f in self._files:
      filepath = os.path.join(self._rel, f[0])
      out_file_lines.append([self._download_link(filepath, self._args),
                             self._file_link(filepath, f[1], self._args)])
    out_lines = []
    for line in out_dir_lines:
      out_lines.append(Fileset.LOG_ROW_TEMPLATE % {
        "download": "", "link": line })
    for line in out_file_lines:
      out_lines.append(Fileset.LOG_ROW_TEMPLATE % {
        "download": line[0], "link": line[1] })
    return Fileset.LOG_TEMPLATE % {
      "toplevel_path": self._rel,
      "rows": '\n'.join(out_lines) }

class TaskObserver(threading.Thread):
  class UnexpectedError(Exception): pass
  class UnexpectedState(Exception): pass

  def __init__(self, root):
    self._pathspec = TaskPath(root = root)
    self._detector = TaskDetector(root)
    self._muxer    = TaskMuxer(self._pathspec)
    self._measurer = TaskMeasurer(self._muxer)
    self._measurer.start()
    self._states   = {}
    self._actives  = set([])  # uid sets
    self._finishes = set([])  #    ..
    self._observed = set([])  #    ..
    self._jobs     = {}       # memoized uid => ThermosJobHeader map
    threading.Thread.__init__(self)

  def run(self):
    total_seconds = 0
    while True:
      time.sleep(1)
      total_seconds += 1
      active_jobs   = self._detector.get_active_uids()
      finished_jobs = self._detector.get_finished_uids()

      self._muxer.lock()
      for active in active_jobs:
        if active in self._finishes: raise Exception("Huh?")
        if active not in self._actives:
          self._actives.add(active)
          self._muxer.add(active)
          self._observed.add(active)
          self._job(active)  # memoize job
          log.debug('pid %s -> active' % active)
          # do stuff
      for finished in finished_jobs:
        if finished in self._actives:
          log.debug('pid %s active -> finished' % finished)
          self._actives.remove(finished)
          self._muxer.pop(finished)
          # do stuff
        self._finishes.add(finished)
      self._muxer.unlock()

  # ugh i kind of hate this distinction now.
  # should we kill it?
  def _job(self, uid):
    job = self._jobs.get(uid, None)
    if job: return job

    uid_map = {
      'active_job_path': self._actives,
      'finished_job_path': self._finishes
    }

    for path_type, uidset in uid_map.iteritems():
      if uid in uidset:
        path = self._pathspec.given(job_uid = uid).getpath(path_type)
        try:
          fp = file(path, "r")
          rr = ThriftRecordReader(fp, ThermosJobHeader)
          job = rr.read()
          if job:
            self._jobs[uid] = job
            return job
        except:
          pass

    return None

  def uid_count(self, type):
    if type == 'all' or type is None:
      return TaskObserver._dump_json(len(self._actives) + len(self._finishes))
    elif type == 'active':
      return TaskObserver._dump_json(len(self._actives))
    elif type == 'finished':
      return TaskObserver._dump_json(len(self._finishes))
    else:
      return TaskObserver._dump_json(0)

  # /uids?offset=X => uids(offset=X) <- list(uids)
  # /uids?type=active[&offset=X][&num=NUM] => active_uids(offset=X) <- list(uids)
  # /uids?type=finished[&offset=X][&num=NUM] => finished_uids(offset=X) <- list(uids)
  def uids(self, type, offset=0, num=None):
    agg = []
    if type == 'all':
      agg += self._actives
      agg += self._finishes
    elif type == 'active':
      agg += self._actives
    elif type == 'finished':
      agg += self._finishes
    agg.sort()

    if offset < 0:
      if len(agg) > abs(-offset):
        offset = offset % len(agg)
      else:
        offset = 0
    if num:
      num += offset
    return TaskObserver._dump_json(agg[offset:num])

  @staticmethod
  def _dump_json(obj):
    d = { 'data': obj }
    return json.dumps(d)

  def _state(self, uid):
    if uid in self._actives:
      return self._muxer.get_state(uid)
    elif uid in self._states:
      return self._states[uid]
    else:
      path = self._pathspec.given(job_uid = uid).getpath('runner_checkpoint')
      self._states[uid] = AlaCarteRunnerState(path).state()
      return self._states[uid]
    log.error(TaskObserver.UnexpectedError("Could not find uid: %s" % uid))
    return None

  def _job_stub(self, uid):
    job = self._job(uid)
    if job is None: return {}
    d = {
      'name':       job.name,
      'role':       job.role,
      'user':       job.user,
      'datacenter': job.datacenter,
      'cluster':    job.cluster
    }
    return d

  def _task_processes(self, uid):
    if uid not in self._actives and uid not in self._finishes:
      return {}
    state = self._state(uid)
    if state is None:
      return {}

    waiting, running, success, failed = [], [], [], []
    for process in state.processes:
      runs = state.processes[process].runs
      if len(runs) == 0:
        waiting.append(process)
      else:
        if state.processes[process].state == TaskState.ACTIVE:
          if runs[-1].run_state == ProcessRunState.WAITING:
            waiting.append(process)
          else:
            running.append(process)
        elif state.processes[process].state == TaskState.SUCCESS:
          success.append(process)
        elif state.processes[process].state == TaskState.FAILED:
          failed.append(process)
        else:
          raise TaskObserver.UnexpectedState(
            "Unexpected TaskState: %s" % state.processes[process].state)

    return {
      'waiting': waiting,
      'running': running,
      'success': success,
      'failed':  failed
    }

  # /task?uid=UIDLIST
  # {
  #   data: {
  #     {uid}: {
  #        uid: int,
  #        job: { name: string, X owner: string, X group: string }
  #        name: string,
  #        replica: int,
  #        state: string [ACTIVE, SUCCESS, FAILED]
  #     X  ports: { name1: 'url', name2: 'url2' }
  #        resource_consumption: {cpu: , ram: , disk:}
  #     X  timeline: {
  #           axes: [ 'time', 'cpu', 'ram', 'disk'],
  #           data: [ (1299534274324, 0.9, 1212492188, 493932999392),
  #                   (1299534275327, 0.92, 23432423423, 52353252343), ... ]
  #        }
  #        processes: { -> names only
  #           waiting: [],
  #           running: [],
  #           success: [],
  #           failed:  []
  #        }
  #     }
  #  }
  def _task(self, uid):
    if uid not in self._actives and uid not in self._finishes:
      return {}
    state = self._state(uid)
    if state is None:
      return {}

    d = {    'uid':                       uid,
            'name':    state.header.task_name,
         'replica': state.header.task_replica,
           'state': TaskState._VALUES_TO_NAMES[state.state] }
    d['job'] = self._job_stub(uid)

    # -----
    cpu = self._measurer.current_cpu_by_uid(uid)
    d['resource_consumption'] = {
       'cpu': cpu,
       'ram':   0, # TODO(wickman)
       'disk':  0  # TODO(wickman)
    }
    d['processes'] = self._task_processes(uid)

    return d

  def task(self, uids):
    output = {}
    for uid in uids:
      w = self._task(uid)
      if w:
        output[uid] = w
    return TaskObserver._dump_json(output)

  # /processes?uid=<tsk_uid1>,<tsk_uid2>,...
  #  {
  #    data:
  #    {
  #      task_uid = tsk_uid
  #      waiting:  [process1, ..., processN]
  #      running:  [process1, ..., processN]
  #      finished: [process1, ..., processN]
  #    }
  # }
  #
  # process schema {
  #   name: string
  #   run:  int
  #   res: { cpu: float, ram: int bytes, disk: int bytes }
  #   use: { cpu: float, ram: int bytes, disk: int bytes }
  #   start_time: (time since epoch in millis (utc))
  #   finish_time: (time since epoch in millis (utc))
  # X endpoints: {}
  #   state: string [WAITING, FORKED, RUNNING, FINISHED, KILLED, FAILED, LOST]
  # }

  def _get_process_resource_reservation(self, uid, task_name, process_name):
    job = self._job(uid)
    if job is None: return {}
    tsk = Helper.task_from_job(job, task_name)
    if tsk is None: return {}
    process = Helper.process_from_task(tsk, process_name)
    if process is None: return {}
    return {
      'reserved': {
        'cpu':  process.footprint.cpu,
        'ram':  process.footprint.ram,
        'disk': process.footprint.disk
      }
    }

  def _get_process_resource_consumption(self, uid, task_name, process_name):
    return {
      'used': {
        'cpu': self._measurer.current_cpu_by_process(uid, task_name, process_name),
        'ram': 0, # implement
        'disk': 0 # implement
      }
    }

  def _get_process_tuple(self, history, run):
    # negative runs is fine: -1 is the last run
    if run >= len(history.runs):
      return {}
    else:
      process_run = history.runs[run]
      if run < 0: run = len(history.runs) + run
      log.debug('got process run: %s' % process_run)
      d = {
        'process_name': process_run.process,
        'process_run':  run,
        'state':        ProcessRunState._VALUES_TO_NAMES[process_run.run_state],
      }
      if process_run.start_time:
        d['start_time'] = process_run.start_time * 1000 # schema states millis since epoch
      if process_run.stop_time:
        d['stop_time'] = process_run.stop_time * 1000 # ^^

      # populate gathered metrics
      # TODO(wickman)
      return d

  # process schema {
  #   name: string
  #   run:  int
  #   res: { cpu: float, ram: int bytes, disk: int bytes }
  #   use: { cpu: float, ram: int bytes, disk: int bytes }
  #   start_time: (time since epoch in millis (utc))
  #   finish_time: (time since epoch in millis (utc))
  #   state: string [WAITING, FORKED, RUNNING, FINISHED, KILLED, FAILED, LOST]
  # }
  def _process(self, uid, process, run = None):
    state = self._state(uid)
    if process not in state.processes: return {}
    history = state.processes[process]

    if run is not None:
      runs = [run]
    else:
      runs = [range(len(history.runs))]

    output = []
    reservation = self._get_process_resource_reservation(uid, state.header.task_name, process)
    for r in runs:
      tup = self._get_process_tuple(history, r)
      tup.update(reservation)
      if tup['state'] == 'RUNNING':
        tup.update(self._get_process_resource_consumption(uid, state.header.task_name, process))
      output.append(tup)
    return output

  def process(self, uid, process, run = None):
    return TaskObserver._dump_json(self.process(uid, process, run))

  # /processes?uid=<tsk_uid1>,<tsk_uid2>,...
  #  {
  #    data:
  #    {
  #      task_uid = tsk_uid
  #      waiting:  [process1, ..., processN]
  #      running:  [process1, ..., processN]
  #      finished: [process1, ..., processN]
  #    }
  # }
  #
  def _processes(self, uid):
    # should we make this a standard preamble in a static?
    if uid not in self._actives and uid not in self._finishes:
      return {}
    state = self._state(uid)
    if state is None: return {}

    processes = self._task_processes(uid)
    d = { 'task_uid': uid }
    for process_type in processes:
      d[process_type] = []
      for process_name in processes[process_type]:
        t = self._process(uid, process_name, run = -1)
        d[process_type].append(t)
    return d

  def processes(self, uids):
    d = {}
    for uid in uids:
      t = self._processes(uid)
      d[uid] = t
    return TaskObserver._dump_json(d)

  #  /logs?uid=UID&process=TASK&run=RUN[&path=RELPATH][&fmt=JSON|HTML]
  #  /file?uid=UID&process=TASK&run=RUN[&filename=RELPATH][&fmt=JSON|HTML]
  #  /download?uid=UID&process=TASK&run=RUN&filename=RELPATH
  #  JSON:
  #  { data: {
  #      files: [ [name1, size1], [name2, size2], ... ]
  #      dirs: [ name1, name2, name3, ... ]
  #    } }
  @staticmethod
  def _sanitize_relpath(base_path, relpath):
    """
      Attempts to sanitize a path through path normalization, also making sure
      that the relative path is contained inside of base_path.
    """
    if relpath is None:
      relpath = "."
    normalized_base = os.path.abspath(base_path)
    normalized = os.path.abspath(os.path.join(base_path, relpath))
    if normalized.startswith(normalized_base):
      return (normalized_base, os.path.relpath(normalized, normalized_base))
    return (None, None)

  def _validate_path(self, uid, process, run, relpath):
    state = self._state(uid)
    if state is None:
      return (None, None)
    if process not in state.processes:
      return (None, None)
    try:
      run_number = int(run)
    except ValueError:
      return (None, None)
    if run_number >= len(state.processes[process].runs):
      return (None, None)
    process_info = state.processes[process].runs[run_number]
    log_path = self._pathspec.given(job_uid = uid, process = process, run = run).getpath('process_logdir')
    out_dirs, out_files = [], []
    sanitized_relpath = self._sanitize_relpath(log_path, relpath)
    return sanitized_relpath

  def download_path(self, uid, process, run, filename):
    (base_path, rel_path) = self._validate_path(uid, process, run, filename)
    return base_path

  def logs(self, uid, process, run, path=None, fmt="html"):
    url_args = { 'uid': uid, 'process': process, 'run': run }
    if path is None:
      path = "."
    (base_path, rel_path) = self._validate_path(uid, process, run, path)
    if base_path is None or fmt not in ('json', 'html'):
      return "404"  # TODO(wickman) 404Exception
    fileset = Fileset(base_path, rel_path, url_args)
    if fmt == 'json':
      return fileset.to_json()
    elif fmt == 'html':
      return fileset.to_html()

  # TODO(wickman): This deserves a test
  @staticmethod
  def _read_chunk(filename, offset=None, bytes=None):
    DEFAULT_CHUNK_LENGTH = 512 * 1024
    MAX_CHUNK_LENGTH = 16 * 1024 * 1024
    try:
      fstat = os.stat(filename)
      if not os.path.isfile(filename):
        return None
    except:
      return None
    filelen = fstat.st_size
    if bytes is None or bytes < 0:
      bytes = DEFAULT_CHUNK_LENGTH
    if bytes > MAX_CHUNK_LENGTH:
      bytes = MAX_CHUNK_LENGTH
    if offset is None:
      offset = filelen - bytes
    if offset < 0: offset = 0
    with open(filename, "r") as fp:
      fp.seek(offset)
      try:
        data = fp.read(bytes)
        return {
          'data': data,
          'filelen': filelen,
          'read': len(data),
          'offset': offset,
          'bytes': bytes
        }
      except:
        return None

  FILE_BROWSER_HTML = """
  <html>
  <title>Log Printer</title>
  <body>
    <h3>%(filename)s</h3>

     <a href="%(prev_link)s">prev</a>
     <a href="%(next_link)s">next</a>

     <br>
     <hr>
     <br>
     <pre>
       %(data)s
     </pre>
     <hr>
  </body>
  </html>
  """

  @staticmethod
  def _render_file_link(filename, offset, bytes, url_args):
    args = dict(url_args)
    args.update({ 'offset': offset, 'bytes': bytes, 'fmt': 'html' })
    return "/file?%s" % urllib.urlencode(args)

  @staticmethod
  def _render_file_chunk(chunk, filename, url_args):
    return TaskObserver.FILE_BROWSER_HTML % {
      'filename': filename,
      'data': chunk['data'],
      'prev_link': TaskObserver._render_file_link(
        filename, chunk['offset'] - chunk['bytes'], chunk['bytes'], url_args),
      'next_link': TaskObserver._render_file_link(
        filename, chunk['offset'] + chunk['bytes'], chunk['bytes'], url_args)
    }

  def browse_file(self, uid, process, run, filename, offset=None, bytes=None, fmt=None):
    url_args = { 'uid': uid, 'process': process, 'run': run, 'filename': filename }
    (base_path, rel_path) = self._validate_path(uid, process, run, filename)
    if base_path is None or fmt not in ('json', 'html'):
      return "404"  # TODO(wickman): Add 404Exception
    abs_path = os.path.join(base_path, rel_path)
    chunk = self._read_chunk(abs_path, offset, bytes)
    if chunk is None:
      return "404"
    if fmt == 'json':
      return TaskObserver._dump_json(chunk)
    elif fmt == 'html':
      return self._render_file_chunk(chunk, rel_path, url_args)
