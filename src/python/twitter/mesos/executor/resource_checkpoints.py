""" Read and write checkpointed history of resource consumption of a task """

import os
import threading
import time

from thrift.TSerialization import serialize as thrift_serialize

from twitter.common import log
from twitter.common.exceptions import ExceptionalThread
from twitter.common.quantity import Amount, Time
from twitter.common.recordio import ThriftRecordReader
from twitter.common.recordio import ThriftRecordWriter
from twitter.thermos.monitoring.process import ProcessSample
from twitter.thermos.monitoring.resource import (
  ResourceHistory,
  ResourceMonitorBase,
  TaskResourceMonitor
)

from gen.twitter.mesos.comm.ttypes import TaskResourceSample


class CheckpointResourceMonitor(TaskResourceMonitor, ExceptionalThread):
  """ Monitor designed to expose resource utilisation from a checkpointed history """

  MAX_HISTORY = 10000

  @staticmethod
  def sample_to_resource_result(sample):
    """Convert a TaskResourceSample to a ResourceResult."""
    # TODO(jon): ugh. refactor to obviate need for this:
    # - have ResourceManager deal with ResourceResults not TaskResourceSamples
    # - similarly, ResourceEnforcer doesn't need to use TRS
    # - have checkpoint read/writers doing the only ResourceResult <-> TaskResourceSample conversion
    mapping = {
      'rate': 'cpuRate',
      'user': 'cpuUserSecs',
      'system': 'cpuSystemSecs',
      'rss': 'ramRssBytes',
      'vms': 'ramVssBytes',
      'nice': '__undefined__',
      'status': '__undefined__',
      'threads': 'numThreads',
    }
    kwargs = dict((k, getattr(sample, v, None)) for k, v in mapping.items())
    return ResourceMonitorBase.ResourceResult(
      sample.numProcesses,
      ProcessSample(**kwargs),
      sample.diskBytes,
    )

  def __init__(self, filename, wait_interval=Amount(15, Time.SECONDS)):
    """
    """
    try:
      fh = open(filename)
      # TODO(jon): create separate recordios for each day, so we don't have to replay the entire
      # history?
    except (IOError, OSError) as err:
      log.error('Error initialising CheckpointResourceMonitor from %s: %s' % (filename, err))
      raise self.Error
    self._filename = filename
    self._reader = ThriftRecordReader(fh, TaskResourceSample)
    self._history = ResourceHistory(self.MAX_HISTORY, initialize=False)
    self._wait_interval = wait_interval.as_(Time.SECONDS)
    self._kill_signal = threading.Event()
    ExceptionalThread.__init__(self)
    self.daemon = True
    log.debug("Initialising resource sample history from %s..." % filename)
    count = 0
    while not self._kill_signal.is_set():
      sample = self._reader.read()
      if not sample: break
      self._add_sample_to_history(sample)
      count += 1
    log.debug("Done (got %d resource samples)" % count)

  def _add_sample_to_history(self, sample):
    self._history.add(
      sample.microTimestamp / 1e6,
      self.sample_to_resource_result(sample)
    )

  def run(self):
    """Thread entrypoint. Loop indefinitely, polling checkpointed history for new samples."""
    log.debug("Polling %s for resource checkpoints" % self._filename)
    while not self._kill_signal.is_set():
      sample = self._reader.read()
      if sample:
        log.debug("Got new sample %s" % sample)
        self._add_sample_to_history(sample)
      self._kill_signal.wait(timeout=self._wait_interval)

  def sample_by_process(self, process_name):
    raise NotImplementedError("CheckpointResourceMonitor does not support per-process sampling.")


class ResourceCheckpointer(ExceptionalThread):
  """ Thread to periodically log snapshots of resource consumption to a file """
  COLLECTION_INTERVAL = Amount(1, Time.MINUTES)

  @staticmethod
  def recordio_writer(checkpoint):
    trw = ThriftRecordWriter(open(checkpoint, 'wb'))
    trw.set_sync(True)
    return trw

  @staticmethod
  def static_writer(checkpoint):
    class Writer(object):
      def write(self, record):
        with open(checkpoint + '~', 'wb') as fp:
          fp.write(thrift_serialize(record))
        os.rename(checkpoint + '~', checkpoint)
    return Writer()

  def __init__(self, sample_provider, filename, recordio=False):
    """
      sample_provider: callable returning the sample to be recorded
      filename: full path to a file in which to record the snapshots
      recordio: boolean indicating whether to write records  [default: False]
    """
    self._sample_provider = sample_provider
    self._output = self.recordio_writer(filename) if recordio else self.static_writer(filename)
    super(ResourceCheckpointer, self).__init__()
    self.daemon = True

  def run(self):
    while True:
      time.sleep(self.COLLECTION_INTERVAL.as_(Time.SECONDS))
      sample = self._sample_provider()
      if sample:
        self._output.write(sample)

