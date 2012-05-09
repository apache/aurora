import threading
import time

import mesos_pb2 as mesos_pb

from twitter.common import log
from twitter.common.quantity import Amount, Time

from gen.twitter.thermos.ttypes import TaskState

from .executor_base import ThermosExecutorBase
from .health_checker import Healthy, HealthCheckerThread
from .http_signaler import HttpSignaler


class StatusManager(threading.Thread):
  POLL_WAIT = Amount(500, Time.MILLISECONDS)
  WAIT_LIMIT = Amount(1, Time.MINUTES)
  ESCALATION_WAIT = Amount(5, Time.SECONDS)

  def __init__(self, runner, driver, task_id, signal_port=None, check_interval=None, clock=time):
    self._driver = driver
    self._runner = runner
    self._task_id = task_id
    self._clock = clock
    self._unhealthy_event = threading.Event()
    if signal_port:
      self._signaler = HttpSignaler(signal_port)
      self._health_checker = HealthCheckerThread(self._signaler.health, check_interval or 30,
          clock=clock)
      self._health_checker.start()
    else:
      self._signaler = None
      self._health_checker = Healthy()
    threading.Thread.__init__(self)

  @property
  def unhealthy_event(self):
    return self._unhealthy_event

  def run(self):
    force_status = force_message = None
    while self._runner.is_alive():
      if not self._health_checker.healthy:
        self._unhealthy_event.set()
        self._health_checker.stop()
        self.terminate_unhealthy()
        force_status = mesos_pb.TASK_FAILED
        force_message = 'Failed health check!'
        break
      self._clock.sleep(self.POLL_WAIT.as_(Time.SECONDS))

    log.info('Executor polling thread detected termination condition.')
    self.terminate(force_status, force_message)

  def terminate_unhealthy(self):
    if not self._signaler:
      return

    # pass 1
    self._signaler.quitquitquit()
    self._clock.sleep(self.ESCALATION_WAIT.as_(Time.SECONDS))
    if not self._runner.is_alive():
      return

    # pass 2
    self._signaler.abortabortabort()
    self._clock.sleep(self.ESCALATION_WAIT.as_(Time.SECONDS))
    if not self._runner.is_alive():
      return

  def _wait_for_rebind(self):
    # TODO(wickman) MESOS-438
    #
    # There is a legit race condition here.  If we catch the is_alive latch
    # down at exactly the right time, there are no proper waits to wait for
    # rebinding to the executor by a third party killing process.
    #
    # While kills in production should be rare, we should monitor the
    # task_state for 60 seconds until it is in a terminal state. If it never
    # reaches a terminal state, then we could either:
    #    1) issue the kills ourself and send a LOST message
    # or 2) send a rebind_task call to the executor and have it attempt to
    #       re-take control of the executor.  perhaps with a max bind
    #       limit before going with route #1.
    wait_limit = self.WAIT_LIMIT
    while wait_limit > Amount(0, Time.SECONDS):
      current_state = self._runner.task_state()
      log.info('Waiting for terminal state, current state: %s' %
        TaskState._VALUES_TO_NAMES.get(current_state, '(unknown)'))
      if ThermosExecutorBase.thermos_status_is_terminal(current_state):
        log.info('Terminal reached, breaking')
        break
      self._clock.sleep(self.POLL_WAIT.as_(Time.SECONDS))
      wait_limit -= self.POLL_WAIT

  def terminate(self, force_status=None, force_message=None):
    self._wait_for_rebind()

    last_state = self._runner.task_state()
    log.info("State we've accepted: %s [force_status=%s, force_message=%s]" % (
        TaskState._VALUES_TO_NAMES.get(last_state, '(unknown)'),
        TaskState._VALUES_TO_NAMES.get(force_status),
        force_message))
    finish_state = None
    if last_state == TaskState.ACTIVE:
      log.error("Runner is dead but task state unexpectedly ACTIVE!")
      self._runner.quitquitquit()
      finish_state = mesos_pb.TASK_LOST
    elif last_state == TaskState.SUCCESS:
      finish_state = mesos_pb.TASK_FINISHED
    elif last_state == TaskState.FAILED:
      finish_state = mesos_pb.TASK_FAILED
    elif last_state == TaskState.KILLED:
      finish_state = mesos_pb.TASK_KILLED
    else:
      log.error("Unknown task state! %s" % TaskState._VALUES_TO_NAMES.get(last_state, '(unknown)'))
      finish_state = mesos_pb.TASK_FAILED

    update = mesos_pb.TaskStatus()
    update.task_id.value = self._task_id
    update.state = force_status if force_status is not None else finish_state
    if force_message:
      update.message = force_message
    log.info('Sending terminal state update: %s' % update.state)
    self._driver.sendStatusUpdate(update)

    # the executor is ephemeral and we just submitted a terminal task state, so shutdown
    log.info('Stopping executor.')
    self._runner.cleanup()
    self._driver.stop()
