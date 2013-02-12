import socket
import threading
import time
import tornado.ioloop
import tornado.web
import unittest

import mesos_pb2 as mesos_pb

from twitter.common.quantity import Amount, Time
from twitter.common.testing.clock import ThreadedClock
from twitter.mesos.executor.http_signaler import HttpSignaler
from twitter.mesos.executor.status_manager import StatusManager
from twitter.mesos.executor.health_checker import HealthCheckerThread

from gen.twitter.thermos.ttypes import TaskState


def thread_yield():
  time.sleep(0.1)


def get_random_port():
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.bind(('localhost', 0))
  _, port = s.getsockname()
  s.close()
  return port


class HealthStatus(object):
  def __init__(self):
    self.value = 'ok'


class Requests(object):
  def __init__(self):
    self.q = []
    self.qqq = threading.Event()
    self.aaa = threading.Event()

  def add(self, value):
    self.q.append(value)
    if value == '/quitquitquit':
      self.qqq.set()
    elif value == '/abortabortabort':
      self.aaa.set()

  def clear(self):
    self.q = []


class MainHandler(tornado.web.RequestHandler):
  def initialize(self, health, requests, event):
    self.health = health
    self.requests = requests
    self.event = event

  def _record(self):
    self.requests.add((self.request.method, self.request.uri))
    self.write(self.health.value)
    self.event.set()

  def get(self):
    self._record()

  def post(self):
    self._record()


class HttpThread(threading.Thread):
  # TODO(jon): consolidate with SignalServer in test_http_signaler?
  def __init__(self):
    self.health = HealthStatus()
    self.requests = Requests()
    self.event = threading.Event()
    self.port = get_random_port()
    self.io_loop = tornado.ioloop.IOLoop()
    threading.Thread.__init__(self)
    self.daemon = True

  def run(self):
    application = tornado.web.Application([
      (r"/.*", MainHandler, dict(requests=self.requests, health=self.health, event=self.event))
    ])
    application.listen(self.port, io_loop=self.io_loop)
    self.io_loop.start()

  def stop(self):
    self.io_loop.stop()


class TestSignaler(unittest.TestCase):
  def setUp(self):
    self.http = HttpThread()
    self.http.start()
    thread_yield()

  def tearDown(self):
    self.http.stop()

  def test_basic_ok(self):
    signaler = HttpSignaler(self.http.port)
    assert signaler.health()[0]
    assert self.http.requests.q == [('GET', '/health')]
    assert signaler.quitquitquit()[0]
    assert signaler.abortabortabort()[0]
    assert self.http.requests.q == [('GET', '/health'),
                                    ('POST', '/quitquitquit'),
                                    ('POST', '/abortabortabort')]

    self.http.requests.clear()
    self.http.health.value = 'bad'
    assert not signaler.health()[0]
    assert self.http.requests.q == [('GET', '/health')]

  def test_faulty_server(self):
    signaler = HttpSignaler(self.http.port)
    assert signaler.health()[0]
    self.http.stop()
    thread_yield()
    assert not signaler.health()[0]


class TestHealthChecker(unittest.TestCase):
  def setUp(self):
    self.http = HttpThread()
    self.http.start()
    thread_yield()

  def tearDown(self):
    self.http.stop()

  def test_initial_interval_2x(self):
    clock = ThreadedClock()
    hct = HealthCheckerThread(lambda: (False, 'Bad health check'), interval_secs=5, clock=clock)
    hct.start()
    thread_yield()
    assert hct.healthy
    clock.tick(6)
    assert hct.healthy
    clock.tick(3)
    assert hct.healthy
    clock.tick(5)
    thread_yield()
    assert not hct.healthy
    hct.stop()

  def test_initial_interval_whatev(self):
    clock = ThreadedClock()
    hct = HealthCheckerThread(
      lambda: (False, 'Bad health check'), interval_secs=5, initial_interval_secs=0, clock=clock)
    hct.start()
    assert not hct.healthy
    hct.stop()


class MockDriver(object):
  def __init__(self):
    self.updates = []
    self.stop_event = threading.Event()

  def sendStatusUpdate(self, update):
    self.updates.append(update)

  def stop(self):
    self.stop_event.set()


class MockRunner(object):
  def __init__(self):
    self._task_state = TaskState.ACTIVE
    self._quit_state = TaskState.FAILED
    self._is_alive = True
    self.cleaned = False
    self.qqq = threading.Event()
    self.killed = threading.Event()

  def kill(self):
    self.killed.set()

  def cleanup(self):
    self.cleaned = True

  def set_dead(self):
    self._is_alive = False

  def set_task_state(self, state):
    self._task_state = state

  def task_state(self):
    return self._task_state

  def quitquitquit(self):
    self._task_state = self._quit_state
    self.qqq.set()

  def is_alive(self):
    return self._is_alive


class TestStatusManager(unittest.TestCase):
  def setUp(self):
    self.http = HttpThread()
    self.http.start()
    self.runner = MockRunner()
    self.driver = MockDriver()
    self.clock = ThreadedClock()
    thread_yield()

  def tearDown(self):
    self.http.stop()

  def test_without_http(self):
    class FastStatusManager(StatusManager):
      PERSISTENCE_WAIT = Amount(0, Time.SECONDS)
    monitor = FastStatusManager(self.runner, self.driver, 'abc', clock=time)
    monitor.start()
    self.runner.set_dead()
    time.sleep(FastStatusManager.POLL_WAIT.as_(Time.SECONDS))
    assert not self.driver.stop_event.is_set()
    self.runner.set_task_state(TaskState.FAILED)
    self.driver.stop_event.wait(timeout=1.0)
    assert self.driver.stop_event.is_set()
    assert len(self.driver.updates) == 1
    assert self.driver.updates[0].state == mesos_pb.TASK_FAILED
    assert self.runner.cleaned

  def test_task_goes_lost(self):
    class FastStatusManager(StatusManager):
      WAIT_LIMIT = Amount(1, Time.SECONDS)
      ESCALATION_WAIT = Amount(1, Time.SECONDS)
    monitor = FastStatusManager(self.runner, self.driver, 'abc')
    monitor.start()
    self.runner.set_dead()
    self.runner.qqq.wait(timeout=2.0)
    self.clock.tick(0.1 + StatusManager.PERSISTENCE_WAIT.as_(Time.SECONDS))
    self.driver.stop_event.wait(timeout=10.0)
    assert self.runner.qqq.is_set()
    assert self.driver.stop_event.is_set()
    assert len(self.driver.updates) == 1
    assert self.driver.updates[0].state == mesos_pb.TASK_LOST
    assert self.runner.cleaned

  def test_unhealthy_task_overridden(self):
    class FastStatusManager(StatusManager):
      ESCALATION_WAIT = Amount(500, Time.MILLISECONDS)
    signaler = HttpSignaler(self.http.port)
    checker = HealthCheckerThread(signaler.health, interval_secs=0.5)
    monitor = FastStatusManager(self.runner, self.driver, 'abc', signaler=signaler,
        health_checkers=[checker])
    monitor.start()

    self.http.health.value = 'bad'
    assert len(self.driver.updates) == 0
    monitor.unhealthy_event.wait(timeout=2.0)
    assert monitor.unhealthy_event.is_set()

    self.http.requests.qqq.wait(timeout=1.0)
    self.http.requests.aaa.wait(timeout=1.0)

    assert not self.driver.stop_event.is_set()
    self.runner.set_dead()
    self.runner.set_task_state(TaskState.KILLED)
    self.clock.tick(0.1 + StatusManager.PERSISTENCE_WAIT.as_(Time.SECONDS))
    self.driver.stop_event.wait(timeout=10.0)
    assert self.driver.stop_event.is_set()
    assert len(self.driver.updates) == 1
    assert self.driver.updates[0].state == mesos_pb.TASK_FAILED
    assert self.driver.updates[0].message.startswith('Failed health check!')
    assert self.runner.cleaned
