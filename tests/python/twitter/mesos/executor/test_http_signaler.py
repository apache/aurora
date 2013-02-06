from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import threading
import time

from twitter.common.exceptions import ExceptionalThread
from twitter.mesos.executor.http_signaler import HttpSignaler

class FastHttpSignaler(HttpSignaler):
  TIMEOUT_SECS = 0.1

class HealthyHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    self.send_response(200)
    self.end_headers()
    self.wfile.write('ok' if self.path == '/health' else '')

class UnhealthyHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    self.send_response(200)
    self.end_headers()
    self.wfile.write('not ok' if self.path == '/health' else '')

class BadHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    self.send_response(31337)

class SlowHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    time.sleep(FastHttpSignaler.TIMEOUT_SECS * 1.1)

class SignalServer(ExceptionalThread):
  def __init__(self, handler):
    self._server = HTTPServer(('', 0), handler)
    super(SignalServer, self).__init__()
    self.daemon = True
    self._stop = threading.Event()
  def run(self):
    while not self._stop.is_set():
      self._server.handle_request()
  def __enter__(self):
    self.start()
    return self._server.server_port
  def __exit__(self, exc_type, exc_val, traceback):
    self._stop.set()

def test_health():
  with SignalServer(HealthyHandler) as port:
    assert HttpSignaler(port).health()
  with SignalServer(UnhealthyHandler) as port:
    assert not HttpSignaler(port).health()
  with SignalServer(BadHandler) as port:
    assert not HttpSignaler(port).health()
  with SignalServer(SlowHandler) as port:
    assert not FastHttpSignaler(port).health()
