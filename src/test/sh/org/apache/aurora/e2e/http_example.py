#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import print_function

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from sys import argv
from threading import Thread


class RequestHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    self.send_response(200)
    self.send_header('Content-Type', 'text/plain')
    self.end_headers()
    self.wfile.write('Hello!\n')


class HealthHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    if self.path == '/health':
      self.send_response(200)
      self.send_header('Content-Type', 'text/plain')
      self.end_headers()
      self.wfile.write('ok')


def start_server(port, handler_class):
  server = HTTPServer(('', port), handler_class)
  print('Listening on port %s' % port)
  server.serve_forever()


request_thread = Thread(target=start_server, args=[int(argv[1]), RequestHandler])
health_thread = Thread(target=start_server, args=[int(argv[2]), HealthHandler])

for thread in [request_thread, health_thread]:
  thread.start()

for thread in [request_thread, health_thread]:
  thread.join()
