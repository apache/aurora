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
from threading import Thread
from urlparse import urlparse, parse_qs

import json
import sys


class RequestHandler(BaseHTTPRequestHandler):
  def do_POST(self):
    task = parse_qs(urlparse(self.path).query)['task'][0]
    body = self.rfile.read(int(self.headers.getheader('content-length', 0)))

    # Only allow draining instance-0
    allow = True if task == 'devcluster/vagrant/test/coordinator/0' else False

    self.send_response(200)
    self.send_header('Content-Type', 'application/json')
    self.end_headers()
    self.wfile.write('{"drain":%s}' % allow)

    print('Request received for {}'.format(task))
    print(json.dumps(json.loads(body), indent=2, sort_keys=True))
    print('Responded: {}'.format(allow))
    sys.stdout.flush()


def start_server(instance, port, handler_class):
  # This service will act as its own SLA Coordinator.
  # We need a static port for the coordinator service so the scheduler can communicate to it when
  # checking SLA. We cannot have 2 tasks binding to the same port so instance-1 will bind to 8080
  # as instance-0 is the one that will be acked for draining. instance-0 will bind to a random port.
  if instance != 1:
    port = 0

  server = HTTPServer(('', port), handler_class)
  print('Listening on port %s' % port)
  sys.stdout.flush()
  server.serve_forever()


thread = Thread(target=start_server, args=[int(sys.argv[1]), 8080, RequestHandler])

thread.start()
thread.join()
