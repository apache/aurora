import socket
import re
import random
import time
import errno

import twitter.common.log
log = twitter.common.log.get()

class EntityParser:
  PORT_RE = r'%port:(\w+)%'

  @staticmethod
  def match_ports(str):
    matcher = re.compile(EntityParser.PORT_RE)
    matched = matcher.findall(str)
    return matched

class EphemeralPortAllocator_CannotAllocate(Exception): pass
class EphemeralPortAllocator_RecoveryError(Exception): pass

class EphemeralPortAllocator:
  PORT_SPEC = "%%port:%s%%"
  SOCKET_RANGE = [32768, 65535]

  def __init__(self):
    self._ports = {}

  def allocate_port(self, name, port = None):
    # if a port is provided, it is presumed during recovery step
    if port is not None:
      if name in self._ports and self._ports[name] != port:
        raise EphemeralPortAllocator_RecoveryError(
          "Recovered port binding %s=>%s conflicts with current binding %s=>%s" % (
          name, port, name, self._ports[name]))
      else:
        self._ports[name] = port
        return port

    if name in self._ports: return self._ports[name]

    while True:
      rand_port = random.randint(*EphemeralPortAllocator.SOCKET_RANGE)
      if rand_port in self._ports.values(): continue
      try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('localhost', rand_port))
        s.close()
        self._ports[name] = rand_port
        break
      except OSError, e:
        if e.errno in (errno.EADDRINUSE, errno.EINVAL):
          log.error('Could not bind port: %s' % e)
          time.sleep(1)
          continue
        else:
          raise  # this is dangerous until we know all the possible failure scenarios
    return self._ports[name]

  def synthesize(self, cmdline):
    ports = EntityParser.match_ports(cmdline)
    newly_allocated_ports = {}
    for port in ports:
      if port not in self._ports:
        self.allocate_port(port)
        newly_allocated_ports[port] = self._ports[port]
      cmdline = cmdline.replace(EphemeralPortAllocator.PORT_SPEC % port, str(self._ports[port]))
    return (cmdline, newly_allocated_ports)
