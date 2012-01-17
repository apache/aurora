import socket
import random
import time
import errno

from twitter.common import log

class PortAllocator(object):
  PORT_RANGE = [32768, 61000]

  class CannotAllocate(Exception): pass

  def __init__(self, prebound=None):
    """
      Construct a port allocator.

      Required:
        :ports => An iterable containing the list of all port names we need.

      Optional:
        :prebound => A mapping from name => port number for pre-specified port numbers
        :recovery => If in recovery mode, fail-soft if we fail to bind to a given port.
          If not in recovery mode, fail hard since the ports are already in use.
    """
    if prebound:
      assert isinstance(prebound, dict)
      self._ports = prebound
    else:
      self._ports = {}

  def allocate(self, name, port=None):
    """
      Given a port name, allocate the named port associated with it.  If
      a port number > 0 is supplied, bind to that port number.

      Returns a tuple of (allocated, port #) where allocated is True if the
      port is a newly allocated port.
    """
    allocated = False
    if isinstance(port, int) and port <= 0:
      raise ValueError('Port must be a positive integer or None.')
    if name not in self._ports:
      self._ports[name] = self._allocate_port() if port is None else port
      allocated = True
    if port:
      assert self._ports[name] == port
    return (allocated, self._ports[name])

  def _allocate_port(self):
    while True:
      rand_port = random.randint(*PortAllocator.PORT_RANGE)
      if rand_port in self._ports.values():
        continue
      try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('localhost', rand_port))
        s.close()
        return rand_port
      except OSError as e:
        if e.errno in (errno.EADDRINUSE, errno.EINVAL):
          log.error('Could not bind port: %s' % e)
          time.sleep(0.2)
          continue
        else:
          raise PortAllocator.CannotAllocate('Could not allocate a port due to %s' % e)
