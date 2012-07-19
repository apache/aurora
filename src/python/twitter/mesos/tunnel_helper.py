import atexit
import errno
import os
import signal
import socket
import subprocess


try:
  from twitter.common import app
  HAS_APP=True

  app.add_option(
    '--tunnel_host',
    type='string',
    dest='tunnel_host',
    default='nest1.corp.twitter.com',
    help='Host to tunnel commands through (default: %default)')

except ImportError:
  HAS_APP=False


class TunnelHelper(object):
  TUNNELS = {}

  @staticmethod
  def get_random_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('localhost', 0))
    _, port = s.getsockname()
    s.close()
    return port

  @staticmethod
  def create_tunnel(remote_host, remote_port, tunnel_host=None, tunnel_port=None):
    """ Create a tunnel from the localport to the remote host & port,
    using sshd_host as the tunneling server.
    """
    tunnel_key = (remote_host, remote_port)
    if tunnel_key in TunnelHelper.TUNNELS:
      return 'localhost', TunnelHelper.TUNNELS[tunnel_key][0]

    if HAS_APP:
      tunnel_host = tunnel_host or app.get_options().tunnel_host
    assert tunnel_host is not None, 'Must specify tunnel host!'
    tunnel_port = tunnel_port or TunnelHelper.get_random_port()

    ssh_cmd_args = ('ssh', '-T', '-L',
                    '%d:%s:%s' % (tunnel_port,
                                  remote_host,
                                  remote_port),
                    tunnel_host)

    TunnelHelper.TUNNELS[tunnel_key] = (tunnel_port,
      subprocess.Popen(ssh_cmd_args, stdin=subprocess.PIPE))
    return 'localhost', tunnel_port


@atexit.register
def cleanup_tunnels():
  for _, po in TunnelHelper.TUNNELS.values():
    try:
      po.kill()
    except OSError as e:
      if e.errno != errno.ESRCH:
        raise
    po.wait()
