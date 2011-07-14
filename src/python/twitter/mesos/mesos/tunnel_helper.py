import errno
import os
import signal
import subprocess

import clusters

from twitter.common import log

class TunnelHelper:
  SSH_TUNNEL_LIFETIME_SECS = 60

  @staticmethod
  def get_tunnel_host(cluster):
    return 'nest1.%s.twitter.com' % clusters.get_dc(cluster)

  @staticmethod
  def create_tunnel(tunnel_host, tunnel_port, remote_host, remote_port):
    """ Create a tunnel from the localport to the remote host & port,
    using sshd_host as the tunneling server.
    """
    # These flags run ssh in the background, where the check_call() function
    # returns after initiating the connection. Running the command "sleep X"
    # causes the process to quit after X seconds, as a safety against orphaned
    # ssh processes.
    TunnelHelper.free_tunnel_ports([tunnel_port, remote_port])

    ssh_cmd_args = ('ssh', '-fnT', '-L',
                    '%d:%s:%s' % (tunnel_port,
                                  remote_host,
                                  remote_port),
                    tunnel_host,
                    'sleep %d' % TunnelHelper.SSH_TUNNEL_LIFETIME_SECS)
    subprocess.check_call(ssh_cmd_args)
    return ('localhost', tunnel_port)

  @staticmethod
  def free_tunnel_ports(ports):
    log.debug('Killing processes that are listening on ports: %s' % repr(ports))
    for port in ports:
      TunnelHelper.maybe_kill_jobs_on_port(port)

  @staticmethod
  def maybe_kill_jobs_on_port(port):
    """ Kill processes listening on the given ports """

    result = subprocess.Popen(['lsof', '-i:%d' % port, '-t'],
                              stdout=subprocess.PIPE).communicate()[0]
    pids_to_kill = [int(x) for x in result.split()]

    mypid = os.getpid()
    for pid in pids_to_kill:
      if pid == mypid: continue
      log.debug('Killing pid %s' % pid)
      try:
        os.kill(pid, signal.SIGKILL)
      except OSError, e:
        if e.errno != errno.ESRCH: # no such process
          raise
