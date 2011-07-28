import time

from mesos_twitter.ttypes import SessionKey

from paramiko import Agent, RSAKey, Message, SSHException
from StringIO import StringIO

class SessionKeyHelper(object):
  class AgentException(Exception): pass

  @staticmethod
  def get_timestamp():
    timestamp_in_ms = int(time.time() * 1000)
    return timestamp_in_ms

  @staticmethod
  def sign_session(session_key):
    try:
      agent = Agent()
    except SSHException, e:
      raise SessionKeyHelper.AgentException("Could not talk to SSH agent: %s" % e)

    ts = SessionKeyHelper.get_timestamp()
    message = str(ts)

    for key in agent.get_keys():
      signed_glob = key.sign_ssh_data(None, message)
      session_key.nonce = ts
      session_key.nonceSig = signed_glob
      return

    raise SessionKeyHelper.AgentException("Could not talk to SSH agent: no keys found.")
