import getpass
import time

from gen.twitter.mesos.ttypes import SessionKey


class DefaultAuthModule(object):
  def __init__(self):
    self._user = getpass.getuser()

  def __call__(self):
    timestamp_in_ms = int(time.time() * 1000)
    return SessionKey(
      user=self._user,
      nonce=timestamp_in_ms,
      nonceSig='UNAUTHENTICATED')
