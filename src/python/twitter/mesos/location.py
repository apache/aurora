import re
import socket

class Location(object):
  """Determine which cluster the code is running in, or CORP if we are not in prod."""

  PROD_SUFFIXES = [
    '.corpdc.twitter.com',
    '.prod.twitter.com',
  ]

  @staticmethod
  def is_corp():
    """
      Returns true if this job is in corp and requires an ssh tunnel for
      scheduler RPCs.
    """
    hostname = socket.gethostname()
    for suffix in Location.PROD_SUFFIXES:
      if hostname.endswith(suffix):
        return False
    return True

  @staticmethod
  def is_prod():
    return not Location.is_corp()
