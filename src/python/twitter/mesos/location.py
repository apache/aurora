import re
import socket

class Location(object):
  """Determine which cluster the code is running in, or CORP if we are not in prod."""

  CORP = "corp"

  @staticmethod
  def get_location():
    hostname = socket.gethostname()
    prod_matcher = re.match('^(\w{3}\d+).*\.twitter\.com$', hostname)
    prod_host = prod_matcher.group(1) if prod_matcher else None
    if hostname.endswith('.local') or hostname.endswith('.office.twttr.net') or 'sfo0' == prod_host:
      return Location.CORP
    elif prod_host:
      return prod_host
    else:
      print 'Can\'t determine location (prod vs. corp). Hostname=%s' % hostname
      return None
