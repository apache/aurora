import re
import socket

class Location(object):
  " Determine which cluster the code is running in, or CORP if we are not in prod. "

  CORP = "corp"

  @staticmethod
  def get_location():
    hostname = socket.gethostname()
    prod_matcher = re.match('^(\w{3}).*.twitter\.com$', hostname)

    if re.search('.+\.local$', hostname):
      return Location.CORP
    elif prod_matcher is not None:
      return prod_matcher.group(1)
    else:
      print 'Can\'t determine location (prod vs. corp). Hostname=%s' % hostname
      return None
