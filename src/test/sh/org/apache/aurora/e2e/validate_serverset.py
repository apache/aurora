import os
import posixpath
import sys
import time

from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError

OK = 1
DID_NOT_REGISTER = 2
DID_NOT_RECOVER_FROM_EXPIRY = 3


serverset = os.getenv('SERVERSET')
client = KazooClient('localhost:2181')
client.start()


def wait_until_znodes(count, timeout=30):
  now = time.time()
  timeout += now
  while now < timeout:
    try:
      children = client.get_children(serverset)
    except NoNodeError:
      children = []
    print('Announced members: %s' % children)
    if len(children) == count:
      return [posixpath.join(serverset, child) for child in children]
    time.sleep(1)
    now += 1
  return []


# job is created with 3 znodes.
znodes = wait_until_znodes(3, timeout=10)
if not znodes:
  sys.exit(DID_NOT_REGISTER)

client.delete(znodes[0])

znodes = wait_until_znodes(3, timeout=10)
if not znodes:
  sys.exit(DID_NOT_RECOVER_FROM_EXPIRY)

sys.exit(OK)
