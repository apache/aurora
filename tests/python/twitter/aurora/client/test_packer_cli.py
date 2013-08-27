import unittest

from twitter.common import app

import twitter.aurora.bin.mesos_client as mesos_client
from twitter.packer.packer_client import Packer

import mox
import pytest


class TestPackerCLI(unittest.TestCase):
  def setUp(self):
    super(TestPackerCLI, self).setUp()
    self.mox = mox.Mox()

  def tearDown(self):
    self.mox.VerifyAll()
    self.mox.UnsetStubs()

  def test_packer_failure(self):
    """Tests packer failures.

    If a packer commmand gets invoked and fails, that failure should propagate
    up and cause the mesos client command to fail.

    Since all of the packer commands handle errors through a common wrapper,
    if this works for package_list, it should also work for all of the other
    commands that use trap_packer_error.
    """
    self.mox.StubOutWithMock(mesos_client, '_get_packer')
    self.mox.StubOutWithMock(app, 'get_options')
    moxOptions = self.mox.CreateMockAnything()
    app.get_options().AndReturn(moxOptions)
    moxOptions.cluster = 'smfd'
    moxOptions.verbosity = 'normal'

    mockPacker = self.mox.CreateMock(Packer)
    mockPacker.list_packages('mchucarroll').AndRaise(SystemExit("Request failed."))
    mesos_client._get_packer(mox.IgnoreArg(), mox.IgnoreArg()).AndReturn(mockPacker)
    self.mox.ReplayAll()
    with pytest.raises(SystemExit):
      mesos_client.package_list(['mchucarroll'])
