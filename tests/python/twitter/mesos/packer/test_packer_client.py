import hashlib
import pytest
import tempfile
import mox
import urllib2

from StringIO import StringIO

import twitter.mesos.packer.packer_client as packer_client

def test_compute_checksum():
  text = '''O say can you see by the dawn's early light'''
  md5 = hashlib.md5()
  md5.update(text)
  digest = md5.hexdigest()
  temp_file = tempfile.NamedTemporaryFile(mode='w')
  temp_file.write(text)
  temp_file.flush()
  assert digest == packer_client.Packer('foo', 1234).compute_checksum(temp_file.name)

def prepare_add(mocker):
  packer = packer_client.Packer('host', 'port')
  role = 'mickey'
  package = 'mouse'
  local_file = '/the/club/house'
  metadata = {'md5sum': '0x42'}
  return (packer, role, package, local_file, metadata)

def test_add_with_checksum():
  mocker = mox.Mox()
  (packer, role, package, local_file, metadata) = prepare_add(mocker)
  digest = metadata['md5sum']
  mocker.StubOutWithMock(packer_client.Packer, 'compute_checksum')
  packer_client.Packer.compute_checksum(local_file).AndReturn(digest)
  mocker.StubOutWithMock(packer, '_add')
  packer._add(role, package, local_file, metadata, digest)
  mocker.ReplayAll()
  packer.add(role, package, local_file, metadata)
  mocker.VerifyAll()

def test_add_without_metadata():
  mocker = mox.Mox()
  (packer, role, package, local_file, metadata) = prepare_add(mocker)
  mocker.StubOutWithMock(packer_client.Packer, 'compute_checksum')
  packer_client.Packer.compute_checksum(local_file).AndReturn(metadata['md5sum'])
  mocker.StubOutWithMock(packer, '_add')
  packer._add(role, package, local_file, None, metadata['md5sum'])
  mocker.ReplayAll()
  packer.add(role, package, local_file, metadata=None)
  mocker.VerifyAll()

def test_add_without_checksum():
  mocker = mox.Mox()
  (packer, role, package, local_file, metadata) = prepare_add(mocker)
  mocker.StubOutWithMock(packer_client.Packer, 'compute_checksum')
  packer_client.Packer.compute_checksum(local_file).AndReturn(metadata['md5sum'])
  digest = metadata['md5sum']
  metadata.pop('md5sum')
  mocker.StubOutWithMock(packer, '_add')
  packer._add(role, package, local_file, metadata, digest)
  mocker.ReplayAll()
  packer.add(role, package, local_file, metadata)
  mocker.VerifyAll()


def verify_add_failure_message_processing(code, message, as_json=False, as_reason=False):
  mocker = mox.Mox()

  response = '{"message":"%s"}' % message if as_json else message
  reason = message if as_reason else None

  temp_file = tempfile.NamedTemporaryFile(mode='w')
  temp_file.write("some data")
  temp_file.flush()

  mocker.StubOutWithMock(packer_client.Packer, 'compose_url')
  packer_client.Packer.compose_url(mox.IgnoreArg(), auth=True).AndReturn('/data/etc.')

  mocker.StubOutWithMock(urllib2, 'build_opener')
  mock_opener = mocker.CreateMock(urllib2.OpenerDirector)
  urllib2.build_opener(mox.IgnoreArg()).AndReturn(mock_opener)

  mock_opener.open(mox.IgnoreArg(), mox.IgnoreArg(), mox.IgnoreArg()).AndRaise(urllib2.HTTPError
    ('http://example.com', code, reason, {}, StringIO(response)))

  mocker.ReplayAll()

  packer = packer_client.Packer('host', 'port')
  try:
    with pytest.raises(packer_client.Packer.Error) as cm:
      packer.add("some-role", "some-package", temp_file.name, "some metadata")

    assert cm.value.message == 'HTTP %d: %s' % (code, message)
    mocker.VerifyAll()
  finally:
    mocker.UnsetStubs()


def test_add_failure_message_parsing_json():
  # Generates a response with a JSON entity from which the message should be parsed
  verify_add_failure_message_processing(413, 'some message', as_json=True)


def test_add_failure_message_parsing_no_json():
  # Generates a response with a plain text entity which should be used as the message
  verify_add_failure_message_processing(413, 'some message')


def test_add_failure_message_parsing_no_entity():
  # Generates a response with no entity and a reason code which should be used as the message
  verify_add_failure_message_processing(413, 'some message', as_reason=True)
