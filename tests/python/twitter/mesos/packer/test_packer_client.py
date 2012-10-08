import hashlib
import pytest
import tempfile
import mox
import twitter.mesos.packer.packer_client as packer_client

def test_compute_checksum():
  text = '''O say can you see by the dawn's early light'''
  md5 = hashlib.md5()
  md5.update(text)
  digest = md5.hexdigest()
  temp_file = tempfile.NamedTemporaryFile(mode='w')
  temp_file.write(text)
  temp_file.flush()
  assert digest == packer_client.Packer.compute_checksum(temp_file.name)

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

