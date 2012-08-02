from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.transport import TSSLSocket
from gen.twitter.thrift.endpoint.ttypes import ServiceInstance

def decode_service_instance(data):
  transportIn = TTransport.TMemoryBuffer(data)
  protocolIn = TBinaryProtocol.TBinaryProtocol(transportIn)
  si = ServiceInstance()
  si.read(protocolIn)
  return si