function makeClient() {
  const transport = new Thrift.Transport('/api');
  const protocol = new Thrift.Protocol(transport);
  return new ReadOnlySchedulerClient(protocol);
}

export default makeClient();
