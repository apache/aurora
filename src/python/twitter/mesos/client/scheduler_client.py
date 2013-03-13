import functools
import getpass
import inspect
import threading
import time

from twitter.common import log
from twitter.common.net.tunnel import TunnelHelper
from twitter.common.quantity import Amount, Time
from twitter.common.zookeeper.serverset import ServerSet
from twitter.common_internal.auth import SSHAgentAuthenticator
from twitter.common_internal.location import Location
from twitter.common_internal.zookeeper.tunneler import TunneledZookeeper
from twitter.mesos.clusters import Cluster

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TSSLSocket, TTransport

from gen.twitter.mesos import MesosAdmin
from gen.twitter.mesos.ttypes import SessionKey


class SchedulerClient(object):
  THRIFT_RETRIES = 5
  RETRY_TIMEOUT = Amount(1, Time.SECONDS)

  class CouldNotConnect(Exception): pass

  @staticmethod
  def get(cluster, **kwargs):
    log.debug('Client location: %s' % ('prod' if Location.is_prod() else 'corp'))

    # TODO(vinod) : Clear up the convention of what --cluster <arg> means
    # Currently arg can be any one of
    # 'localhost:<scheduler port>'
    # '<cluster name>'
    # '<cluster name>: <zk port>'
    # Instead of encoding zk port inside the <arg> string make it explicit.
    if cluster.startswith('localhost:'):
      log.info('Attempting to talk to local scheduler.')
      port = int(cluster.split(':')[1])
      return LocalSchedulerClient(port, ssl=True)
    else:
      zk_port = 2181 if ':' not in cluster else int(cluster.split(':')[1])
      return ZookeeperSchedulerClient(cluster, zk_port, ssl=True, **kwargs)

  def __init__(self, verbose=False, ssl=False):
    self._client = None
    self._verbose = verbose
    self._ssl = ssl

  def get_thrift_client(self):
    if self._client is None:
      self._client = self._connect()
    return self._client

  # per-class implementation -- mostly meant to set up a valid host/port
  # pair and then delegate the opening to SchedulerClient._connect_scheduler
  def _connect(self):
    return None

  @staticmethod
  def _connect_scheduler(host, port, with_ssl=False):
    if with_ssl:
      socket = TSSLSocket.TSSLSocket(host, port, validate=False)
    else:
      socket = TSocket.TSocket(host, port)
    transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    schedulerClient = MesosAdmin.Client(protocol)
    for _ in range(SchedulerClient.THRIFT_RETRIES):
      try:
        transport.open()
        return schedulerClient
      except TTransport.TTransportException:
        time.sleep(SchedulerClient.RETRY_TIMEOUT.as_(Time.SECONDS))
        continue
    raise SchedulerClient.CouldNotConnect('Could not connect to %s:%s' % (host, port))


class ZookeeperSchedulerClient(SchedulerClient):
  SCHEDULER_ZK_PATH = '/twitter/service/mesos-scheduler'
  SERVERSET_TIMEOUT = Amount(10, Time.SECONDS)

  @classmethod 
  def get_scheduler_serverset(cls, cluster, port=2181, verbose=False, **kw):
    zk = TunneledZookeeper.get(cluster.zk, port=port, verbose=verbose)
    return zk, ServerSet(zk, cluster.scheduler_zk_path)

  def __init__(self, cluster, port=2181, ssl=False, verbose=False):
    SchedulerClient.__init__(self, verbose=verbose, ssl=ssl)
    self._cluster = cluster
    self._zkport = port
    self._endpoint = None

  def _connect(self):
    joined = threading.Event()
    def on_join(elements):
      joined.set()
    zk, serverset = self.get_scheduler_serverset(Cluster.get(self._cluster),
        port=self._zkport, verbose=self._verbose, on_join=on_join)
    joined.wait(timeout=self.SERVERSET_TIMEOUT.as_(Time.SECONDS))
    serverset_endpoints = list(serverset)
    if len(serverset_endpoints) == 0:
      zk.close()
      raise self.CouldNotConnect('No schedulers detected in %s!' % self._cluster)
    instance = serverset_endpoints[0]
    self._endpoint = instance.service_endpoint
    self._http = instance.additional_endpoints.get('http')
    host, port = self._maybe_tunnel(self._cluster, self._endpoint.host, self._endpoint.port)
    return self._connect_scheduler(host, port, self._ssl)

  @classmethod
  def _maybe_tunnel(cls, cluster, host, port):
    # Open a tunnel to the scheduler if necessary
    if Location.is_corp() and not Cluster.get(cluster).force_notunnel:
      log.info('Creating ssh tunnel for %s' % cluster)
      return TunnelHelper.create_tunnel(host, port)
    return host, port

  @property
  def url(self):
    proxy_url = Cluster.get(self._cluster).proxy_url
    if proxy_url:
      return proxy_url
    if self._http:
      return 'http://%s:%s' % (self._http.host, self._http.port)


class LocalSchedulerClient(SchedulerClient):
  def __init__(self, port, ssl=False):
    SchedulerClient.__init__(self, verbose=True, ssl=ssl)
    self._host = 'localhost'
    self._port = port

  def _connect(self):
    return self._connect_scheduler(self._host, self._port, with_ssl=self._ssl)

  @property
  def url(self):
    return 'http://localhost:8081'


class SchedulerProxy(object):
  """
    This class is responsible for creating a reliable thrift client to the
    twitter scheduler.  Basically all the dirty work needed by the
    MesosClientAPI.
  """
  CONNECT_MAXIMUM_WAIT = Amount(1, Time.MINUTES)
  RPC_RETRY_INTERVAL = Amount(5, Time.SECONDS)
  RPC_MAXIMUM_WAIT = Amount(10, Time.MINUTES)

  class TimeoutError(Exception): pass

  @staticmethod
  def assert_valid_cluster(cluster):
    assert cluster, "Cluster not specified!"
    if cluster.find(':') > -1:
      scluster = cluster.split(':')

      if scluster[0] != 'localhost':
        Cluster.assert_exists(scluster[0])

      if len(scluster) == 2:
        try:
          int(scluster[1])
        except ValueError as e:
          log.fatal('The cluster argument is invalid: %s (error: %s)' % (cluster, e))
          assert False, 'Invalid cluster argument: %s' % cluster
    else:
      Cluster.assert_exists(cluster)
  
  @classmethod
  def create_session(cls, user):
    try:
      nonce, nonce_sig = SSHAgentAuthenticator.create_session(user)
      return SessionKey(user=user, nonce=nonce, nonceSig=nonce_sig)
    except SSHAgentAuthenticator.AuthorizationError as e:
      log.warning('Could not authenticate: %s' % e)
      log.warning('Attempting unauthenticated communication.')
      return SessionKey(user=user, nonce=SSHAgentAuthenticator.get_timestamp(),
          nonceSig='UNAUTHENTICATED')

  def __init__(self, cluster, verbose=False):
    self.cluster = cluster
    self._session_key = self._client = self._scheduler = None
    self.verbose = verbose
    self.assert_valid_cluster(cluster)

  def with_scheduler(method):
    """Decorator magic to make sure a connection is made to the scheduler"""
    def _wrapper(self, *args, **kwargs):
      if not self._scheduler:
        self._construct_scheduler()
      return method(self, *args, **kwargs)
    return _wrapper

  def invalidate(self):
    self._session_key = self._client = self._scheduler = None

  def requires_auth(method):
    def _wrapper(self, *args, **kwargs):
      if not self._session_key:
        self._session_key = self.create_session(getpass.getuser())
      return method(self, *args, **kwargs)
    return _wrapper

  @with_scheduler
  def client(self):
    return self._client

  @requires_auth
  def session_key(self):
    return self._session_key

  @with_scheduler
  def scheduler(self):
    return self._scheduler

  def _construct_scheduler(self):
    """
      Populates:
        self._scheduler
        self._client
    """
    self._scheduler = SchedulerClient.get(self.cluster, verbose=self.verbose)
    assert self._scheduler, "Could not find scheduler (cluster = %s)" % self.cluster
    start = time.time()
    while (time.time() - start) < self.CONNECT_MAXIMUM_WAIT.as_(Time.SECONDS):
      try:
        self._client = self._scheduler.get_thrift_client()
        break
      except SchedulerClient.CouldNotConnect as e:
        log.warning('Could not connect to scheduler: %s' % e)
    if not self._client:
      raise self.TimeoutError('Timed out trying to connect to scheduler at %s' % self.cluster)

  def __getattr__(self, method_name):
    # If the method does not exist, getattr will return AttributeError for us.
    method = getattr(MesosAdmin.Client, method_name)
    if not callable(method):
      return method

    @functools.wraps(method)
    def method_wrapper(*args):
      start = time.time()
      while (time.time() - start) < self.RPC_MAXIMUM_WAIT.as_(Time.SECONDS):
        try:
          method = getattr(self.client(), method_name)
          if not callable(method):
            return method

          # Inject a session key for the 'session' argument if none was provided
          # TODO(ksweeney): Do Thrift type introspection instead of relying on parameter name.
          method_signature = inspect.getargspec(method).args
          if 'session' in method_signature and len(args) < len(method_signature):
            session_position = method_signature.index('session')
            assert session_position >= 0
            args = args[:session_position] + (self.session_key(),) + args[session_position:]
          return method(*args)
        except (TTransport.TTransportException, self.TimeoutError) as e:
          log.warning('Connection error with scheduler: %s, reconnecting...' % e)
          self.invalidate()
          time.sleep(self.RPC_RETRY_INTERVAL.as_(Time.SECONDS))
      raise self.TimeoutError('Timed out attempting to issue %s to %s' % (
          method_name, self.cluster))

    return method_wrapper
