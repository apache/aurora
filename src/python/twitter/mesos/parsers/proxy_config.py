from abc import ABCMeta, abstractmethod

class ProxyConfig(object):
  __metaclass__ = ABCMeta

  class InvalidConfig(Exception):
    pass

  @abstractmethod
  def job(self):
    """Return the JobConfiguration representation of this job."""
    pass

  @abstractmethod
  def name(self):
    pass

  @abstractmethod
  def hdfs_path(self):
    pass

  @abstractmethod
  def cluster(self):
    pass

  @abstractmethod
  def ports(self):
    pass

  @abstractmethod
  def role(self):
    pass
