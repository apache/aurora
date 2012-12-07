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
  def set_hdfs_path(self):
    pass

  @abstractmethod
  def cluster(self):
    pass

  @abstractmethod
  def ports(self):
    pass

  @abstractmethod
  def task_links(self):
    pass

  @abstractmethod
  def role(self):
    pass

  @abstractmethod
  def package(self):
    """Return 3-tuple of (role, package_name, version) or None if no package specified."""
    pass

  @abstractmethod
  def add_package(self, package):
    """Add a 3-tuple of (role, package_name, version)"""
    pass

  @abstractmethod
  def package_files(self):
    """Returns a list of package file paths"""
    pass

  @abstractmethod
  def is_dedicated(self):
    """Returns True if this is a configuration for a dedicated job."""
    pass
