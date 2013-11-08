import os
import pkg_resources


class HttpTemplate(object):
  @staticmethod
  def load(name):
    return pkg_resources.resource_string(
        __name__, os.path.join('templates', '%s.tpl' % name))
