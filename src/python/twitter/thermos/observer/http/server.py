import os
import copy
import bottle
import pkg_resources

class BottleServer(object):
  """
    Wrapper around bottle to make class-bound servers a little easier
    to write.
  """
  bottle = bottle
  Response = bottle.HTTPResponse

  @staticmethod
  def route(*args, **kwargs):
    def annotated(function):
      if not hasattr(function, '__routes__'):
        function.__routes__ = []
      function.__routes__.append((args, kwargs))
      return function
    return annotated

  @staticmethod
  def view(*args, **kwargs):
    def annotated(function):
      function.__view__ = (args, kwargs)
      return function
    return annotated

  @staticmethod
  def mako_view(*args, **kwargs):
    kwargs.update(template_adapter = bottle.MakoTemplate)
    return BottleServer.view(*args, **kwargs)

  def __init__(self):
    self._app = bottle.Bottle()
    self._request = bottle.request   # it's sort of upsetting that these are globals
    self._response = bottle.response # in bottle, but, c'est la vie.
    self._hostname = None
    self._port = None
    for attr in dir(self):
      if hasattr(self, attr) and hasattr(getattr(self, attr), '__view__'):
        args, kw = getattr(self,attr).__view__
        setattr(self, attr, bottle.view(*args, **kw)(getattr(self, attr)))
      if hasattr(self, attr) and hasattr(getattr(self, attr), '__routes__'):
        for rt in getattr(self,attr).__routes__:
          kw = copy.deepcopy(rt[1])
          kw.update({'callback': getattr(self, attr)})
          self._app.route(*rt[0], **kw)

  def app(self):
    return self._app

  def hostname(self):
    return self._hostname

  def port(self):
    return self._port

  def run(self, hostname, port):
    self._hostname = hostname
    self._port = port
    bottle.run(self._app, host=hostname, port=port)


class BottleTemplate(object):
  @staticmethod
  def load(name):
    return pkg_resources.resource_string(
        __name__, os.path.join('templates', '%s.tpl' % name))
