import socket
import os
import re
import posixpath
import mimetypes
import pkg_resources
from wsgiref.simple_server import make_server
from urlparse import parse_qs

from twitter.common import log

from observer import TaskObserver

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

class ObserverRequestTranslator:
  def __init__(self, observer):
    self._observer = observer

  def _args_error(self, d, e):
    log.error("Failed to parse arguments %s: %s" % (repr(d), e))

  def __call__(self, method, request):
    internal_method_name = 'method_%s' % method
    if internal_method_name in ObserverRequestTranslator.__dict__:
      return ObserverRequestTranslator.__dict__[internal_method_name](self, request)
    else:
      # 404 handling
      log.error("Unknown method: %s" % method)
      return None

  def method_uids(self, d):
    num = d.get('num', [None])[0]
    if num: num = int(num)
    return ('json', self._observer.uids(type   = d.get('type', ['all'])[0],
                                        offset = int(d.get('offset', [0])[0]),
                                        num    = num))

  def method_uid_count(self, d):
    return ('json', self._observer.uid_count(type = d.get('type', ['all'])[0]))

  def method_task(self, d):
    try:
      if 'uid' not in d:
        return ('json', '{}')
      uids = [int(uid) for uid in d['uid'][0].split(',')]
    except Exception as e:
      self._args_error(d, e)
      return ('json', '{}')
    return ('json', self._observer.task(uids))

  def method_process(self, d):
    run = d.get('run', [None])[0]
    if run: run = int(run)
    return ('json',
            self._observer.process(
              uid = int(d.get('uid', [-1])[0]),
              process = d.get('process', [None])[0],
              run = run))

  def method_processes(self, d):
    try:
      if 'uid' not in d:
        return ('json', '{}')
      uids = [int(uid) for uid in d['uid'][0].split(',')]
    except Exception as e:
      self._args_error(d, e)
      return ('json', '{}')
    return ('json', self._observer.processes(uids))

  # TODO(wickman): Make some boilerplate wsgi extractors to avoid the [0] bullshit.
  def method_logs(self, d):
    try:
      uid = int(d['uid'][0])
      process = d['process'][0]
      run = int(d['run'][0])
      path = d.get('path', [None])[0]
      fmt = d.get('fmt', ['json'])[0]
    except Exception as e:
      self._args_error(d, e)
      return ('json', '{}')
    return (fmt, self._observer.logs(uid, process, run, path=path, fmt=fmt))

  # TODO(wickman): logs/file/download all share uid/process/run
  #   extraction.  abstract this out somehow.
  def method_file(self, d):
    try:
      uid = int(d['uid'][0])
      process = d['process'][0]
      run = int(d['run'][0])
      filename = d.get('filename', [None])[0]
      fmt = d.get('fmt', ['json'])[0]
      offset = d.get('offset', [None])[0]
      if offset: offset = int(offset)
      bytes = d.get('bytes', [None])[0]
      if bytes: bytes = int(bytes)
    except Exception as e:
      self._args_error(d, e)
      return ('json', '{}')
    return (fmt,
      self._observer.browse_file(
        uid, process, run, filename, offset=offset, bytes=bytes, fmt=fmt))

class ObserverHttpHandler:
  INDEX_HTML = """
  <html>
  <title>thermos(%(hostname)s)</title>

  <link rel="stylesheet"
        type="text/css"
        href="https://tools.local.twitter.com/blueprint/css/compiled/global.css"/>

  <script src="http://tools.local.twitter.com/blueprint/js/mootools-core.js"> </script>

  %%(scripts)s

  <body>

  <div id="topbar">%%(topbar)s</div>

  <div id="workspace">
    <div class="fixed-container" id="defaultLayout">
    </div>
  </div>

  </body>
  </html>
  """

  URI_MAP = {
    'index': ['observer.js']
  }

  TOPBARS = {
    'index': '<h2>index</h2>'
  }

  def __init__(self, hostname, port, observer):
    self._extensions_map = mimetypes.types_map.copy()
    self._extensions_map.update({'': 'text/html',
                                '.ico' : 'image/vnd.microsoft.icon',
                                '.json': 'application/json',
                                '.svg' : 'image/svg+xml' })
    self._observer = ObserverRequestTranslator(observer)
    self._httpd    = make_server(hostname, port, self.serve)
    self._hostname = socket.gethostname().split('.')[0]
    self._html     = ObserverHttpHandler.INDEX_HTML % { 'hostname': self._hostname }

    log.info('detecting assets...')
    assets   = pkg_resources.resource_listdir(__name__, 'assets')
    cached_assets = {}
    for asset in assets:
      log.info('  detected asset: %s' % asset)
      cached_assets[asset] = pkg_resources.resource_string(__name__, os.path.join('assets', asset))
    self._assets = cached_assets

    self._httpd.serve_forever()

  @staticmethod
  def script_tags(*script_names):
    return '\n'.join('<script src="%s"></script>' % s for s in script_names)

  @staticmethod
  def getfield(f):
    """convert values from cgi.Field objects to plain values."""
    if isinstance(f, list):
      return [ObserverHttpHandler.getfield(x) for x in f]
    else:
      return f.value

  def guess_type(self, path):
    """return a mimetype for the given path based on file extension."""
    base, ext = posixpath.splitext(path)
    if ext in self._extensions_map:
      return self._extensions_map[ext]
    ext = ext.lower()
    if ext in self._extensions_map:
      return self._extensions_map[ext]
    else:
      return self._extensions_map['']

  def content_type(self, extension):
    return self.guess_type('hello.%s' % extension)

  @staticmethod
  def is_post_request(environ):
    if environ['REQUEST_METHOD'].upper() != 'POST':
      return False
    content_type = environ.get('CONTENT_TYPE', 'application/x-www-form-urlencoded')
    return (content_type.startswith('application/x-www-form-urlencoded') \
            or content_type.startswith('multipart/form-data'))

  def serve(self, environ, start_response):
      """serves requests using the WSGI callable interface."""

      d = {}
      if ObserverHttpHandler.is_post_request(environ):
        try:
          request_body_size = int(environ.get('CONTENT_LENGTH', 0))
        except (ValueError):
          request_body_size = 0
        request_body = environ['wsgi.input'].read(request_body_size)
        d = parse_qs(request_body)
      else:
        d = parse_qs(environ['QUERY_STRING'])

      uri = environ.get('PATH_INFO', '/')
      if not uri:
        uri = '/index'
      else:
        uri = re.sub(r'^/$', '/index', uri)
      if uri[0] == '/': uri = uri[1:]

      # is this a native URI?
      if uri in ObserverHttpHandler.URI_MAP:
        scripts = ObserverHttpHandler.script_tags(*ObserverHttpHandler.URI_MAP.get(uri, []))
        response = self._html % {
          'scripts': scripts,
          'topbar': ObserverHttpHandler.TOPBARS.get(uri, '')
        }
        start_response("200 OK", [('Content-type', 'text/html')])
        return [response]

      # is this an asset?
      elif uri in self._assets:
        asset = self._assets[uri]
        log.info('Returning content-type: %s, length: %s' % (self.guess_type(uri), len(asset)))
        start_response("200 OK", [('Content-type', self.guess_type(uri))])
        return [asset]

      # is this an observer JSON endpoint?
      else:
        response = self._observer(uri, d)
        if response is not None:
          content_type = self.content_type(response[0])
          content = response[1]
          start_response("200 OK", [('Content-type', content_type)])
          return [content]
        else:
          # 404
          start_response("404 Not Found", [])
          return ["Bad robot!"]
