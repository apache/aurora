from twitter.common import log

from gen.twitter.mesos.ttypes import SessionKey


_AUTH_MODULES = []


class SessionKeyError(Exception): pass


def register_auth_module(auth_module):
  """
    Add an auth module into the registry used by make_session_key. The last registered
    module is the first to be called.

    args:
      auth_module: A 0-arg callable that should return a SessionKey or raises a SessionKeyError.
  """
  if not callable(auth_module):
    raise ValueError('auth_module should be callable.')
  _AUTH_MODULES.insert(0, auth_module)


def make_session_key():
  """
    Attempts to create a session key by calling the registered modules in order. If a module
    raises a SessionKeyError the next will be tried.
  """
  if not _AUTH_MODULES:
    raise SessionKeyError('No auth modules have been registered. Please call register_auth_module.')

  for auth_module in _AUTH_MODULES:
    try:
      log.debug('Using auth module: %r' % auth_module)
      session_key = auth_module()
      if not isinstance(session_key, SessionKey):
        raise SessionKeyError('Expected %r but got %r from auth module %r' % (
          SessionKey, session_key.__class__, auth_module))
      return session_key
    except SessionKeyError as e:
      log.warning('Unable to use auth module %r due to %s' % (auth_module, e))

  raise SessionKeyError('Unable to create a session key.')
