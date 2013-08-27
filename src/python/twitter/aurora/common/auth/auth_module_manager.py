from twitter.common import log

from gen.twitter.aurora.ttypes import SessionKey

from .auth_module import AuthModule, InsecureAuthModule


_INSECURE_AUTH_MODULE = InsecureAuthModule()
_AUTH_MODULES = {
  _INSECURE_AUTH_MODULE.mechanism: _INSECURE_AUTH_MODULE
}


class SessionKeyError(Exception): pass


def register_auth_module(auth_module):
  """
    Add an auth module into the registry used by make_session_key. An auth module is discovered
    via its auth mechanism.

    args:
      auth_module: A 0-arg callable that should return a SessionKey or raises a SessionKeyError
                   and extend AuthModule.
  """
  if not isinstance(auth_module, AuthModule):
    raise TypeError('Given auth module must be a AuthModule subclass, got %s' % type(auth_module))
  if not callable(auth_module):
    raise TypeError('auth_module should be callable.')
  _AUTH_MODULES[auth_module.mechanism] = auth_module


def make_session_key(auth_mechanism='UNAUTHENTICATED'):
  """
    Attempts to create a session key by calling the auth module registered to the auth mechanism. 
    If an auth module does not exist for an auth mechanism, an InsecureAuthModule will be used.
  """
  if not _AUTH_MODULES:
    raise SessionKeyError('No auth modules have been registered. Please call register_auth_module.')

  auth_module = _AUTH_MODULES.get(auth_mechanism) or _INSECURE_AUTH_MODULE
  log.debug('Using auth module: %r' % auth_module)
  session_key = auth_module()
  if not isinstance(session_key, SessionKey):
    raise SessionKeyError('Expected %r but got %r from auth module %r' % (
      SessionKey, session_key.__class__, auth_module))
  return session_key
