import time
import base64

from twitter.common_internal.ods import ODS
from paramiko import Agent, RSAKey, Message, SSHException

class SessionKeyHelper(object):
  class AgentError(Exception): pass
  class LDAPError(Exception): pass
  class AuthorizationError(Exception): pass

  @staticmethod
  def get_timestamp():
    timestamp_in_ms = int(time.time() * 1000)
    return timestamp_in_ms

  @staticmethod
  def get_ods_key(username):
    """
      Talk to ODS and the ssh-agent in order to figure out which key is
      your valid ODS key.
    """
    ods = ODS()
    if ods.get_user(username) is None:
      raise SessionKeyHelper.LDAPError('Could not query %s from ODS!' % username)

    # parse all the ldap pubkeys a user has submitted to ODS
    all_ldap_pubkeys = []
    for key in ODS.query_keys(username, ods):
      if key.startswith('ssh-rsa'):
        pkey = RSAKey(data=base64.decodestring(key.split()[1]))
        if pkey:
          all_ldap_pubkeys.append(pkey)

    try:
      agent = Agent()
    except SSHException, e:
      raise SessionKeyHelper.AgentError('Could not talk to SSH agent: %s' % e)

    # for each key in your ssh agent, find the first that matches one of your ODS
    # keys.  the reason we have to do this is because many people have several ssh
    # keys exported by their ssh agent for external websites e.g. GitHub, so signing
    # your session with that will fail since ODS is unaware of those keys.
    for key in agent.get_keys():
      message = 'this is a very important message to keep safe.'
      signed_glob = key.sign_ssh_data(None, message)
      signed_message = Message(signed_glob)
      for ldap_key in all_ldap_pubkeys:
        signed_message.rewind()
        # found a match between ldap_key and agent key, so use this.
        if ldap_key.verify_ssh_sig(message, signed_message):
          return key

    return None

  @staticmethod
  def sign_session(session_key, username):
    key = SessionKeyHelper.get_ods_key(username)
    if key is None:
      raise SessionKeyHelper.AuthorizationError(
        'Could not find valid key for %s' % username)
    ts = SessionKeyHelper.get_timestamp()
    message = str(ts)
    signed_glob = key.sign_ssh_data(None, message)
    session_key.nonce = ts
    session_key.nonceSig = signed_glob
