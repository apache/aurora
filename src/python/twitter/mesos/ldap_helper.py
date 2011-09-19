import ldap

class LdapConnectionException(Exception): pass
class LdapRoleInformation(object):
  def __init__(self):
    try:
      self._conn = ldap.initialize('ldap://ldap.local.twitter.com')
      self._conn.protocol_version = ldap.VERSION3
    except ldap.LDAPError, e:
      raise LdapConnectionException("Could not connect to LDAP: %s" % e)
    self._groups_dn = 'cn=Groups,dc=ods,dc=twitter,dc=corp'
    self._users_dn = 'cn=Users,dc=ods,dc=twitter,dc=corp'
    self._scope = ldap.SCOPE_SUBTREE

  def is_valid_role_pair(self, role, user):
    if role == user: return True  # short circuit
    if not self.is_role_account(role): return False
    groups = self.groups_from_uid(user)
    return role in groups

  @staticmethod
  def ldap_attr(obj, key):
    "extract key from the mysterious ldap data layout"
    return obj[0][1][key][0]

  def is_role_account(self, user):
    ldap_user = self._get_user(user)
    if len(ldap_user) != 1:
      return False
    uid = self.ldap_attr(ldap_user, 'uid')
    return self._conn.compare_s(
      "cn=serviceaccount,cn=groups,dc=ods,dc=twitter,dc=corp", 'memberUid', uid)

  def _get_user(self, user):
    filter = 'uid=%s' % user
    request = self._conn.search(self._users_dn, self._scope, filter)
    result_type, result_data = self._conn.result(request, 1)
    if result_type == ldap.RES_SEARCH_RESULT:
      return result_data

  def groups_from_uid(self, uid):
    filter = 'memberUid=%s' % uid
    request = self._conn.search(self._groups_dn, self._scope, filter)
    groups = []
    while True:
      result_type, result_data = self._conn.result(request, 0)
      if result_data:
        groups.append(self.ldap_attr(result_data, 'cn'))
      else:
        break
    return groups

class LdapHelper:
  @staticmethod
  def is_valid_identity(role, user):
    lc = LdapRoleInformation()
    return lc.is_valid_role_pair(role, user)
