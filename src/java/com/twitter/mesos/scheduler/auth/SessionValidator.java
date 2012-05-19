package com.twitter.mesos.scheduler.auth;

import com.google.inject.Inject;

import org.apache.commons.lang.StringUtils;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;
import com.twitter.common_internal.ldap.Ods;
import com.twitter.common_internal.ldap.Ods.LdapException;
import com.twitter.common_internal.ldap.User;
import com.twitter.mesos.gen.SessionKey;
import com.twitter.mesos.scheduler.identity.AuthorizedKeySet;
import com.twitter.mesos.scheduler.identity.AuthorizedKeySet.KeyParseException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Validator for RPC sessions with the mesos scheduler.
 *
 * @author William Farner
 */
public interface SessionValidator {

  /**
   * Checks whether a session key is authenticated, and has permission to act as a role.
   *
   * @param sessionKey Key to validate.
   * @param targetRole Role to validate the key against.
   * @throws AuthFailedException If the key cannot be validated as the role.
   */
  void checkAuthenticated(SessionKey sessionKey, String targetRole) throws AuthFailedException;

  /**
   * Thrown when authentication is not successful.
   */
  public static class AuthFailedException extends Exception {
    public AuthFailedException(String msg) {
      super(msg);
    }

    public AuthFailedException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  /**
   * Session validator that verifies users against the twitter ODS LDAP server.
   */
  public static class SessionValidatorImpl implements SessionValidator {

    private static final Amount<Long, Time> MAXIMUM_NONCE_DRIFT = Amount.of(60L, Time.SECONDS);

    private final Clock clock;
    private final UserValidator userValidator;

    @Inject
    public SessionValidatorImpl(Clock clock, UserValidator userValidator) {
      this.clock = checkNotNull(clock);
      this.userValidator = checkNotNull(userValidator);
    }

    @Override
    public void checkAuthenticated(SessionKey sessionKey, String targetRole)
        throws AuthFailedException {

      if (StringUtils.isBlank(sessionKey.getUser())
          || !sessionKey.isSetNonce()
          || !sessionKey.isSetNonceSig()) {
        throw new AuthFailedException("Incorrectly specified session key.");
      }

      long now = this.clock.nowMillis();
      long diff = Math.abs(now - sessionKey.getNonce());
      if (Amount.of(diff, Time.MILLISECONDS).compareTo(MAXIMUM_NONCE_DRIFT) > 0) {
        throw new AuthFailedException("Session key nonce expired.");
      }

      userValidator.assertRoleAccess(sessionKey, targetRole);
    }
  }


  public interface UserValidator {

    /**
     * Validates the sessionKey against the user.
     *
     * @param sessionKey to validate.
     * @param targetRole to validate the sessionKey against.
     * @throws AuthFailedException If the key cannot be validated as the role.
     */
    void assertRoleAccess(SessionKey sessionKey, String targetRole) throws AuthFailedException;

    /**
     * User validator that checks against ODS LDAP Server.
     */
    public static class ODSValidator implements UserValidator {

      private final Ods ods;

      @Inject
      public ODSValidator(Ods ods) {
        this.ods = checkNotNull(ods);
      }

      @Override
      public void assertRoleAccess(SessionKey sessionKey, String targetRole)
          throws AuthFailedException {

        String userId = sessionKey.getUser();
        AuthorizedKeySet keySet;
        try {
          if (!userId.equals(targetRole)) {
            if (!ods.isRoleAccount(targetRole)) {
              throw new AuthFailedException(targetRole + " is not a role account.");
            }
          }

          User user = ods.getUser(userId);
          if (user == null) {
            throw new AuthFailedException(String.format("User %s not found.", userId));
          }

          try {
            keySet = AuthorizedKeySet.createFromKeys(ods.expandKeys(targetRole));
          } catch (KeyParseException e) {
            throw new AuthFailedException("Failed to parse SSH keys for user " + userId);
          }
        } catch (LdapException e) {
          throw new AuthFailedException("LDAP request failed: " + e.getMessage(), e);
        }

        if (!keySet.verify(
            Long.toString(sessionKey.getNonce()).getBytes(),
            sessionKey.getNonceSig())) {
          throw new AuthFailedException("Authentication failed for " + userId);
        }
      }
    }

    /**
     * User validator that simply checks for non-blank signature.
     */
    public static class TestValidator implements UserValidator {

      @Override
      public void assertRoleAccess(SessionKey sessionKey, String targetRole)
          throws AuthFailedException {

        String signature = new String(sessionKey.getNonceSig());
        if (StringUtils.isBlank(signature)) {
          throw new AuthFailedException("Blank signature");
        }
      }
    }
  }
}
