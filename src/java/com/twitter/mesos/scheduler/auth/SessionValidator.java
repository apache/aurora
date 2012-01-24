package com.twitter.mesos.scheduler.auth;

import com.google.inject.Inject;

import org.apache.commons.lang.StringUtils;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;
import com.twitter.common_internal.ldap.Ods;
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

    private final Ods ods;
    private final Clock clock;

    @Inject
    public SessionValidatorImpl(Ods ods, Clock clock) {
      this.ods = checkNotNull(ods);
      this.clock = checkNotNull(clock);
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

      String userId = sessionKey.getUser();
      if (!userId.equals(targetRole)) {
        if (!ods.isRoleAccount(targetRole)) {
          throw new AuthFailedException(targetRole + " %s is not a role account.");
        }
      }

      User user = ods.getUser(userId);
      if (user == null) {
        throw new AuthFailedException(String.format("User %s not found.", userId));
      }

      AuthorizedKeySet keySet;
      try {
        keySet = AuthorizedKeySet.createFromKeys(ods.expandKeys(targetRole));
      } catch (KeyParseException e) {
        throw new AuthFailedException("Failed to parse SSH keys for user " + userId);
      }

      if (!keySet.verify(
          Long.toString(sessionKey.getNonce()).getBytes(),
          sessionKey.getNonceSig())) {
        throw new AuthFailedException("Authentication failed for " + userId);
      }
    }
  }
}
