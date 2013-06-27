package com.twitter.mesos.auth;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;
import com.twitter.common_internal.ldap.Ods;
import com.twitter.common_internal.ldap.Ods.LdapException;
import com.twitter.common_internal.ldap.User;
import com.twitter.mesos.auth.AuthorizedKeySet.KeyParseException;
import com.twitter.mesos.gen.SessionKey;

import static com.twitter.mesos.gen.Constants.GOOD_IDENTIFIER_PATTERN_JVM;

/**
 * Validator for RPC sessions with the mesos scheduler.
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
  static class SessionValidatorImpl implements SessionValidator {

    @VisibleForTesting
    static final Amount<Long, Time> MAXIMUM_NONCE_DRIFT = Amount.of(60L, Time.SECONDS);

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

      if (!Pattern.matches(GOOD_IDENTIFIER_PATTERN_JVM, targetRole)) {
        throw new AuthFailedException("Target role " + targetRole + " is not well formed.");
      }

      long now = this.clock.nowMillis();
      long diff = Math.abs(now - sessionKey.getNonce());
      if (Amount.of(diff, Time.MILLISECONDS).compareTo(MAXIMUM_NONCE_DRIFT) > 0) {
        throw new AuthFailedException("Session key nonce expired. "
            + "Ensure system clock is synced with the server.");
      }

      userValidator.assertRoleAccess(sessionKey, targetRole);
    }

    /**
     * Creates a SessionKey from the supplied parameters
     *
     * @param user Owner of the SessionKey
     * @param nonce Nonce of the SessionKey
     * @param hexToken Hex-encoded signature of the nonce
     * @throws DecoderException If the provided hexToken cannot be properly decoded
     */
    public static SessionKey createKey(String user, long nonce, String hexToken)
        throws DecoderException {
      return new SessionKey(user, nonce, ByteBuffer.wrap(Hex.decodeHex(hexToken.toCharArray())));
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

    @BindingAnnotation @Target({FIELD, PARAMETER, METHOD}) @Retention(RUNTIME)
    public @interface Secure { }

    @BindingAnnotation @Target({FIELD, PARAMETER, METHOD}) @Retention(RUNTIME)
    public @interface Unsecure { }

    /**
     * User validator that checks against ODS LDAP Server.
     */
    static class ODSValidator implements UserValidator {

      private final Ods ods;

      @Inject
      ODSValidator(Ods ods) {
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
    static class UnsecureValidator implements UserValidator {

      @Override
      public void assertRoleAccess(SessionKey sessionKey, String targetRole)
          throws AuthFailedException {

        String signature = new String(sessionKey.getNonceSig());
        if (StringUtils.isBlank(signature)) {
          throw new AuthFailedException("Blank signature");
        }
      }
    }

    /**
     * Validator that allows angrybird user non-authenticated access.
     * All other users are validated against ODS.
     */
    static class AngryBirdValidator implements UserValidator {

      private final UserValidator odsValidator;
      private final UserValidator unsecureValidator;

      @Inject
      AngryBirdValidator(
          @Secure UserValidator odsValidator,
          @Unsecure UserValidator unsecureValidator) {
        this.odsValidator = odsValidator;
        this.unsecureValidator = unsecureValidator;
      }

      @Override
      public void assertRoleAccess(SessionKey sessionKey, String targetRole)
          throws AuthFailedException {

        String userId = sessionKey.getUser();

        if ("angrybird".equals(userId)) {
          unsecureValidator.assertRoleAccess(sessionKey, targetRole);
        } else {
          odsValidator.assertRoleAccess(sessionKey, targetRole);
        }
      }
    }
  }
}
