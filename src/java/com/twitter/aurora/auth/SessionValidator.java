package com.twitter.aurora.auth;

import com.twitter.aurora.gen.SessionKey;

/**
 * Validator for RPC sessions with Aurora.
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
}
