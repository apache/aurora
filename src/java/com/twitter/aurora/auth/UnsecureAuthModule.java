package com.twitter.aurora.auth;

import java.util.Set;
import java.util.logging.Logger;

import com.google.inject.AbstractModule;

import com.twitter.aurora.gen.SessionKey;

/**
 * An authentication module that uses an {@link UnsecureSessionValidator}. This behavior
 * can be overridden by binding a secure validator, querying an internal authentication system,
 * to {@link SessionValidator}.
 */
public class UnsecureAuthModule extends AbstractModule {

  private static final Logger LOG = Logger.getLogger(UnsecureAuthModule.class.getName());

  @Override
  protected void configure() {
    LOG.info("Using default (UNSECURE!!!) authentication module.");
    bind(SessionValidator.class).to(UnsecureSessionValidator.class);
  }

  static class UnsecureSessionValidator implements SessionValidator {
    private static final Logger LOG = Logger.getLogger(UnsecureSessionValidator.class.getName());

    @Override
    public SessionContext checkAuthenticated(SessionKey key, Set<String> targetRoles)
        throws AuthFailedException {

      LOG.warning("Using unsecure session validator for key: " + key + " roles: " + targetRoles);
      return new SessionContext() {
        @Override public String getIdentity() {
          return "UNSECURE";
        }
      };
    }

    @Override
    public String toString(SessionKey sessionKey) {
      return sessionKey.toString();
    }
  }
}
