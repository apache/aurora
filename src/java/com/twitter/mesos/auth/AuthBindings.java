package com.twitter.mesos.auth;

import com.google.inject.Binder;
import com.google.inject.Singleton;

import com.twitter.common_internal.ldap.Ods;
import com.twitter.mesos.auth.SessionValidator.SessionValidatorImpl;
import com.twitter.mesos.auth.SessionValidator.UserValidator;
import com.twitter.mesos.auth.SessionValidator.UserValidator.ODSValidator;
import com.twitter.mesos.auth.SessionValidator.UserValidator.TestValidator;

/**
 * Utility class containing binding functions for different authentication strategies.
 */
public final class AuthBindings {

  private AuthBindings() {
    // Utility class.
  }

  /**
   * Prepares bindings for an LDAP-backed auth system.
   *
   * @param binder Binder to bind against.
   */
  public static void bindLdapAuth(Binder binder) {
    binder.bind(SessionValidator.class).to(SessionValidatorImpl.class);
    binder.bind(UserValidator.class).to(ODSValidator.class);
    binder.bind(ODSValidator.class).in(Singleton.class);
    binder.bind(Ods.class).in(Singleton.class);
  }

  /**
   * Prepares bindings for an auth system suitable for use in a test environment.
   *
   * @param binder Binder to bind against.
   */
  public static void bindTestAuth(Binder binder) {
    binder.bind(SessionValidator.class).to(SessionValidatorImpl.class);
    binder.bind(UserValidator.class).to(TestValidator.class);
    binder.bind(TestValidator.class).in(Singleton.class);
  }
}
