package com.twitter.mesos.auth;

import java.lang.annotation.Annotation;

import com.google.inject.Binder;
import com.google.inject.Singleton;

import com.twitter.common_internal.ldap.Ods;
import com.twitter.mesos.auth.SessionValidator.SessionValidatorImpl;
import com.twitter.mesos.auth.SessionValidator.UserValidator;
import com.twitter.mesos.auth.SessionValidator.UserValidator.AngryBirdValidator;
import com.twitter.mesos.auth.SessionValidator.UserValidator.ODSValidator;
import com.twitter.mesos.auth.SessionValidator.UserValidator.UnsecureValidator;

/**
 * Utility class containing binding functions for different authentication strategies.
 */
public final class AuthBindings {

  private AuthBindings() {
    // Utility class.
  }

  private static void bindAuth(Binder binder, Class<? extends UserValidator> userValidator) {
    binder.bind(SessionValidator.class).to(SessionValidatorImpl.class);
    binder.bind(UserValidator.class).to(userValidator);
    binder.bind(userValidator).in(Singleton.class);
  }

  private static void bindAnnotateAuth(
      Binder binder,
      Class<? extends UserValidator> userValidator,
      Class<? extends Annotation> annotation) {
    binder.bind(UserValidator.class).annotatedWith(annotation).to(userValidator);
    binder.bind(userValidator).in(Singleton.class);
  }

  /**
   * Prepares bindings for an LDAP-backed auth system.
   *
   * @param binder Binder to bind against.
   */
  public static void bindLdapAuth(Binder binder) {
    bindAuth(binder, ODSValidator.class);
    binder.bind(Ods.class).in(Singleton.class);
  }

  /**
   * Prepares bindings for an auth system suitable for use in a test environment.
   *
   * @param binder Binder to bind against.
   */
  public static void bindTestAuth(Binder binder) {
    bindAuth(binder, UnsecureValidator.class);
  }

  /**
   * Prepares bindings for an auth system suitable for use with AngryBird.
   * In this mode, only the angrybird user is allowed un-authenticated access,
   * whereas everyone else needs go through LDAP auth.
   *
   * @param binder Binder to bind against.
   */
  public static void bindAngryBirdAuth(Binder binder) {
    bindAuth(binder, AngryBirdValidator.class);
    bindAnnotateAuth(binder, ODSValidator.class, UserValidator.Secure.class);
    bindAnnotateAuth(binder, UnsecureValidator.class, UserValidator.Unsecure.class);
    binder.bind(Ods.class).in(Singleton.class);
  }
}
