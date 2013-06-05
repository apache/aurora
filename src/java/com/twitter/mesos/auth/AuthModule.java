package com.twitter.mesos.auth;

import java.lang.annotation.Annotation;
import java.util.logging.Logger;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Singleton;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common_internal.ldap.Ods;
import com.twitter.mesos.auth.SessionValidator.SessionValidatorImpl;
import com.twitter.mesos.auth.SessionValidator.UserValidator;
import com.twitter.mesos.auth.SessionValidator.UserValidator.AngryBirdValidator;
import com.twitter.mesos.auth.SessionValidator.UserValidator.ODSValidator;
import com.twitter.mesos.auth.SessionValidator.UserValidator.UnsecureValidator;

/**
 * Binding module to set up an authentication system, exposed as {@link SessionValidator}.
 */
public class AuthModule extends AbstractModule {

  private static final Logger LOG = Logger.getLogger(AuthModule.class.getName());

  private enum AuthMode {
    UNSECURE,
    ANGRYBIRD_UNSECURE,
    SECURE
  }

  @CmdLine(name = "auth_mode", help = "Enforces RPC authentication with mesos client.")
  private static final Arg<AuthMode> AUTH_MODE = Arg.create(AuthMode.SECURE);

  @Override
  protected void configure() {
    switch(AUTH_MODE.get()) {
      case SECURE:
        LOG.info("Using secure authentication mode");
        bindAuth(binder(), ODSValidator.class);
        bind(Ods.class).in(Singleton.class);
        break;

      case UNSECURE:
        LOG.warning("Using unsecure authentication mode");
        bindAuth(binder(), UnsecureValidator.class);
        break;

      case ANGRYBIRD_UNSECURE:
        LOG.warning("Using angrybird authentication mode");
        bindAuth(binder(), AngryBirdValidator.class);
        bindAnnotatedAuth(binder(), ODSValidator.class, UserValidator.Secure.class);
        bindAnnotatedAuth(binder(), UnsecureValidator.class, UserValidator.Unsecure.class);
        bind(Ods.class).in(Singleton.class);
        break;

      default:
        throw new IllegalArgumentException("Invalid authentication mode: " + AUTH_MODE.get());
    }
  }

  private static void bindAuth(Binder binder, Class<? extends UserValidator> userValidator) {
    binder.bind(SessionValidator.class).to(SessionValidatorImpl.class);
    binder.bind(UserValidator.class).to(userValidator);
    binder.bind(userValidator).in(Singleton.class);
  }

  private static void bindAnnotatedAuth(
      Binder binder,
      Class<? extends UserValidator> userValidator,
      Class<? extends Annotation> annotation) {

    binder.bind(UserValidator.class).annotatedWith(annotation).to(userValidator);
    binder.bind(userValidator).in(Singleton.class);
  }
}
