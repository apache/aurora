package com.twitter.mesos.scheduler.thrift.auth;

import java.lang.annotation.Annotation;
import java.util.Map;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;

import org.aopalliance.intercept.MethodInterceptor;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotEmpty;
import com.twitter.common_internal.ldap.Ods;
import com.twitter.mesos.GuiceUtils;
import com.twitter.mesos.auth.SessionValidator;
import com.twitter.mesos.auth.SessionValidator.SessionValidatorImpl;
import com.twitter.mesos.auth.SessionValidator.UserValidator;
import com.twitter.mesos.auth.SessionValidator.UserValidator.AngryBirdValidator;
import com.twitter.mesos.auth.SessionValidator.UserValidator.ODSValidator;
import com.twitter.mesos.auth.SessionValidator.UserValidator.UnsecureValidator;
import com.twitter.mesos.gen.MesosAdmin;
import com.twitter.mesos.scheduler.thrift.auth.CapabilityValidator.Capability;
import com.twitter.mesos.scheduler.thrift.auth.CapabilityValidator.CapabilityValidatorImpl;

/**
 * Binding module for authentication of users with special capabilities for admin functions.
 */
public class AuthModule extends AbstractModule {

  private static final Logger LOG = Logger.getLogger(AuthModule.class.getName());

  private static final Map<Capability, String> DEFAULT_CAPABILITIES =
      ImmutableMap.of(Capability.ROOT, "mesos");

  @CmdLine(name = "auth_mode", help = "Enforces RPC authentication with mesos client.")
  private static final Arg<AuthMode> AUTH_MODE = Arg.create(AuthMode.SECURE);

  @NotEmpty
  @CmdLine(name = "user_capabilities",
      help = "Concrete name mappings for administration capabilities.")
  private static final Arg<Map<Capability, String>> USER_CAPABILITIES =
      Arg.create(DEFAULT_CAPABILITIES);

  private enum AuthMode {
    UNSECURE,
    ANGRYBIRD_UNSECURE,
    SECURE
  }

  @Override
  protected void configure() {
    requireBinding(CapabilityValidator.class);
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

    MethodInterceptor authInterceptor = new CapabilityMethodValidator();
    requestInjection(authInterceptor);
    bindInterceptor(
        Matchers.subclassesOf(MesosAdmin.Iface.class),
        GuiceUtils.interfaceMatcher(MesosAdmin.Iface.class, true),
        authInterceptor);
  }

  /**
   * Creates a module that will bind {@link CapabilityValidator}.
   * <p>
   * This is isolated to simplify testing with a custom user capability validator.
   *
   * @return Binding module.
   */
  public static Module userCapabilityModule() {
    return new AbstractModule() {
      @Override protected void configure() {
        requireBinding(SessionValidator.class);
        Preconditions.checkArgument(
            USER_CAPABILITIES.get().containsKey(Capability.ROOT),
            "A ROOT capability must be provided with --user_capabilities");
        bind(new TypeLiteral<Map<Capability, String>>() {
        }).toInstance(USER_CAPABILITIES.get());
        bind(CapabilityValidator.class).to(CapabilityValidatorImpl.class);
        bind(CapabilityValidatorImpl.class).in(Singleton.class);
      }
    };
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
