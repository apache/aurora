package com.twitter.mesos.scheduler.thrift.auth;

import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;

import org.aopalliance.intercept.MethodInterceptor;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotEmpty;
import com.twitter.mesos.GuiceUtils;
import com.twitter.mesos.auth.SessionValidator;
import com.twitter.mesos.gen.MesosAdmin;
import com.twitter.mesos.scheduler.thrift.auth.CapabilityValidator.Capability;
import com.twitter.mesos.scheduler.thrift.auth.CapabilityValidator.CapabilityValidatorImpl;

/**
 * Binding module for authentication of users with special capabilities for admin functions.
 */
public class AuthModule extends AbstractModule {

  private static final Map<Capability, String> DEFAULT_CAPABILITIES =
      ImmutableMap.of(Capability.ROOT, "mesos");

  @NotEmpty
  @CmdLine(name = "user_capabilities",
      help = "Concrete name mappings for administration capabilities.")
  private static final Arg<Map<Capability, String>> USER_CAPABILITIES =
      Arg.create(DEFAULT_CAPABILITIES);

  @Override
  protected void configure() {
    requireBinding(CapabilityValidator.class);
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
}
