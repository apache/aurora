package com.twitter.mesos.scheduler.thrift.auth;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Singleton;
import com.google.inject.matcher.Matchers;

import org.aopalliance.intercept.MethodInterceptor;

import com.twitter.mesos.GuiceUtils;
import com.twitter.mesos.auth.SessionValidator;
import com.twitter.mesos.gen.MesosAdmin;
import com.twitter.mesos.scheduler.thrift.SchedulerThriftRouter;
import com.twitter.mesos.scheduler.thrift.auth.CapabilityValidator.CapabilityValidatorImpl;

/**
 * Binding module for authentication of users with special capabilities for admin functions.
 */
public class ThriftAuthModule extends AbstractModule {

  @Override
  protected void configure() {
    requireBinding(CapabilityValidator.class);
    MethodInterceptor authInterceptor = new CapabilityMethodValidator();
    requestInjection(authInterceptor);
    bindInterceptor(
        Matchers.subclassesOf(SchedulerThriftRouter.class),
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
        bind(CapabilityValidator.class).to(CapabilityValidatorImpl.class);
        bind(CapabilityValidatorImpl.class).in(Singleton.class);
      }
    };
  }
}
