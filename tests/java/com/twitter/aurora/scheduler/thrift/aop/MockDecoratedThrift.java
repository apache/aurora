package com.twitter.aurora.scheduler.thrift.aop;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import com.twitter.aurora.gen.MesosAdmin;
import com.twitter.aurora.scheduler.thrift.auth.DecoratedThrift;

/**
 * An injected forwarding thrift implementation that delegates to a bound mock interface.
 * <p>
 * This is required to allow AOP to take place. For more details, see
 * https://code.google.com/p/google-guice/wiki/AOP#Limitations
 */
@DecoratedThrift
class MockDecoratedThrift extends ForwardingThrift {

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER, ElementType.METHOD})
  @BindingAnnotation
  private @interface MockThrift { }

  @Inject
  MockDecoratedThrift(@MockThrift MesosAdmin.Iface delegate) {
    super(delegate);
  }

  static void bindForwardedMock(Binder binder, MesosAdmin.Iface mockThrift) {
    binder.bind(MesosAdmin.Iface.class).annotatedWith(MockThrift.class).toInstance(mockThrift);
    binder.bind(MesosAdmin.Iface.class).to(MockDecoratedThrift.class);
  }
}
