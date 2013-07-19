package com.twitter.aurora.scheduler.thrift.aop;

import com.google.inject.AbstractModule;
import com.google.inject.matcher.Matchers;

import com.twitter.aurora.gen.MesosAdmin;

/**
 * Binding module for AOP-style decorations of the thrift API.
 */
public class AopModule extends AbstractModule {
  @Override
  protected void configure() {
    bindInterceptor(
        Matchers.subclassesOf(MesosAdmin.Iface.class),
        Matchers.any(),
        new LoggingInterceptor());
  }
}
