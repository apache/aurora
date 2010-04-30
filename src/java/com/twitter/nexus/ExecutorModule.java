package com.twitter.nexus;

import com.google.inject.AbstractModule;

import java.util.logging.Logger;

/**
 * ExecutorModule
 *
 * @author Florian Leibert
 */
public class ExecutorModule extends AbstractModule {
  private final static java.util.logging.Logger LOG = Logger.getLogger(ExecutorModule.class.getName());

  @Override
  protected void configure() {
    bind(ExecutorCore.class);
    bind(ExecutorHub.class);
  }
}
