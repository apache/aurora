package com.twitter.mesos.scheduler;

import java.util.Map;

import com.google.common.collect.Maps;

import com.twitter.common.thrift.ThriftServer;

/**
 * TODO(John Sirois): consider ejecting support or refactoring thrift server to only require a
 * narrow subset of the ThriftService interface - lots of nothing going on here - these methods
 * are neither implemented nor exposed over thrift.
 *
 * @author John Sirois
 */
class SchedulerThriftServer extends ThriftServer {

  private static final String NO_IMPL = "Not implemented";

  SchedulerThriftServer() {
    super("TwitterMesosScheduler", "1");
  }

  @Override
  protected void tryShutdown() throws Exception {
    // TODO(William Farner): Implement.
  }

  @Override
  public String getStatusDetails() {
    // TODO(William Farner): Return something useful here.
    return NO_IMPL;
  }

  @Override
  public Map<String, Long> getCounters() {
    // TODO(William Farner): Return something useful here.
    return Maps.newHashMap();
  }

  @Override
  public long getCounter(String key) {
    // TODO(William Farner): Return something useful here.
    return 0;
  }

  @Override
  public void setOption(String key, String value) {
    // TODO(William Farner): Implement.
  }

  @Override
  public String getOption(String key) {
    // TODO(William Farner): Return something useful here.
    return NO_IMPL;
  }

  @Override
  public Map<String, String> getOptions() {
    // TODO(William Farner): Return something useful here.
    return Maps.newHashMap();
  }
}
