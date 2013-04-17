package com.twitter.mesos.scheduler.thrift;

import com.twitter.common.thrift.ThriftServer;

/**
 * Thin implementation of ThriftServer.
 *
 * @author John Sirois
 */
class SchedulerThriftServer extends ThriftServer {

  SchedulerThriftServer() {
    super("TwitterMesosScheduler", "1");
  }
}
