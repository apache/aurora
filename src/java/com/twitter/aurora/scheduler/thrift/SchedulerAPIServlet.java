package com.twitter.aurora.scheduler.thrift;

import com.google.inject.Inject;

import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.server.TServlet;

import com.twitter.aurora.gen.AuroraAdmin;

/**
 * A servlet that exposes the scheduler Thrift API over HTTP/JSON.
 */
class SchedulerAPIServlet extends TServlet {

  @Inject
  SchedulerAPIServlet(AuroraAdmin.Iface schedulerThriftInterface) {
    super(new AuroraAdmin.Processor(schedulerThriftInterface), new TJSONProtocol.Factory());
  }
}
