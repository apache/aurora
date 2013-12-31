package org.apache.aurora.scheduler.thrift;

import javax.inject.Inject;

import org.apache.aurora.gen.AuroraAdmin;

import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.server.TServlet;

/**
 * A servlet that exposes the scheduler Thrift API over HTTP/JSON.
 */
class SchedulerAPIServlet extends TServlet {

  @Inject
  SchedulerAPIServlet(AuroraAdmin.Iface schedulerThriftInterface) {
    super(new AuroraAdmin.Processor<>(schedulerThriftInterface), new TJSONProtocol.Factory());
  }
}
