package com.twitter.mesos.scheduler.httphandlers;

import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.common.base.Closure;
import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.scheduler.ClusterName;
import com.twitter.mesos.scheduler.MesosSchedulerImpl.SlaveHosts;
import com.twitter.mesos.scheduler.storage.Storage;

import static com.google.common.base.Preconditions.checkNotNull;

import static org.apache.mesos.Protos.SlaveID;

import static com.twitter.common.base.MorePreconditions.checkNotBlank;
import static com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import static com.twitter.mesos.scheduler.storage.Storage.Work;


/**
 * HTTP interface to serve as a HUD for the mesos slaves tracked in the scheduler.
 */
@Path("/slaves")
public class Slaves extends JerseyTemplateServlet {
  private final String clusterName;
  private final SlaveHosts slaveHosts;
  private Storage storage;

  private final Function<Map.Entry<String, SlaveID>, Slave> slaveMapping =
      new Function<Map.Entry<String, SlaveID>, Slave>() {
        @Override public Slave apply(Map.Entry<String, SlaveID> input) {
          return new Slave(input.getKey(), input.getValue(), getHostAttributes(input.getKey()));
        }
      };

  /**
   * Injected constructor.
   *
   * @param clusterName cluster name
   * @param slaveHosts slave hosts
   * @param storage store to fetch the host attributes from
   */
  @Inject
  public Slaves(
      @ClusterName String clusterName,
      SlaveHosts slaveHosts,
      Storage storage) {

    super("slaves");
    this.clusterName = checkNotBlank(clusterName);
    this.slaveHosts = checkNotNull(slaveHosts);
    this.storage = checkNotNull(storage);
  }

  private List<Attribute> getHostAttributes(final String key) {
    return ImmutableList.copyOf(storage.doInTransaction(new Work.Quiet<Iterable<Attribute>>() {
      @Override public Iterable<Attribute> apply(StoreProvider storeProvider) {
        return storeProvider.getAttributeStore().getHostAttributes(key);
      }
    }));
  }

  /**
   * Fetches the listing of known slaves.
   *
   * @return HTTP response.
   */
  @GET
  @Produces(MediaType.TEXT_HTML)
  public Response get() {
    return fillTemplate(new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
        template.setAttribute("cluster_name", clusterName);
        template.setAttribute("slaves", ImmutableList.copyOf(
            Iterables.transform(slaveHosts.getSlaves().entrySet(), slaveMapping)));
      }
    });
  }

  /**
   * Template object to represent a slave.
   */
  private static class Slave {
    final String host;
    final SlaveID id;
    final List<Attribute> attributes;

    Slave(String host, SlaveID id, List<Attribute> attributes) {
      this.host = host;
      this.id = id;
      this.attributes = attributes;
    }

    public String getHost() {
      return host;
    }

    public SlaveID getId() {
      return id;
    }

    public String getAttributes() {
      return Joiner.on(", ").join(attributes);
    }
  }
}
