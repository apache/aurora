package com.twitter.mesos.scheduler.httphandlers;

import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.common.base.Closure;
import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.MaintenanceMode;
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

  private Iterable<HostAttributes> getHostAttributes() {
    return storage.doInTransaction(new Work.Quiet<Iterable<HostAttributes>>() {
      @Override public Iterable<HostAttributes> apply(StoreProvider storeProvider) {
        return storeProvider.getAttributeStore().getHostAttributes();
      }
    });
  }

  private static final Function<HostAttributes, String> ATTR_HOST =
      new Function<HostAttributes, String>() {
        @Override public String apply(HostAttributes attributes) {
          return attributes.getHost();
        }
      };

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

        Map<String, SlaveID> hostToId = slaveHosts.getSlaves();
        final Map<String, HostAttributes> hostToAttributes =
            Maps.uniqueIndex(getHostAttributes(), ATTR_HOST);
        Iterable<Slave> slaves = Iterables.transform(hostToId.entrySet(),
            new Function<Entry<String, SlaveID>, Slave>() {
              @Override public Slave apply(Entry<String, SlaveID> entry) {
                HostAttributes attributes = hostToAttributes.get(entry.getKey());
                return new Slave(entry.getKey(), entry.getValue(), attributes);
              }
            });

        template.setAttribute("slaves", ImmutableList.copyOf(slaves));
      }
    });
  }

  private static final Ordering<Attribute> ATTR_ORDER = Ordering.natural().onResultOf(
      new Function<Attribute, String>() {
        @Override public String apply(Attribute attr) {
          return attr .getName();
        }
      });

  /**
   * Template object to represent a slave.
   */
  private static class Slave {
    final String host;
    final SlaveID id;
    final MaintenanceMode mode;
    final Iterable<Attribute> attributes;

    Slave(String host, SlaveID id, @Nullable HostAttributes attributes) {
      this.host = host;
      this.id = id;
      if (attributes != null) {
        this.mode = attributes.getMode();
        this.attributes = ATTR_ORDER.sortedCopy(attributes.getAttributes());
      } else {
        this.mode = null;
        this.attributes = ImmutableList.of();
      }
    }

    public String getHost() {
      return host;
    }

    public SlaveID getId() {
      return id;
    }

    public MaintenanceMode getMode() {
      return mode;
    }

    private static final Function<Attribute, String> ATTR_TO_STRING =
        new Function<Attribute, String>() {
          @Override public String apply(Attribute attr) {
            return attr.getName() + "=[" + Joiner.on(",").join(attr.getValues()) + "]";
          }
        };

    public String getAttributes() {
      return Joiner.on(", ").join(Iterables.transform(attributes, ATTR_TO_STRING));
    }
  }
}
