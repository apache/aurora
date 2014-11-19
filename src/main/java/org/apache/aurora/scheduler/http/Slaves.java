/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.http;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.twitter.common.base.Closure;

import org.antlr.stringtemplate.StringTemplate;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IAttribute;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;

import static java.util.Objects.requireNonNull;

import static com.twitter.common.base.MorePreconditions.checkNotBlank;

import static org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import static org.apache.aurora.scheduler.storage.Storage.Work;

/**
 * HTTP interface to serve as a HUD for the mesos slaves tracked in the scheduler.
 */
@Path("/slaves")
public class Slaves extends JerseyTemplateServlet {
  private final String clusterName;
  private final Storage storage;

  /**
   * Injected constructor.
   *
   * @param serverInfo server meta-data that contains the cluster name
   * @param storage store to fetch the host attributes from
   */
  @Inject
  public Slaves(IServerInfo serverInfo, Storage storage) {
    super("slaves");
    this.clusterName = checkNotBlank(serverInfo.getClusterName());
    this.storage = requireNonNull(storage);
  }

  private Iterable<IHostAttributes> getHostAttributes() {
    return storage.read(new Work.Quiet<Iterable<IHostAttributes>>() {
      @Override
      public Iterable<IHostAttributes> apply(StoreProvider storeProvider) {
        return storeProvider.getAttributeStore().getHostAttributes();
      }
    });
  }

  private static final Function<IHostAttributes, Slave> TO_SLAVE =
      new Function<IHostAttributes, Slave>() {
        @Override
        public Slave apply(IHostAttributes attributes) {
          return new Slave(attributes);
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
      @Override
      public void execute(StringTemplate template) {
        template.setAttribute("cluster_name", clusterName);

        template.setAttribute("slaves",
            FluentIterable.from(getHostAttributes()).transform(TO_SLAVE).toList());
      }
    });
  }

  private static final Ordering<IAttribute> ATTR_ORDER = Ordering.natural().onResultOf(
      new Function<IAttribute, String>() {
        @Override
        public String apply(IAttribute attr) {
          return attr .getName();
        }
      });

  /**
   * Template object to represent a slave.
   */
  private static class Slave {
    private final IHostAttributes attributes;

    Slave(IHostAttributes attributes) {
      this.attributes = attributes;
    }

    public String getHost() {
      return attributes.getHost();
    }

    public String getId() {
      return attributes.getSlaveId();
    }

    public MaintenanceMode getMode() {
      return attributes.getMode();
    }

    private static final Function<IAttribute, String> ATTR_TO_STRING =
        new Function<IAttribute, String>() {
          @Override
          public String apply(IAttribute attr) {
            return attr.getName() + "=[" + Joiner.on(",").join(attr.getValues()) + "]";
          }
        };

    public String getAttributes() {
      return Joiner.on(", ").join(
          Iterables.transform(ATTR_ORDER.sortedCopy(attributes.getAttributes()), ATTR_TO_STRING));
    }
  }
}
