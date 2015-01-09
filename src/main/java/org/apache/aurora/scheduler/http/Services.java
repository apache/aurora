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

import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.Service.State;

import org.apache.aurora.GuavaUtils.ServiceManagerIface;
import org.apache.aurora.scheduler.AppStartup;
import org.apache.aurora.scheduler.SchedulerLifecycle.SchedulerActive;

/**
 * Servlet to dump current status of services.
 */
@Path("/services")
public final class Services {
  private final ImmutableList<ServiceManagerIface> serviceManagers;

  @Inject
  Services(
      @SchedulerActive ServiceManagerIface schedulerActiveServiceManager,
      @AppStartup ServiceManagerIface appStartupServiceManager) {

    serviceManagers = ImmutableList.of(schedulerActiveServiceManager, appStartupServiceManager);
  }

  private static final Function<Service, Map<String, Object>> SERVICE_TO_BEAN =
      new Function<Service, Map<String, Object>>() {
        @Override
        public Map<String, Object> apply(Service service) {
          State state = service.state();
          ImmutableMap.Builder<String, Object> bean = ImmutableMap.builder();
          bean.put("name", service.getClass().getSimpleName());
          bean.put("state", state);
          if (state == State.FAILED) {
            bean.put("failureCause", service.failureCause().toString());
          }
          return bean.build();
        }
      };

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getServices() {
    return Response.ok()
        .entity(FluentIterable.from(serviceManagers)
            .transformAndConcat(new Function<ServiceManagerIface, Iterable<Service>>() {
              @Override
              public Iterable<Service> apply(ServiceManagerIface input) {
                return input.servicesByState().values();
              }
            })
            .transform(SERVICE_TO_BEAN)
            .toList())
        .build();
  }
}
