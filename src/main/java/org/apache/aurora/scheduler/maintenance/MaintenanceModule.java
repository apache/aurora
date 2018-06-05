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
package org.apache.aurora.scheduler.maintenance;

import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Singleton;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.config.validators.PositiveAmount;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Binding module for maintenance related logic.
 */
public class MaintenanceModule extends AbstractModule {

  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceModule.class);

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-host_maintenance_polling_interval",
        validateValueWith = PositiveAmount.class,
        description = "Interval between polling for pending host maintenance requests.")
    public TimeAmount hostMaintenancePollingInterval = new TimeAmount(1, Time.MINUTES);
  }

  private final Options options;

  public MaintenanceModule(final Options options) {
    this.options = options;
  }

  @Override
  protected void configure() {
    bind(new TypeLiteral<Amount<Long, Time>>() { })
        .annotatedWith(
            MaintenanceController.MaintenanceControllerImpl.PollingInterval.class)
        .toInstance(options.hostMaintenancePollingInterval);
    bind(MaintenanceController.class).to(MaintenanceController.MaintenanceControllerImpl.class);
    bind(MaintenanceController.MaintenanceControllerImpl.class).in(Singleton.class);
    PubsubEventModule.bindSubscriber(
        binder(),
        MaintenanceController.MaintenanceControllerImpl.class);

    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(ScheduledExecutorService.class).toInstance(
            AsyncUtil.singleThreadLoggingScheduledExecutor("MaintenanceController-%d", LOG));
      }
    });
    SchedulerServicesModule.addSchedulerActiveServiceBinding(binder())
        .to(MaintenanceController.MaintenanceControllerImpl.class);
  }
}
