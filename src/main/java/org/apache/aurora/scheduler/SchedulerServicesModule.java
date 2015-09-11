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
package org.apache.aurora.scheduler;

import java.util.Set;

import javax.inject.Singleton;

import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.binder.LinkedBindingBuilder;
import com.google.inject.multibindings.Multibinder;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.GuavaUtils.ServiceManagerIface;
import org.apache.aurora.scheduler.SchedulerLifecycle.SchedulerActive;

/**
 * Coordinates scheduler startup.
 */
public class SchedulerServicesModule extends AbstractModule {
  /**
   * Register a Service to run as close as possible to app startup.
   *
   * Usage: {@code addAppStartupServiceBinding(binder()).to(YourService.class)}.
   *
   * @param binder Binder for the current non-private module.
   * @return a linked binding builder with the normal Guice EDSL methods.
   */
  public static LinkedBindingBuilder<Service> addAppStartupServiceBinding(Binder binder) {
    return Multibinder.newSetBinder(binder, Service.class, AppStartup.class).addBinding();
  }

  /**
   * Register a Service to run after storage is ready, but before the scheduler has announced
   * leadership. If this service fails to startup the scheduler will abort.
   *
   * Usage: {@code addSchedulerActiveServiceBinding(binder()).to(YourService.class)}.
   *
   * @param binder Binder for the current non-private module.
   * @return a linked binding builder with the normal Guice EDSL methods.
   */
  public static LinkedBindingBuilder<Service> addSchedulerActiveServiceBinding(Binder binder) {
    return Multibinder.newSetBinder(binder, Service.class, SchedulerActive.class).addBinding();
  }

  @Override
  protected void configure() {
    // Add a binding.
    Multibinder.newSetBinder(binder(), Service.class, AppStartup.class);
    Multibinder.newSetBinder(binder(), Service.class, SchedulerActive.class);
  }

  @Provides
  @Singleton
  @AppStartup
  ServiceManagerIface provideAppStartupServiceManager(@AppStartup Set<Service> services) {
    return GuavaUtils.serviceManager(new ServiceManager(services));
  }

  @Provides
  @Singleton
  @SchedulerActive
  ServiceManagerIface provideSchedulerActiveServiceManager(@SchedulerActive Set<Service> services) {
    return GuavaUtils.serviceManager(new ServiceManager(services));
  }
}
