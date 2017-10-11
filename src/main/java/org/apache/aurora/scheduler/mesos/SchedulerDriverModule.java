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
package org.apache.aurora.scheduler.mesos;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.Executor;

import javax.inject.Qualifier;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;

import org.apache.aurora.scheduler.app.SchedulerMain.Options.DriverKind;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.aurora.scheduler.mesos.MesosCallbackHandler.MesosCallbackHandlerImpl;
import org.apache.mesos.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static com.google.common.base.Preconditions.checkState;

/**
 * A module that creates a {@link Driver} binding.
 */
public class SchedulerDriverModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(SchedulerDriverModule.class);
  private final DriverKind kind;

  public SchedulerDriverModule(DriverKind kind) {
    this.kind = kind;
  }

  /**
   * Binding annotation for the executor the incoming Mesos message handler uses.
   */
  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface SchedulerExecutor { }

  @Override
  protected void configure() {
    bind(Scheduler.class).to(MesosSchedulerImpl.class);
    bind(org.apache.mesos.v1.scheduler.Scheduler.class).to(VersionedMesosSchedulerImpl.class);
    bind(MesosSchedulerImpl.class).in(Singleton.class);
    bind(MesosCallbackHandler.class).to(MesosCallbackHandlerImpl.class);
    bind(MesosCallbackHandlerImpl.class).in(Singleton.class);
    // TODO(zmanji): Create singleThreadedExecutor (non-scheduled) variant.
    bind(Executor.class).annotatedWith(SchedulerExecutor.class)
        .toInstance(AsyncUtil.singleThreadLoggingScheduledExecutor("SchedulerImpl-%d", LOG));

    switch (kind) {
      case SCHEDULER_DRIVER:
        bind(Driver.class).to(SchedulerDriverService.class);
        bind(SchedulerDriverService.class).in(Singleton.class);
        break;
      case V0_DRIVER:
        bind(Driver.class).to(VersionedSchedulerDriverService.class);
        bind(VersionedSchedulerDriverService.class).in(Singleton.class);
        PubsubEventModule.bindSubscriber(binder(), VersionedSchedulerDriverService.class);
        break;
      case V1_DRIVER:
        bind(Driver.class).to(VersionedSchedulerDriverService.class);
        bind(VersionedSchedulerDriverService.class).in(Singleton.class);
        PubsubEventModule.bindSubscriber(binder(), VersionedSchedulerDriverService.class);
        break;
      default:
        checkState(false, "Unknown driver kind.");
        break;
    }

    PubsubEventModule.bindSubscriber(binder(), TaskStatusStats.class);
    bind(TaskStatusStats.class).in(Singleton.class);
  }
}
