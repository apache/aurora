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

import java.util.concurrent.Executor;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.PrivateModule;

import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.events.PubsubEventModule;
import org.apache.mesos.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A module that creates a {@link Driver} binding.
 */
public class SchedulerDriverModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(SchedulerDriverModule.class);

  @Override
  protected void configure() {
    install(new PrivateModule() {
      @Override
      protected void configure() {
        bind(Driver.class).to(SchedulerDriverService.class);
        bind(SchedulerDriverService.class).in(Singleton.class);
        expose(Driver.class);

        bind(Scheduler.class).to(MesosSchedulerImpl.class);
        bind(MesosSchedulerImpl.class).in(Singleton.class);
        expose(Scheduler.class);

        // TODO(zmanji): Create singleThreadedExecutor (non-scheduled) variant.
        bind(Executor.class).annotatedWith(MesosSchedulerImpl.SchedulerExecutor.class)
            .toInstance(AsyncUtil.singleThreadLoggingScheduledExecutor("SchedulerImpl-%d", LOG));
      }
    });

    PubsubEventModule.bindSubscriber(binder(), TaskStatusStats.class);
    bind(TaskStatusStats.class).in(Singleton.class);
  }
}
