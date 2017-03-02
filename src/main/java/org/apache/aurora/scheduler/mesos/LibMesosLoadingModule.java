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

import com.google.inject.AbstractModule;

import org.apache.aurora.scheduler.app.SchedulerMain;
import org.apache.mesos.v1.scheduler.V0Mesos;
import org.apache.mesos.v1.scheduler.V1Mesos;

import static com.google.common.base.Preconditions.checkState;

/**
 * A module that binds a driver factory which requires the libmesos native libary.
 */
public class LibMesosLoadingModule extends AbstractModule {
  private final SchedulerMain.DriverKind kind;

  public LibMesosLoadingModule(SchedulerMain.DriverKind kind)  {
    this.kind = kind;
  }

  @Override
  protected void configure() {
    switch(kind) {
      case SCHEDULER_DRIVER:
        bind(DriverFactory.class).to(DriverFactoryImpl.class);
        break;
      case V0_DRIVER:
        bind(VersionedDriverFactory.class).toInstance((scheduler, frameworkInfo, master, creds)
            -> new V0Mesos(scheduler, frameworkInfo, master, creds.orNull()));
        break;
      case V1_DRIVER:
        bind(VersionedDriverFactory.class).toInstance((scheduler, frameworkInfo, master, creds)
            -> new V1Mesos(scheduler, master, creds.orNull()));
        break;
      default:
        checkState(false, "Unknown driver kind");
        break;
    }
  }
}
