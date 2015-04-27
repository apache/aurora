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

import com.google.common.base.Optional;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import static org.apache.mesos.Protos.FrameworkInfo;

/**
 * A minimal shim over the constructor to {@link MesosSchedulerDriver} to minimize code that
 * requires the libmesos native library.
 */
class DriverFactoryImpl implements DriverFactory {

  @Override
  public SchedulerDriver create(
      Scheduler scheduler,
      Optional<Protos.Credential> credentials,
      FrameworkInfo frameworkInfo,
      String master) {
    if (credentials.isPresent()) {
      return new MesosSchedulerDriver(
          scheduler,
          frameworkInfo,
          master,
          false, // Disable implicit acknowledgements.
          credentials.get());
    } else {
      return new MesosSchedulerDriver(
          scheduler,
          frameworkInfo,
          master,
          false); // Disable implicit acknowledgements.
    }
  }
}
