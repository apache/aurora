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

import java.util.Optional;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.Credential;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.v1.Protos;

import static org.apache.aurora.scheduler.mesos.ProtosConversion.convert;

/**
 * A minimal shim over the constructor to {@link MesosSchedulerDriver} to minimize code that
 * requires the libmesos native library.
 */
class DriverFactoryImpl implements DriverFactory {

  @Override
  public SchedulerDriver create(
      Scheduler scheduler,
      Optional<Protos.Credential> credentials,
      Protos.FrameworkInfo frameworkInfo,
      String master) {

    FrameworkInfo convertedFrameworkInfo = convert(frameworkInfo);
    Optional<Credential> convertedCredentials = credentials.map(ProtosConversion::convert);

    if (credentials.isPresent()) {
      return new MesosSchedulerDriver(
          scheduler,
          convertedFrameworkInfo,
          master,
          false, // Disable implicit acknowledgements.
          convertedCredentials.get());
    } else {
      return new MesosSchedulerDriver(
          scheduler,
          convertedFrameworkInfo,
          master,
          false); // Disable implicit acknowledgements.
    }
  }
}
