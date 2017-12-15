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

import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.scheduler.Mesos;
import org.apache.mesos.v1.scheduler.Scheduler;

/**
 * A layer over the constructor for {@link org.apache.mesos.v1.scheduler.Mesos}. This is needed
 * since {@link org.apache.mesos.v1.scheduler.Mesos} implementations statically load libmesos.
 */
public interface VersionedDriverFactory {
  Mesos create(
      Scheduler scheduler,
      Protos.FrameworkInfo frameworkInfo,
      String master,
      Optional<Protos.Credential> credentials);
}
