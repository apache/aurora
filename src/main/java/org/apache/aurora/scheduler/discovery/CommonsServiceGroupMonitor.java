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
package org.apache.aurora.scheduler.discovery;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.net.pool.DynamicHostSet;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.scheduler.app.ServiceGroupMonitor;

import static java.util.Objects.requireNonNull;

class CommonsServiceGroupMonitor implements ServiceGroupMonitor {
  private Optional<Command> closeCommand = Optional.empty();
  private final DynamicHostSet<ServiceInstance> serverSet;
  private final AtomicReference<ImmutableSet<ServiceInstance>> services =
      new AtomicReference<>(ImmutableSet.of());

  @Inject
  CommonsServiceGroupMonitor(DynamicHostSet<ServiceInstance> serverSet) {
    this.serverSet = requireNonNull(serverSet);
  }

  @Override
  public void start() throws MonitorException {
    try {
      closeCommand = Optional.of(serverSet.watch(services::set));
    } catch (DynamicHostSet.MonitorException e) {
      throw new MonitorException("Unable to watch scheduler host set.", e);
    }
  }

  @Override
  public void close() {
    closeCommand.ifPresent(Command::execute);
  }

  @Override
  public ImmutableSet<ServiceInstance> get() {
    return services.get();
  }
}
