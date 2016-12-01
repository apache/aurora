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
package org.apache.aurora.scheduler.app;

import java.io.Closeable;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.thrift.ServiceInstance;

/**
 * Monitors a service group's membership and supplies a live view of the most recent set.
 */
public interface ServiceGroupMonitor extends Supplier<ImmutableSet<ServiceInstance>>, Closeable {

  /**
   * Indicates a problem initiating monitoring of a service group.
   */
  class MonitorException extends Exception {
    public MonitorException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Starts monitoring the service group.
   *
   * When the service group membership no longer needs to be maintained, this monitor should be
   * {@link #close() closed}.
   *
   * @throws MonitorException if there is a problem initiating monitoring of the service group.
   */
  void start() throws MonitorException;
}
