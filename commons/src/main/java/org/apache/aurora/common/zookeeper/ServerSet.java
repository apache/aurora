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
package org.apache.aurora.common.zookeeper;

import java.net.InetSocketAddress;
import java.util.Map;

import org.apache.aurora.common.io.Codec;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.common.zookeeper.Group.JoinException;

/**
 * A logical set of servers registered in ZooKeeper.  Intended to be used by servers in a
 * common service to advertise their presence to server-set protocol-aware clients.
 *
 * Standard implementations should use the {@link #JSON_CODEC} to serialize the service instance
 * rendezvous data to zookeeper so that standard clients can interoperate.
 */
public interface ServerSet {

  /**
   * Encodes a {@link ServiceInstance} as a JSON object.
   *
   * This is the default encoding for service instance data in ZooKeeper.
   */
  Codec<ServiceInstance> JSON_CODEC = new JsonCodec();

  /**
   * Attempts to join a server set for this logical service group.
   *
   * @param endpoint the primary service endpoint
   * @param additionalEndpoints and additional endpoints keyed by their logical name
   * @return an EndpointStatus object that allows the endpoint to adjust its status
   * @throws JoinException if there was a problem joining the server set
   * @throws InterruptedException if interrupted while waiting to join the server set
   */
  EndpointStatus join(
      InetSocketAddress endpoint,
      Map<String, InetSocketAddress> additionalEndpoints)
      throws JoinException, InterruptedException;

  /**
   * A handle to a service endpoint's status data that allows updating it to track current events.
   */
  interface EndpointStatus {

    /**
     * Removes the endpoint from the server set.
     *
     * @throws UpdateException if there was a problem leaving the ServerSet.
     */
    void leave() throws UpdateException;
  }

  /**
   * Indicates an error updating a service's status information.
   */
  class UpdateException extends Exception {
    public UpdateException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
