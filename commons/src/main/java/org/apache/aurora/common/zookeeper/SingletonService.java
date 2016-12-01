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

/**
 * A service that uses master election to only allow a single service instance to be active amongst
 * a set of potential servers at a time.
 */
public interface SingletonService {

  /**
   * Indicates an error attempting to lead a group of servers.
   */
  class LeadException extends Exception {
    public LeadException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Indicates an error attempting to advertise leadership of a group of servers.
   */
  class AdvertiseException extends Exception {
    public AdvertiseException(String message) {
      super(message);
    }

    public AdvertiseException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Indicates an error attempting to leave a group of servers, abdicating leadership of the group.
   */
  class LeaveException extends Exception {
    public LeaveException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Attempts to lead the singleton service.
   *
   * @param endpoint The primary endpoint to register as a leader candidate in the service.
   * @param additionalEndpoints Additional endpoints that are available on the host.
   * @param listener Handler to call when the candidate is elected or defeated.
   * @throws LeadException If there was a problem joining or watching the ZooKeeper group.
   * @throws InterruptedException If the thread watching/joining the group was interrupted.
   */
  void lead(
      InetSocketAddress endpoint,
      Map<String, InetSocketAddress> additionalEndpoints,
      LeadershipListener listener)
      throws LeadException, InterruptedException;

  /**
   * A listener to be notified of changes in the leadership status.
   * Implementers should be careful to avoid blocking operations in these callbacks.
   */
  interface LeadershipListener {

    /**
     * Notifies the listener that is is current leader.
     *
     * @param control A controller handle to advertise and/or leave advertised presence.
     */
    void onLeading(LeaderControl control);

    /**
     * Notifies the listener that it is no longer leader.
     */
    void onDefeated();
  }

  /**
   * A controller for the state of the leader.  This will be provided to the leader upon election,
   * which allows the leader to decide when to advertise as leader of the server set and terminate
   * leadership at will.
   */
  interface LeaderControl {

    /**
     * Advertises the leader's server presence to clients.
     *
     * @throws AdvertiseException If there was an error advertising the singleton leader to clients
     *     of the server set.
     * @throws InterruptedException If interrupted while advertising.
     */
    void advertise() throws AdvertiseException, InterruptedException;

    /**
     * Leaves candidacy for leadership, removing advertised server presence if applicable.
     *
     * @throws LeaveException If the leader's status could not be updated or there was an error
     *     abdicating server set leadership.
     */
    void leave() throws LeaveException;
  }
}
