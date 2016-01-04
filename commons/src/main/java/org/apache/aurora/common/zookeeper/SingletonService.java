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
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.aurora.common.base.ExceptionalCommand;
import org.apache.aurora.common.zookeeper.Candidate.Leader;
import org.apache.aurora.common.zookeeper.Group.JoinException;
import org.apache.zookeeper.data.ACL;

/**
 * A service that uses master election to only allow a single instance of the server to join
 * the {@link ServerSet} at a time.
 */
public class SingletonService {
  @VisibleForTesting
  static final String LEADER_ELECT_NODE_PREFIX = "singleton_candidate_";

  /**
   * Creates a candidate that can be combined with an existing server set to form a singleton
   * service using {@link #SingletonService(ServerSet, Candidate)}.
   *
   * @param zkClient The ZooKeeper client to use.
   * @param servicePath The path where service nodes live.
   * @param acl The acl to apply to newly created candidate nodes and serverset nodes.
   * @return A candidate that can be housed with a standard server set under a single zk path.
   */
  public static Candidate createSingletonCandidate(
      ZooKeeperClient zkClient,
      String servicePath,
      Iterable<ACL> acl) {

    return new CandidateImpl(new Group(zkClient, acl, servicePath, LEADER_ELECT_NODE_PREFIX));
  }

  private final ServerSet serverSet;
  private final Candidate candidate;

  /**
   * Creates a new singleton service that uses the supplied candidate to vie for leadership and then
   * advertises itself in the given server set once elected.
   *
   * @param serverSet The server set to advertise in on election.
   * @param candidate The candidacy to use to vie for election.
   */
  public SingletonService(ServerSet serverSet, Candidate candidate) {
    this.serverSet = Preconditions.checkNotNull(serverSet);
    this.candidate = Preconditions.checkNotNull(candidate);
  }

  /**
   * Attempts to lead the singleton service.
   *
   * @param endpoint The primary endpoint to register as a leader candidate in the service.
   * @param additionalEndpoints Additional endpoints that are available on the host.
   * @param listener Handler to call when the candidate is elected or defeated.
   * @throws Group.WatchException If there was a problem watching the ZooKeeper group.
   * @throws Group.JoinException If there was a problem joining the ZooKeeper group.
   * @throws InterruptedException If the thread watching/joining the group was interrupted.
   */
  public void lead(final InetSocketAddress endpoint,
                   final Map<String, InetSocketAddress> additionalEndpoints,
                   final LeadershipListener listener)
                   throws Group.WatchException, Group.JoinException, InterruptedException {

    Preconditions.checkNotNull(listener);

    candidate.offerLeadership(new Leader() {
      private ServerSet.EndpointStatus endpointStatus = null;
      @Override public void onElected(final ExceptionalCommand<JoinException> abdicate) {
        listener.onLeading(new LeaderControl() {
          ServerSet.EndpointStatus endpointStatus = null;
          final AtomicBoolean left = new AtomicBoolean(false);

          // Methods are synchronized to prevent simultaneous invocations.
          @Override public synchronized void advertise()
              throws JoinException, InterruptedException {

            Preconditions.checkState(!left.get(), "Cannot advertise after leaving.");
            Preconditions.checkState(endpointStatus == null, "Cannot advertise more than once.");
            endpointStatus = serverSet.join(endpoint, additionalEndpoints);
          }

          @Override public synchronized void leave() throws ServerSet.UpdateException, JoinException {
            Preconditions.checkState(left.compareAndSet(false, true),
                "Cannot leave more than once.");
            if (endpointStatus != null) {
              endpointStatus.leave();
            }
            abdicate.execute();
          }
        });
      }

      @Override public void onDefeated() {
        listener.onDefeated(endpointStatus);
      }
    });
  }

  /**
   * A listener to be notified of changes in the leadership status.
   * Implementers should be careful to avoid blocking operations in these callbacks.
   */
  public interface LeadershipListener {

    /**
     * Notifies the listener that is is current leader.
     *
     * @param control A controller handle to advertise and/or leave advertised presence.
     */
    void onLeading(LeaderControl control);

    /**
     * Notifies the listener that it is no longer leader.  The leader should take this opportunity
     * to remove its advertisement gracefully.
     *
     * @param status A handle on the endpoint status for the advertised leader.
     */
    void onDefeated(@Nullable ServerSet.EndpointStatus status);
  }

  /**
   * A controller for the state of the leader.  This will be provided to the leader upon election,
   * which allows the leader to decide when to advertise in the underlying {@link ServerSet} and
   * terminate leadership at will.
   */
  public interface LeaderControl {

    /**
     * Advertises the leader's server presence to clients.
     *
     * @throws JoinException If there was an error advertising.
     * @throws InterruptedException If interrupted while advertising.
     */
    void advertise() throws JoinException, InterruptedException;

    /**
     * Leaves candidacy for leadership, removing advertised server presence if applicable.
     *
     * @throws ServerSet.UpdateException If the leader's status could not be updated.
     * @throws JoinException If there was an error abdicating from leader election.
     */
    void leave() throws ServerSet.UpdateException, JoinException;
  }
}
