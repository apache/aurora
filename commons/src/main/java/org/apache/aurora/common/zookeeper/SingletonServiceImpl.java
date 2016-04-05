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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.aurora.common.base.ExceptionalCommand;
import org.apache.aurora.common.zookeeper.Candidate.Leader;
import org.apache.aurora.common.zookeeper.Group.JoinException;
import org.apache.zookeeper.data.ACL;

public class SingletonServiceImpl implements SingletonService {
  @VisibleForTesting
  static final String LEADER_ELECT_NODE_PREFIX = "singleton_candidate_";

  /**
   * Creates a candidate that can be combined with an existing server set to form a singleton
   * service using {@link #SingletonServiceImpl(ServerSet, Candidate)}.
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
  public SingletonServiceImpl(ServerSet serverSet, Candidate candidate) {
    this.serverSet = Preconditions.checkNotNull(serverSet);
    this.candidate = Preconditions.checkNotNull(candidate);
  }

  @Override
  public void lead(final InetSocketAddress endpoint,
                   final Map<String, InetSocketAddress> additionalEndpoints,
                   final LeadershipListener listener)
                   throws LeadException, InterruptedException {

    Preconditions.checkNotNull(listener);

    try {
      candidate.offerLeadership(new Leader() {
        @Override public void onElected(final ExceptionalCommand<JoinException> abdicate) {
          listener.onLeading(new LeaderControl() {
            ServerSet.EndpointStatus endpointStatus = null;
            final AtomicBoolean left = new AtomicBoolean(false);

            // Methods are synchronized to prevent simultaneous invocations.
            @Override public synchronized void advertise()
                throws AdvertiseException, InterruptedException {

              Preconditions.checkState(!left.get(), "Cannot advertise after leaving.");
              Preconditions.checkState(endpointStatus == null, "Cannot advertise more than once.");
              try {
                endpointStatus = serverSet.join(endpoint, additionalEndpoints);
              } catch (JoinException e) {
                throw new AdvertiseException("Problem advertising endpoint " + endpoint, e);
              }
            }

            @Override public synchronized void leave() throws LeaveException {
              Preconditions.checkState(left.compareAndSet(false, true),
                  "Cannot leave more than once.");
              if (endpointStatus != null) {
                try {
                  endpointStatus.leave();
                } catch (ServerSet.UpdateException e) {
                  throw new LeaveException("Problem updating endpoint status for abdicating leader " +
                      "at endpoint " + endpoint, e);
                }
              }
              try {
                abdicate.execute();
              } catch (JoinException e) {
                throw new LeaveException("Problem abdicating leadership for endpoint " + endpoint, e);
              }
            }
          });
        }

        @Override public void onDefeated() {
          listener.onDefeated();
        }
      });
    } catch (JoinException e) {
      throw new LeadException("Problem joining leadership group for endpoint " + endpoint, e);
    } catch (Group.WatchException e) {
      throw new LeadException("Problem getting initial membership list for leadership group.", e);
    }
  }
}
