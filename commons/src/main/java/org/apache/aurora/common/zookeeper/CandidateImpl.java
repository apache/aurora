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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import org.apache.aurora.common.zookeeper.Group.JoinException;
import org.apache.aurora.common.zookeeper.Group.Membership;
import org.apache.aurora.common.zookeeper.Group.WatchException;
import org.apache.aurora.common.zookeeper.ZooKeeperClient.ZooKeeperConnectionException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements leader election for small groups of candidates.  This implementation is subject to the
 * <a href="http://hadoop.apache.org/zookeeper/docs/r3.2.1/recipes.html#sc_leaderElection">
 * herd effect</a> for a given group and should only be used for small (~10 member) candidate pools.
 */
public class CandidateImpl implements Candidate {
  private static final Logger LOG = LoggerFactory.getLogger(CandidateImpl.class);

  private static final byte[] UNKNOWN_CANDIDATE_DATA = "<unknown>".getBytes(Charsets.UTF_8);

  private static final Supplier<byte[]> IP_ADDRESS_DATA_SUPPLIER = () -> {
    try {
      return InetAddress.getLocalHost().getHostAddress().getBytes();
    } catch (UnknownHostException e) {
      LOG.warn("Failed to determine local address!", e);
      return UNKNOWN_CANDIDATE_DATA;
    }
  };

  private static final Function<Iterable<String>, String> MOST_RECENT_JUDGE =
      candidates -> Ordering.natural().min(candidates);

  private final Group group;

  /**
   * Creates a candidate that can be used to offer leadership for the given {@code group}.
   */
  public CandidateImpl(Group group) {
    this.group = Preconditions.checkNotNull(group);
  }

  @Override
  public Optional<byte[]> getLeaderData()
      throws ZooKeeperConnectionException, KeeperException, InterruptedException {

    String leaderId = getLeader(group.getMemberIds());
    return leaderId == null
        ? Optional.<byte[]>absent()
        : Optional.of(group.getMemberData(leaderId));
  }

  @Override
  public Supplier<Boolean> offerLeadership(final Leader leader)
      throws JoinException, WatchException, InterruptedException {

    final Membership membership = group.join(IP_ADDRESS_DATA_SUPPLIER, leader::onDefeated);

    final AtomicBoolean elected = new AtomicBoolean(false);
    final AtomicBoolean abdicated = new AtomicBoolean(false);
    group.watch(memberIds -> {
      boolean noCandidates = Iterables.isEmpty(memberIds);
      String memberId = membership.getMemberId();

      if (noCandidates) {
        LOG.warn("All candidates have temporarily left the group: " + group);
      } else if (!Iterables.contains(memberIds, memberId)) {
        LOG.error(
            "Current member ID {} is not a candidate for leader, current voting: {}",
            memberId, memberIds);
      } else {
        boolean electedLeader = memberId.equals(getLeader(memberIds));
        boolean previouslyElected = elected.getAndSet(electedLeader);

        if (!previouslyElected && electedLeader) {
          LOG.info("Candidate {} is now leader of group: {}",
              membership.getMemberPath(), memberIds);

          leader.onElected(() -> {
            membership.cancel();
            abdicated.set(true);
          });
        } else if (!electedLeader) {
          if (previouslyElected) {
            leader.onDefeated();
          }
          LOG.info(
              "Candidate {} waiting for the next leader election, current voting: {}",
              membership.getMemberPath(), memberIds);
        }
      }
    });

    return () -> !abdicated.get() && elected.get();
  }

  @Nullable
  private String getLeader(Iterable<String> memberIds) {
    return Iterables.isEmpty(memberIds) ? null : MOST_RECENT_JUDGE.apply(memberIds);
  }
}
