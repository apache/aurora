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
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import org.apache.zookeeper.KeeperException;

import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.base.ExceptionalCommand;
import org.apache.aurora.common.zookeeper.Group.GroupChangeListener;
import org.apache.aurora.common.zookeeper.Group.JoinException;
import org.apache.aurora.common.zookeeper.Group.Membership;
import org.apache.aurora.common.zookeeper.Group.WatchException;
import org.apache.aurora.common.zookeeper.ZooKeeperClient.ZooKeeperConnectionException;

/**
 * Implements leader election for small groups of candidates.  This implementation is subject to the
 * <a href="http://hadoop.apache.org/zookeeper/docs/r3.2.1/recipes.html#sc_leaderElection">
 * herd effect</a> for a given group and should only be used for small (~10 member) candidate pools.
 */
public class CandidateImpl implements Candidate {
  private static final Logger LOG = Logger.getLogger(CandidateImpl.class.getName());

  private static final byte[] UNKNOWN_CANDIDATE_DATA = "<unknown>".getBytes(Charsets.UTF_8);

  private static final Supplier<byte[]> IP_ADDRESS_DATA_SUPPLIER = new Supplier<byte[]>() {
    @Override public byte[] get() {
      try {
        return InetAddress.getLocalHost().getHostAddress().getBytes();
      } catch (UnknownHostException e) {
        LOG.log(Level.WARNING, "Failed to determine local address!", e);
        return UNKNOWN_CANDIDATE_DATA;
      }
    }
  };

  private static final Function<Iterable<String>, String> MOST_RECENT_JUDGE =
      new Function<Iterable<String>, String>() {
        @Override public String apply(Iterable<String> candidates) {
          return Ordering.natural().min(candidates);
        }
      };

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

    final Membership membership = group.join(IP_ADDRESS_DATA_SUPPLIER, new Command() {
      @Override public void execute() {
        leader.onDefeated();
      }
    });

    final AtomicBoolean elected = new AtomicBoolean(false);
    final AtomicBoolean abdicated = new AtomicBoolean(false);
    group.watch(new GroupChangeListener() {
        @Override public void onGroupChange(Iterable<String> memberIds) {
          boolean noCandidates = Iterables.isEmpty(memberIds);
          String memberId = membership.getMemberId();

          if (noCandidates) {
            LOG.warning("All candidates have temporarily left the group: " + group);
          } else if (!Iterables.contains(memberIds, memberId)) {
            LOG.severe(String.format(
                "Current member ID %s is not a candidate for leader, current voting: %s",
                memberId, memberIds));
          } else {
            boolean electedLeader = memberId.equals(getLeader(memberIds));
            boolean previouslyElected = elected.getAndSet(electedLeader);

            if (!previouslyElected && electedLeader) {
              LOG.info(String.format("Candidate %s is now leader of group: %s",
                  membership.getMemberPath(), memberIds));

              leader.onElected(new ExceptionalCommand<JoinException>() {
                @Override public void execute() throws JoinException {
                  membership.cancel();
                  abdicated.set(true);
                }
              });
            } else if (!electedLeader) {
              if (previouslyElected) {
                leader.onDefeated();
              }
              LOG.info(String.format(
                  "Candidate %s waiting for the next leader election, current voting: %s",
                  membership.getMemberPath(), memberIds));
            }
          }
        }
      });

    return new Supplier<Boolean>() {
        @Override public Boolean get() {
          return !abdicated.get() && elected.get();
        }
      };
  }

  @Nullable
  private String getLeader(Iterable<String> memberIds) {
    return Iterables.isEmpty(memberIds) ? null : MOST_RECENT_JUDGE.apply(memberIds);
  }
}
