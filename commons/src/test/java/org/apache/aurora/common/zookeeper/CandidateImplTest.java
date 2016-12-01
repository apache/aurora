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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

import org.apache.aurora.common.base.ExceptionalCommand;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.zookeeper.testing.BaseZooKeeperClientTest;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CandidateImplTest extends BaseZooKeeperClientTest {
  private static final List<ACL> ACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;
  private static final String SERVICE = "/twitter/services/puffin_linkhose/leader";
  private static final Amount<Integer, Time> TIMEOUT = Amount.of(1, Time.MINUTES);

  private LinkedBlockingDeque<CandidateImpl> candidateBuffer;

  @Before
  public void mySetUp() throws IOException {
    candidateBuffer = new LinkedBlockingDeque<>();
  }

  private Group createGroup(ZooKeeperClient zkClient) throws IOException {
    return new Group(zkClient, ACL, SERVICE);
  }

  private class Reign implements Candidate.Leader {
    private ExceptionalCommand<Group.JoinException> abdicate;
    private final CandidateImpl candidate;
    private final String id;
    private CountDownLatch defeated = new CountDownLatch(1);

    Reign(String id, CandidateImpl candidate) {
      this.id = id;
      this.candidate = candidate;
    }

    @Override
    public void onElected(ExceptionalCommand<Group.JoinException> abdicate) {
      candidateBuffer.offerFirst(candidate);
      this.abdicate = abdicate;
    }

    @Override
    public void onDefeated() {
      defeated.countDown();
    }

    public void abdicate() throws Group.JoinException {
      Preconditions.checkState(abdicate != null);
      abdicate.execute();
    }

    public void expectDefeated() throws InterruptedException {
      defeated.await();
    }

    @Override
    public String toString() {
      return id;
    }
  }

  @Test
  public void testOfferLeadership() throws Exception {
    ZooKeeperClient zkClient1 = createZkClient(TIMEOUT);
    final CandidateImpl candidate1 = new CandidateImpl(createGroup(zkClient1)) {
      @Override public String toString() {
        return "Leader1";
      }
    };
    ZooKeeperClient zkClient2 = createZkClient(TIMEOUT);
    final CandidateImpl candidate2 = new CandidateImpl(createGroup(zkClient2)) {
      @Override public String toString() {
        return "Leader2";
      }
    };
    ZooKeeperClient zkClient3 = createZkClient(TIMEOUT);
    final CandidateImpl candidate3 = new CandidateImpl(createGroup(zkClient3)) {
      @Override public String toString() {
        return "Leader3";
      }
    };

    Reign candidate1Reign = new Reign("1", candidate1);
    Reign candidate2Reign = new Reign("2", candidate2);
    Reign candidate3Reign = new Reign("3", candidate3);

    Supplier<Boolean> candidate1Leader = candidate1.offerLeadership(candidate1Reign);
    Supplier<Boolean> candidate2Leader = candidate2.offerLeadership(candidate2Reign);
    Supplier<Boolean> candidate3Leader = candidate3.offerLeadership(candidate3Reign);

    assertTrue("Since initial group join is synchronous, candidate 1 should be the first leader",
        candidate1Leader.get());

    shutdownNetwork();
    restartNetwork();

    assertTrue("A re-connect without a session expiration should leave the leader elected",
        candidate1Leader.get());

    candidate1Reign.abdicate();
    assertSame(candidate1, candidateBuffer.takeLast());
    assertFalse(candidate1Leader.get());
    // Active abdication should trigger defeat.
    candidate1Reign.expectDefeated();

    CandidateImpl secondCandidate = candidateBuffer.takeLast();
    assertTrue("exactly 1 remaining candidate should now be leader: " + secondCandidate + " "
               + candidateBuffer,
        candidate2Leader.get() ^ candidate3Leader.get());

    if (secondCandidate == candidate2) {
      expireSession(zkClient2);
      assertSame(candidate3, candidateBuffer.takeLast());
      assertTrue(candidate3Leader.get());
      // Passive expiration should trigger defeat.
      candidate2Reign.expectDefeated();
    } else {
      expireSession(zkClient3);
      assertSame(candidate2, candidateBuffer.takeLast());
      assertTrue(candidate2Leader.get());
      // Passive expiration should trigger defeat.
      candidate3Reign.expectDefeated();
    }
  }

  @Test
  public void testEmptyMembership() throws Exception {
    ZooKeeperClient zkClient1 = createZkClient(TIMEOUT);
    final CandidateImpl candidate1 = new CandidateImpl(createGroup(zkClient1));
    Reign candidate1Reign = new Reign("1", candidate1);

    candidate1.offerLeadership(candidate1Reign);
    assertSame(candidate1, candidateBuffer.takeLast());
    candidate1Reign.abdicate();
    assertFalse(candidate1.getLeaderData().isPresent());
  }
}
