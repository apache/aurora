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
package org.apache.aurora.scheduler.updater;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.twitter.common.collections.Pair;

import org.apache.aurora.gen.JobUpdateStatus;
import org.junit.Test;

import static org.apache.aurora.gen.JobUpdateStatus.ABORTED;
import static org.apache.aurora.gen.JobUpdateStatus.ERROR;
import static org.apache.aurora.gen.JobUpdateStatus.FAILED;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_FORWARD;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_FORWARD;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_BACK_AWAITING_PULSE;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_BACK_PAUSED;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_FORWARD_AWAITING_PULSE;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_FORWARD_PAUSED;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction.ROLL_BACK;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction.ROLL_FORWARD;
import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction.STOP_WATCHING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JobUpdateStateMachineTest {

  private static final Map<Pair<JobUpdateStatus, JobUpdateStatus>, MonitorAction> EXPECTED =
      ImmutableMap.<Pair<JobUpdateStatus, JobUpdateStatus>, MonitorAction>builder()
          .put(Pair.of(ROLLING_FORWARD, ROLLING_BACK), ROLL_BACK)
          .put(Pair.of(ROLLING_FORWARD, ROLL_FORWARD_PAUSED), STOP_WATCHING)
          .put(Pair.of(ROLLING_FORWARD, ROLL_FORWARD_AWAITING_PULSE), STOP_WATCHING)
          .put(Pair.of(ROLLING_FORWARD, ROLLED_FORWARD), STOP_WATCHING)
          .put(Pair.of(ROLLING_FORWARD, ABORTED), STOP_WATCHING)
          .put(Pair.of(ROLLING_FORWARD, ERROR), STOP_WATCHING)
          .put(Pair.of(ROLLING_FORWARD, FAILED), STOP_WATCHING)
          .put(Pair.of(ROLLING_BACK, ROLL_BACK_PAUSED), STOP_WATCHING)
          .put(Pair.of(ROLLING_BACK, ROLL_BACK_AWAITING_PULSE), STOP_WATCHING)
          .put(Pair.of(ROLLING_BACK, ROLLED_BACK), STOP_WATCHING)
          .put(Pair.of(ROLLING_BACK, ABORTED), STOP_WATCHING)
          .put(Pair.of(ROLLING_BACK, ERROR), STOP_WATCHING)
          .put(Pair.of(ROLLING_BACK, FAILED), STOP_WATCHING)
          .put(Pair.of(ROLL_FORWARD_PAUSED, ROLLING_FORWARD), ROLL_FORWARD)
          .put(Pair.of(ROLL_FORWARD_PAUSED, ROLL_FORWARD_AWAITING_PULSE), STOP_WATCHING)
          .put(Pair.of(ROLL_FORWARD_PAUSED, ABORTED), STOP_WATCHING)
          .put(Pair.of(ROLL_FORWARD_PAUSED, ERROR), STOP_WATCHING)
          .put(Pair.of(ROLL_BACK_PAUSED, ROLLING_BACK), ROLL_BACK)
          .put(Pair.of(ROLL_BACK_PAUSED, ROLL_BACK_AWAITING_PULSE), STOP_WATCHING)
          .put(Pair.of(ROLL_BACK_PAUSED, ABORTED), STOP_WATCHING)
          .put(Pair.of(ROLL_BACK_PAUSED, ERROR), STOP_WATCHING)
          .put(Pair.of(ROLL_FORWARD_AWAITING_PULSE, ROLLING_FORWARD), ROLL_FORWARD)
          .put(Pair.of(ROLL_FORWARD_AWAITING_PULSE, ROLL_FORWARD_PAUSED), STOP_WATCHING)
          .put(Pair.of(ROLL_FORWARD_AWAITING_PULSE, ABORTED), STOP_WATCHING)
          .put(Pair.of(ROLL_FORWARD_AWAITING_PULSE, ERROR), STOP_WATCHING)
          .put(Pair.of(ROLL_BACK_AWAITING_PULSE, ROLLING_BACK), ROLL_BACK)
          .put(Pair.of(ROLL_BACK_AWAITING_PULSE, ROLL_BACK_PAUSED), STOP_WATCHING)
          .put(Pair.of(ROLL_BACK_AWAITING_PULSE, ABORTED), STOP_WATCHING)
          .put(Pair.of(ROLL_BACK_AWAITING_PULSE, ERROR), STOP_WATCHING)
          .build();

  @Test
  public void testTransition() {
    for (JobUpdateStatus from : JobUpdateStatus.values()) {
      for (JobUpdateStatus to : JobUpdateStatus.values()) {
        Pair<JobUpdateStatus, JobUpdateStatus> key = Pair.of(from, to);
        MonitorAction expected = EXPECTED.get(key);
        try {
          JobUpdateStateMachine.assertTransitionAllowed(from, to);
          MonitorAction actual = JobUpdateStateMachine.getActionForStatus(to);
          if (expected == null) {
            fail("Transition " + key + " should have been disallowed, but got result " + actual);
          }
          assertEquals("Failed transition " + key, expected, actual);
        } catch (UpdateStateException e) {
          if (expected != null) {
            fail("Expected " + expected + " for transition " + key
                + " but the transition was disallowed: " + e);
          }
        }
      }
    }
  }
}
