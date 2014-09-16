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

import org.junit.Test;

import static org.apache.aurora.scheduler.updater.JobUpdateStateMachine.MonitorAction;
import static org.apache.aurora.scheduler.updater.OneWayJobUpdater.InstanceUpdateStatus;
import static org.apache.aurora.scheduler.updater.OneWayJobUpdater.OneWayStatus;
import static org.junit.Assert.assertEquals;

/**
 * Tests for auto-generated functions in enums, to achieve 100% test coverage.
 */
public class EnumsTest {

  @Test
  public void testInstanceAction() {
    for (InstanceAction value : InstanceAction.values()) {
      assertEquals(value, InstanceAction.valueOf(value.toString()));
    }
  }

  @Test
  public void testInstanceUpdateStatus() {
    for (InstanceUpdateStatus value : InstanceUpdateStatus.values()) {
      assertEquals(value, InstanceUpdateStatus.valueOf(value.toString()));
    }
  }

  @Test
  public void testOneWayStatus() {
    for (OneWayStatus value : OneWayStatus.values()) {
      assertEquals(value, OneWayStatus.valueOf(value.toString()));
    }
  }

  @Test
  public void testStateEvaluatorResult() {
    for (StateEvaluator.Result value : StateEvaluator.Result.values()) {
      assertEquals(value, StateEvaluator.Result.valueOf(value.toString()));
    }
  }

  @Test
  public void testMonitorAction() {
    for (MonitorAction value : MonitorAction.values()) {
      assertEquals(value, MonitorAction.valueOf(value.toString()));
    }
  }
}
