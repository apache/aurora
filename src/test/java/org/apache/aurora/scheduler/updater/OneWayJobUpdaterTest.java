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
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.scheduler.updater.strategy.UpdateStrategy;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.updater.OneWayJobUpdater.EvaluationResult;
import static org.apache.aurora.scheduler.updater.OneWayJobUpdater.InstanceAction;
import static org.apache.aurora.scheduler.updater.OneWayJobUpdater.OneWayStatus;
import static org.apache.aurora.scheduler.updater.StateEvaluator.Result;
import static org.apache.aurora.scheduler.updater.StateEvaluator.Result.EVALUATE_AFTER_RUNNING_LIMIT;
import static org.apache.aurora.scheduler.updater.StateEvaluator.Result.EVALUATE_ON_STATE_CHANGE;
import static org.apache.aurora.scheduler.updater.StateEvaluator.Result.FAILED;
import static org.apache.aurora.scheduler.updater.StateEvaluator.Result.KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE;
import static org.apache.aurora.scheduler.updater.StateEvaluator.Result.REPLACE_TASK_AND_EVALUATE_ON_STATE_CHANGE;
import static org.apache.aurora.scheduler.updater.StateEvaluator.Result.SUCCEEDED;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class OneWayJobUpdaterTest extends EasyMockTest {
  private static final Set<Integer> EMPTY = ImmutableSet.of();
  private static final Map<Integer, InstanceAction> NO_ACTIONS = ImmutableMap.of();

  private UpdateStrategy<Integer> strategy;
  private StateEvaluator<String> instance0;
  private StateEvaluator<String> instance1;
  private StateEvaluator<String> instance2;
  private StateEvaluator<String> instance3;
  private Map<Integer, StateEvaluator<String>> allInstances;
  private InstanceStateProvider<Integer, String> stateProvider;

  private OneWayJobUpdater<Integer, String> jobUpdater;

  @Before
  public void setUp() {
    strategy = createMock(new Clazz<UpdateStrategy<Integer>>() { });
    instance0 = createMock(new Clazz<StateEvaluator<String>>() { });
    instance1 = createMock(new Clazz<StateEvaluator<String>>() { });
    instance2 = createMock(new Clazz<StateEvaluator<String>>() { });
    instance3 = createMock(new Clazz<StateEvaluator<String>>() { });
    allInstances = ImmutableMap.of(
        0, instance0,
        1, instance1,
        2, instance2,
        3, instance3);
    stateProvider = createMock(new Clazz<InstanceStateProvider<Integer, String>>() { });
  }

  private void evaluate(OneWayStatus expectedStatus, Map<Integer, InstanceAction> expectedActions) {
    assertEquals(
        new EvaluationResult<>(expectedStatus, expectedActions),
        jobUpdater.evaluate(ImmutableSet.<Integer>of(), stateProvider));
  }

  private void evaluate(
      int instanceId,
      OneWayStatus expectedStatus,
      Map<Integer, InstanceAction> expectedActions) {

    assertEquals(
        new EvaluationResult<>(expectedStatus, expectedActions),
        jobUpdater.evaluate(ImmutableSet.of(instanceId), stateProvider));
  }

  private void expectEvaluate(
      int instanceId,
      StateEvaluator<String> instanceMock,
      String state,
      Result result) {

    expect(stateProvider.getState(instanceId)).andReturn(state);
    expect(instanceMock.evaluate(state)).andReturn(result);
  }

  @Test
  public void testSuccessfulUpdate() {
    expect(strategy.getNextGroup(ImmutableSet.of(0, 1, 2, 3), EMPTY))
        .andReturn(ImmutableSet.of(0, 2));
    String s0 = "0";
    String s1 = "1";
    String s2 = "2";
    String s3 = "3";
    expectEvaluate(
        0,
        instance0,
        s0,
        KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE);
    expectEvaluate(
        2,
        instance2,
        s2,
        REPLACE_TASK_AND_EVALUATE_ON_STATE_CHANGE);

    expectEvaluate(0, instance0, s0, EVALUATE_ON_STATE_CHANGE);
    expect(strategy.getNextGroup(ImmutableSet.of(1, 3), ImmutableSet.of(0, 2))).andReturn(EMPTY);
    expectEvaluate(0, instance0, s0, SUCCEEDED);
    expect(strategy.getNextGroup(ImmutableSet.of(1, 3), ImmutableSet.of(2))).andReturn(EMPTY);
    expectEvaluate(2, instance2, s2, SUCCEEDED);
    expect(strategy.getNextGroup(ImmutableSet.of(1, 3), EMPTY))
        .andReturn(ImmutableSet.of(1, 3));
    expectEvaluate(
        1,
        instance1,
        s1,
        SUCCEEDED);
    expectEvaluate(
        3,
        instance3,
        s3,
        EVALUATE_AFTER_RUNNING_LIMIT);
    expectEvaluate(3, instance3, s3, SUCCEEDED);

    control.replay();

    jobUpdater = new OneWayJobUpdater<>(strategy, 0, allInstances);

    evaluate(
        OneWayStatus.WORKING,
        ImmutableMap.of(
            0, InstanceAction.KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE,
            2, InstanceAction.REPLACE_TASK_AND_EVALUATE_ON_STATE_CHANGE));
    evaluate(
        0,
        OneWayStatus.WORKING,
        ImmutableMap.of(0, InstanceAction.EVALUATE_ON_STATE_CHANGE));
    evaluate(
        0,
        OneWayStatus.WORKING,
        NO_ACTIONS);
    evaluate(
        2,
        OneWayStatus.WORKING,
        ImmutableMap.of(
            3, InstanceAction.EVALUATE_AFTER_RUNNING_LIMIT));
    evaluate(
        3,
        OneWayStatus.SUCCEEDED,
        NO_ACTIONS);
  }

  @Test
  public void testFailedUpdate() {
    expect(strategy.getNextGroup(ImmutableSet.of(0, 1, 2, 3), EMPTY))
        .andReturn(ImmutableSet.of(0, 1));
    String s0 = "0";
    String s1 = "1";
    expectEvaluate(
        0,
        instance0,
        s0,
        FAILED);
    expectEvaluate(
        1,
        instance1,
        s1,
        KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE);

    control.replay();

    jobUpdater = new OneWayJobUpdater<>(strategy, 0, allInstances);

    evaluate(
        OneWayStatus.FAILED,
        ImmutableMap.of(
            1, InstanceAction.KILL_TASK_AND_EVALUATE_ON_STATE_CHANGE));

    // The updater should now reject further attempts to evaluate.
    try {
      jobUpdater.evaluate(ImmutableSet.<Integer>of(), stateProvider);
      fail();
    } catch (IllegalStateException e) {
      // Expected.
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadInput() {
    control.replay();

    new OneWayJobUpdater<>(strategy, 0, ImmutableMap.<Integer, StateEvaluator<String>>of());
  }

  @Test
  public void testEvaluateCompletedInstance() {
    expect(strategy.getNextGroup(ImmutableSet.of(0, 1, 2, 3), EMPTY))
        .andReturn(ImmutableSet.of(0));
    expect(strategy.getNextGroup(ImmutableSet.of(1, 2, 3), EMPTY))
        .andReturn(ImmutableSet.<Integer>of());
    String s0 = "0";
    expectEvaluate(
        0,
        instance0,
        s0,
        SUCCEEDED);

    control.replay();

    jobUpdater = new OneWayJobUpdater<>(strategy, 0, allInstances);

    evaluate(
        OneWayStatus.WORKING,
        NO_ACTIONS);

    // Instance 0 is already considered finished, so any further notifications of its state will
    // no-op.
    evaluate(
        0,
        OneWayStatus.WORKING,
        NO_ACTIONS);
  }
}
