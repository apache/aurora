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
package org.apache.aurora.scheduler.updater.strategy;

import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import org.junit.Test;

import static com.google.common.collect.ImmutableSet.of;

import static org.junit.Assert.assertEquals;

public class VariableBatchStrategyTest {

  private static final Ordering<Integer> ORDERING = Ordering.natural();
  private static final Set<Integer> EMPTY = of();

  @Test(expected = IllegalArgumentException.class)
  public void testBadParameter() {
    new VariableBatchStrategy<>(ORDERING, ImmutableList.of(0), true);
  }

  @Test
  public void testNoWorkToDo() {
    UpdateStrategy<Integer> strategy = new VariableBatchStrategy<>(ORDERING,
        ImmutableList.of(2),
        true);
    assertEquals(EMPTY, strategy.getNextGroup(EMPTY, of(0, 1)));
    assertEquals(EMPTY, strategy.getNextGroup(EMPTY, EMPTY));
  }

  @Test
  public void testVariableBatchCompletion() {
    UpdateStrategy<Integer> strategy = new VariableBatchStrategy<>(ORDERING,
        ImmutableList.of(2),
        true);
    assertEquals(EMPTY, strategy.getNextGroup(of(2, 3), of(0, 1)));
    assertEquals(EMPTY, strategy.getNextGroup(of(2, 3), of(1)));
    assertEquals(of(2, 3), strategy.getNextGroup(of(2, 3), EMPTY));
  }

  @Test
  public void testBatchesIgnoreInstanceValues() {
    // Batches are defined as groups of instances, not partitioned based on the instance ID values.
    UpdateStrategy<Integer> strategy = new VariableBatchStrategy<>(ORDERING,
        ImmutableList.of(2),
        true);
    assertEquals(of(0, 1), strategy.getNextGroup(of(0, 1, 2, 3), EMPTY));
    assertEquals(of(1, 2), strategy.getNextGroup(of(1, 2, 3), EMPTY));
    assertEquals(of(2, 3), strategy.getNextGroup(of(2, 3), EMPTY));
    assertEquals(of(3, 8), strategy.getNextGroup(of(3, 8), EMPTY));
  }

  @Test
  public void testExhausted() {
    UpdateStrategy<Integer> strategy = new VariableBatchStrategy<>(ORDERING,
        ImmutableList.of(3),
        true);
    assertEquals(of(0, 1, 2), strategy.getNextGroup(of(0, 1, 2), EMPTY));
    assertEquals(of(0, 1), strategy.getNextGroup(of(0, 1), EMPTY));
    assertEquals(of(1), strategy.getNextGroup(of(1), EMPTY));
  }

  @Test
  public void testActiveTooLarge() {
    UpdateStrategy<Integer> strategy = new VariableBatchStrategy<>(ORDERING,
        ImmutableList.of(2),
        true);
    assertEquals(EMPTY, strategy.getNextGroup(of(0, 1, 2), of(3, 4, 5)));
  }

  @Test
  public void testIncreasingGroupSizes() {
    UpdateStrategy<Integer> strategy = new VariableBatchStrategy<>(ORDERING,
        ImmutableList.of(1, 2, 3),
        true);
    assertEquals(of(0), strategy.getNextGroup(of(0, 1, 2, 3, 4, 5), EMPTY));
    assertEquals(of(1, 2), strategy.getNextGroup(of(1, 2, 3, 4, 5), EMPTY));
    assertEquals(of(3, 4, 5), strategy.getNextGroup(of(3, 4, 5), EMPTY));
  }

  @Test
  public void testDecreasingGroupSizes() {
    UpdateStrategy<Integer> strategy = new VariableBatchStrategy<>(ORDERING,
        ImmutableList.of(3, 2, 1),
        true);
    assertEquals(of(0, 1, 2), strategy.getNextGroup(of(0, 1, 2, 3, 4, 5), EMPTY));
    assertEquals(of(3, 4), strategy.getNextGroup(of(3, 4, 5), EMPTY));
    assertEquals(of(5), strategy.getNextGroup(of(5), EMPTY));
  }

  @Test
  public void testSeeSawGroupSizes() {
    UpdateStrategy<Integer> strategy = new VariableBatchStrategy<>(ORDERING,
        ImmutableList.of(1, 3, 2, 4),
        true);
    assertEquals(of(0), strategy.getNextGroup(of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), EMPTY));
    assertEquals(of(1, 2, 3), strategy.getNextGroup(of(1, 2, 3, 4, 5, 6, 7, 8, 9), EMPTY));
    assertEquals(of(4, 5), strategy.getNextGroup(of(4, 5, 6, 7, 8, 9), EMPTY));
    assertEquals(of(6, 7, 8, 9), strategy.getNextGroup(of(6, 7, 8, 9), EMPTY));
  }

  @Test
  public void testMoreInstancesThanSumOfGroupSizes() {
    UpdateStrategy<Integer> strategy = new VariableBatchStrategy<>(ORDERING,
        ImmutableList.of(1, 2),
        true);
    assertEquals(of(0), strategy.getNextGroup(of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), EMPTY));
    assertEquals(of(1, 2), strategy.getNextGroup(of(1, 2, 3, 4, 5, 6, 7, 8, 9), EMPTY));
    assertEquals(of(3, 4), strategy.getNextGroup(of(3, 4, 5, 6, 7, 8, 9), EMPTY));
    assertEquals(of(5, 6), strategy.getNextGroup(of(5, 6, 7, 8, 9), EMPTY));
    assertEquals(of(7, 8), strategy.getNextGroup(of(7, 8, 9), EMPTY));
    assertEquals(of(9), strategy.getNextGroup(of(9), EMPTY));
  }

  @Test
  public void testRollback() {
    UpdateStrategy<Integer> strategy = new VariableBatchStrategy<>(ORDERING.reverse(),
        ImmutableList.of(1, 2, 3),
        false);
    assertEquals(of(3, 4, 5), strategy.getNextGroup(of(0, 1, 2, 3, 4, 5), EMPTY));
    assertEquals(of(1, 2), strategy.getNextGroup(of(0, 1, 2), EMPTY));
    assertEquals(of(0), strategy.getNextGroup(of(0), EMPTY));
  }

  @Test
  public void testRollbackMidWay() {
    UpdateStrategy<Integer> strategy = new VariableBatchStrategy<>(ORDERING.reverse(),
        ImmutableList.of(1, 2, 3),
        false);
    assertEquals(of(1, 2), strategy.getNextGroup(of(0, 1, 2), EMPTY));
    assertEquals(of(0), strategy.getNextGroup(of(0), EMPTY));
  }

  @Test
  public void testRollbackOverflow() {
    UpdateStrategy<Integer> strategy = new VariableBatchStrategy<>(ORDERING.reverse(),
        ImmutableList.of(1, 2),
        false);
    assertEquals(of(5, 6), strategy.getNextGroup(of(0, 1, 2, 3, 4, 5, 6), EMPTY));
    assertEquals(of(4, 3), strategy.getNextGroup(of(0, 1, 2, 3, 4), EMPTY));
    assertEquals(of(2, 1), strategy.getNextGroup(of(0, 1, 2), EMPTY));
    assertEquals(of(0), strategy.getNextGroup(of(0), EMPTY));
  }

}
