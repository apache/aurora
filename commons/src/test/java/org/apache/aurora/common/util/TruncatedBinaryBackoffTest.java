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
package org.apache.aurora.common.util;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author John Sirois
 */
public class TruncatedBinaryBackoffTest {
  @Test(expected = NullPointerException.class)
  public void testNullInitialBackoffRejected() {
    new TruncatedBinaryBackoff(null, Amount.of(1L, Time.SECONDS));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testZeroInitialBackoffRejected() {
    new TruncatedBinaryBackoff(Amount.of(0L, Time.SECONDS), Amount.of(1L, Time.SECONDS));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeInitialBackoffRejected() {
    new TruncatedBinaryBackoff(Amount.of(-1L, Time.SECONDS), Amount.of(1L, Time.SECONDS));
  }

  @Test(expected = NullPointerException.class)
  public void testNullMaximumBackoffRejected() {
    new TruncatedBinaryBackoff(Amount.of(1L, Time.SECONDS), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMaximumBackoffLessThanInitialBackoffRejected() {
    new TruncatedBinaryBackoff(Amount.of(2L, Time.SECONDS), Amount.of(1L, Time.SECONDS));
  }

  @Test
  public void testCalculateBackoffMs() {
    TruncatedBinaryBackoff backoff =
        new TruncatedBinaryBackoff(Amount.of(1L, Time.MILLISECONDS),
            Amount.of(12L, Time.MILLISECONDS));

    try {
      backoff.calculateBackoffMs(-1L);
      fail("calculateBackoffMs should throw an exception when give a negative value.");
    } catch (IllegalArgumentException e) {
      // expected
    }

    long calculateBackoffMs0 = backoff.calculateBackoffMs(0);
    assertTrue(1 <= calculateBackoffMs0 && calculateBackoffMs0 <= 2);

    long calculateBackoffMs1 = backoff.calculateBackoffMs(1);
    assertTrue(1 <= calculateBackoffMs1 && calculateBackoffMs1 <= 2);

    long calculateBackoffMs2 = backoff.calculateBackoffMs(2);
    assertTrue(2 <= calculateBackoffMs2 && calculateBackoffMs2 <= 4);

    long calculateBackoffMs4 = backoff.calculateBackoffMs(4);
    assertTrue(4 <= calculateBackoffMs4 && calculateBackoffMs4 <= 8);

    long calculateBackoffMs8 = backoff.calculateBackoffMs(8);
    assertTrue(8 <= calculateBackoffMs8 && calculateBackoffMs8 <= 12);

    assertEquals(12, backoff.calculateBackoffMs(16));
  }

  @Test
  public void testShouldContinue() {
    TruncatedBinaryBackoff backoff =
        new TruncatedBinaryBackoff(Amount.of(1L, Time.MILLISECONDS),
            Amount.of(6L, Time.MILLISECONDS), true);

    assertTrue(backoff.shouldContinue(0));
    assertTrue(backoff.shouldContinue(1));
    assertTrue(backoff.shouldContinue(2));
    assertTrue(backoff.shouldContinue(4));
    assertFalse(backoff.shouldContinue(6));
    assertFalse(backoff.shouldContinue(8));
  }
}
