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
package org.apache.aurora.common.stats;

import org.junit.Before;
import org.junit.Test;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.testing.FakeTicker;

import static org.junit.Assert.assertEquals;

/**
 * @author William Farner
 */
public class ElapsedTest {

  private static final Amount<Long, Time> ONE_SECOND = Amount.of(1L, Time.SECONDS);

  private static final String NAME = "elapsed";

  private FakeTicker ticker;

  @Before
  public void setUp() {
    ticker = new FakeTicker();
    Stats.flush();
  }

  private Elapsed elapsed(Time granularity) {
    return new Elapsed(NAME, granularity, ticker);
  }

  @Test
  public void testTimeSince() {
    Elapsed elapsed = elapsed(Time.MILLISECONDS);
    checkValue(0);
    ticker.advance(ONE_SECOND);
    checkValue(1000);

    elapsed.reset();
    checkValue(0);

    elapsed.reset();
    ticker.advance(ONE_SECOND);
    checkValue(1000);
    ticker.advance(ONE_SECOND);
    checkValue(2000);
    ticker.advance(ONE_SECOND);
    checkValue(3000);
    ticker.advance(ONE_SECOND);
    checkValue(4000);
  }

  @Test
  public void testGranularity() {
    Elapsed elapsed = elapsed(Time.HOURS);
    checkValue(0);
    ticker.advance(Amount.of(1L, Time.DAYS));
    checkValue(24);

    elapsed.reset();
    ticker.advance(Amount.of(1L, Time.MINUTES));
    checkValue(0);
  }

  private void checkValue(long expected) {
    long actual = (Long) Stats.getVariable(NAME).read();
    assertEquals(expected, actual);
  }
}
