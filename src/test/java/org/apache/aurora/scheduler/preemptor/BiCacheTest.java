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
package org.apache.aurora.scheduler.preemptor;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.scheduler.preemptor.BiCache.BiCacheSettings;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BiCacheTest {
  private static final Amount<Long, Time> HOLD_DURATION = Amount.of(1L, Time.MINUTES);
  private static final String STAT_NAME = "cache_size_stat";
  private static final String KEY_1 = "Key 1";
  private static final String KEY_2 = "Key 2";
  private static final Optional<Integer> NO_VALUE = Optional.absent();

  private FakeStatsProvider statsProvider;
  private FakeClock clock;
  private BiCache<String, Integer> biCache;

  @Before
  public void setUp() {
    statsProvider = new FakeStatsProvider();
    clock = new FakeClock();
    biCache = new BiCache<>(statsProvider, new BiCacheSettings(HOLD_DURATION, STAT_NAME), clock);
  }

  @Test
  public void testExpiration() {
    biCache.put(KEY_1, 1);
    assertEquals(Optional.of(1), biCache.get(KEY_1));
    assertEquals(1L, statsProvider.getLongValue(STAT_NAME));

    clock.advance(HOLD_DURATION);

    assertEquals(NO_VALUE, biCache.get(KEY_1));
    assertEquals(ImmutableSet.of(), biCache.getByValue(1));
    assertEquals(0L, statsProvider.getLongValue(STAT_NAME));
  }

  @Test
  public void testRemoval() {
    biCache.put(KEY_1, 1);
    assertEquals(1L, statsProvider.getLongValue(STAT_NAME));
    assertEquals(Optional.of(1), biCache.get(KEY_1));
    biCache.remove(KEY_1, 1);
    assertEquals(NO_VALUE, biCache.get(KEY_1));
    assertEquals(0L, statsProvider.getLongValue(STAT_NAME));
  }

  @Test(expected = NullPointerException.class)
  public void testRemovalWithNullKey() {
    biCache.remove(null, 1);
  }

  @Test
  public void testDifferentKeysIdenticalValues() {
    biCache.put(KEY_1, 1);
    biCache.put(KEY_2, 1);
    assertEquals(2L, statsProvider.getLongValue(STAT_NAME));

    assertEquals(Optional.of(1), biCache.get(KEY_1));
    assertEquals(Optional.of(1), biCache.get(KEY_2));
    assertEquals(ImmutableSet.of(KEY_1, KEY_2), biCache.getByValue(1));

    biCache.remove(KEY_1, 1);
    assertEquals(NO_VALUE, biCache.get(KEY_1));
    assertEquals(Optional.of(1), biCache.get(KEY_2));
    assertEquals(ImmutableSet.of(KEY_2), biCache.getByValue(1));
    assertEquals(1L, statsProvider.getLongValue(STAT_NAME));

    clock.advance(HOLD_DURATION);
    assertEquals(NO_VALUE, biCache.get(KEY_1));
    assertEquals(NO_VALUE, biCache.get(KEY_2));
    assertEquals(ImmutableSet.of(), biCache.getByValue(1));
    assertEquals(0L, statsProvider.getLongValue(STAT_NAME));
  }

  @Test
  public void testIdenticalKeysDifferentValues() {
    biCache.put(KEY_1, 1);
    biCache.put(KEY_1, 2);
    assertEquals(Optional.of(2), biCache.get(KEY_1));
    assertEquals(ImmutableSet.of(), biCache.getByValue(1));
    assertEquals(ImmutableSet.of(KEY_1), biCache.getByValue(2));
    assertEquals(1L, statsProvider.getLongValue(STAT_NAME));
  }
}
