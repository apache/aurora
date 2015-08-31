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

import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;

/**
 * @author William Farner
 */
public class StatsTest {

  @After
  public void tearDown() {
    Stats.flush();
  }

  @Test
  public void testSimpleExport() {
    AtomicLong var = Stats.exportLong("test_long");
    assertCounter("test_long", 0);
    var.incrementAndGet();
    assertCounter("test_long", 1);
    var.addAndGet(100);
    assertCounter("test_long", 101);
  }

  @Test
  public void testSlashesSupported() {
    AtomicLong slash = Stats.exportLong("d/f");
    slash.incrementAndGet();
    assertCounter("d/f", 1);
  }

  @Test
  public void testNotSame() {
    AtomicLong firstExport = Stats.exportLong("somevar");
    firstExport.incrementAndGet();
    firstExport.incrementAndGet();
    assertCounter("somevar", 2L);
    AtomicLong secondExport = Stats.exportLong("somevar");
    assertNotSame(firstExport, secondExport);
    secondExport.incrementAndGet();
    assertCounter("somevar", 2L); // We keep the first one!
  }

  @Test
  public void testNormalizesSpace() {
    AtomicLong leading = Stats.exportLong("  leading space");
    AtomicLong trailing = Stats.exportLong("trailing space   ");
    AtomicLong surround = Stats.exportLong("   surround space   ");

    leading.incrementAndGet();
    trailing.incrementAndGet();
    surround.incrementAndGet();
    assertCounter("__leading_space", 1);
    assertCounter("trailing_space___", 1);
    assertCounter("___surround_space___", 1);
  }

  @Test
  public void testNormalizesIllegalChars() {
    AtomicLong colon = Stats.exportLong("a:b");
    AtomicLong plus = Stats.exportLong("b+c");
    AtomicLong hyphen = Stats.exportLong("c-d");

    colon.incrementAndGet();
    plus.incrementAndGet();
    hyphen.incrementAndGet();
    assertCounter("a_b", 1);
    assertCounter("b_c", 1);
    assertCounter("c_d", 1);
  }

  private void assertCounter(String name, long value) {
    assertThat(Stats.<Long>getVariable(name).read(), is(value));
  }

  private void assertCounter(String name, double value) {
    Double var = (Double) Stats.getVariable(name).read();
    assertEquals(var, value, 1e-6);
  }
}

