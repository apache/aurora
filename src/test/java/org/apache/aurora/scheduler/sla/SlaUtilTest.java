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
package org.apache.aurora.scheduler.sla;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class SlaUtilTest {

  private List<Long> samples;

  @Test
  public void testPercentileEmpty() {
    samples = new LinkedList<>();
    Number actual = SlaUtil.percentile(samples, 75);
    assertEquals(0.0, actual);

    actual = SlaUtil.percentile(samples, 99);
    assertEquals(0.0, actual);

    actual = SlaUtil.percentile(samples, 99.9);
    assertEquals(0.0, actual);

    actual = SlaUtil.percentile(samples, 100);
    assertEquals(0.0, actual);

    actual = SlaUtil.percentile(samples, 0);
    assertEquals(0.0, actual);
  }

  @Test
  public void testPercentileSingleValue() {
    samples = new LinkedList<>(Collections.singletonList(10L));
    Number actual = SlaUtil.percentile(samples, 75);
    assertEquals(10.0, actual);

    actual = SlaUtil.percentile(samples, 99);
    assertEquals(10.0, actual);

    actual = SlaUtil.percentile(samples, 99.9);
    assertEquals(10.0, actual);

    actual = SlaUtil.percentile(samples, 100);
    assertEquals(10.0, actual);

    actual = SlaUtil.percentile(samples, 0);
    assertEquals(10.0, actual);
  }

  @Test
  public void testPercentileConstant() {
    samples = new LinkedList<>();
    for (int i = 0; i < 100; i++) {
      samples.add(10L);
    }
    Number actual = SlaUtil.percentile(samples, 75);
    assertEquals(10.0, actual);

    actual = SlaUtil.percentile(samples, 99);
    assertEquals(10.0, actual);

    actual = SlaUtil.percentile(samples, 99.9);
    assertEquals(10.0, actual);

    actual = SlaUtil.percentile(samples, 100);
    assertEquals(10.0, actual);

    actual = SlaUtil.percentile(samples, 0);
    assertEquals(10.0, actual);
  }

  @Test
  public void testPercentileLinearEven() {
    samples = new LinkedList<>();
    for (int i = 0; i < 100; i += 4) {
      samples.add((long) i);
    }
    samples.add(100L);

    assertSame(samples.size() % 2, 0);

    Number actual = SlaUtil.percentile(samples, 50);
    assertEquals(50.0, actual);

    actual = SlaUtil.percentile(samples, 75);
    assertEquals(25.0, actual);

    actual = SlaUtil.percentile(samples, 99);
    assertEquals(1.0, actual);

    actual = SlaUtil.percentile(samples, 99.9);
    assertEquals(0.1, actual);

    actual = SlaUtil.percentile(samples, 100);
    assertEquals(0.0, actual);

    actual = SlaUtil.percentile(samples, 0);
    assertEquals(100.0, actual);
  }

  @Test
  public void testPercentileLinearOdd() {
    samples = new LinkedList<>();
    for (int i = 0; i <= 100; i++) {
      samples.add((long) i);
    }

    assertSame(samples.size() % 2, 1);

    Number actual = SlaUtil.percentile(samples, 50);
    assertEquals(50.0, actual);

    actual = SlaUtil.percentile(samples, 75);
    assertEquals(25.0, actual);

    actual = SlaUtil.percentile(samples, 99);
    assertEquals(1.0, actual);

    actual = SlaUtil.percentile(samples, 99.9);
    assertEquals(0.1, actual);

    actual = SlaUtil.percentile(samples, 100);
    assertEquals(0.0, actual);

    actual = SlaUtil.percentile(samples, 0);
    assertEquals(100.0, actual);
  }

  @Test
  public void testPercentileInterpolate() {
    samples = new LinkedList<>(Arrays.asList(30L, 70L, 90L, 60L));
    Number actual = SlaUtil.percentile(samples, 75);
    assertEquals(52.5, actual);

    actual = SlaUtil.percentile(samples, 99);
    assertEquals(30.9, actual);

    actual = SlaUtil.percentile(samples, 99.9);
    assertEquals(30.09, actual);

    actual = SlaUtil.percentile(samples, 100);
    assertEquals(30.0, actual);

    actual = SlaUtil.percentile(samples, 0);
    assertEquals(90.0, actual);
  }
}
