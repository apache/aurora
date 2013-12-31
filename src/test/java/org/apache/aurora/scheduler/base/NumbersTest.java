/**
 * Copyright 2013 Apache Software Foundation
 *
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
package org.apache.aurora.scheduler.base;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NumbersTest {

  @Test
  public void testToRanges() {
    assertEquals(ImmutableSet.<Range<Integer>>of(), Numbers.toRanges(ImmutableList.<Integer>of()));
    assertEquals(ImmutableSet.of(Range.closed(1, 1)), Numbers.toRanges(ImmutableList.of(1)));
    assertEquals(ImmutableSet.of(Range.closed(0, 3)),
        Numbers.toRanges(ImmutableList.of(0, 1, 2, 3)));
    assertEquals(ImmutableSet.of(Range.closed(0, 3), Range.closed(5, 5)),
        Numbers.toRanges(ImmutableList.of(0, 1, 2, 3, 5)));
    assertEquals(
        ImmutableSet.of(
            Range.closed(0, 1),
            Range.closed(5, 6),
            Range.closed(9, 9),
            Range.closed(100, 100)),
        Numbers.toRanges(ImmutableList.of(0, 1, 5, 6, 9, 100)));
  }
}
