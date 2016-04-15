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
package org.apache.aurora.common.base;

import java.util.function.Consumer;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.fail;

/**
 * @author John Sirois
 */
public class ConsumersTest extends EasyMockTest {

  private static final Clazz<Consumer<Integer>> INT_CLOSURE_CLZ = new Clazz<Consumer<Integer>>() { };
  static class Thrown extends RuntimeException { }

  @Test
  public void testCombine() {
    Consumer<Integer> work1 = createMock(INT_CLOSURE_CLZ);
    Consumer<Integer> work2 = createMock(INT_CLOSURE_CLZ);

    @SuppressWarnings("unchecked") // Needed because type information lost in vargs.
        Consumer<Integer> wrapper = Consumers.combine(ImmutableList.of(work1, work2));

    work1.accept(1);
    work2.accept(1);

    work1.accept(2);
    work2.accept(2);

    control.replay();

    wrapper.accept(1);
    wrapper.accept(2);
  }

  @Test
  public void testCombineOneThrows() {
    Consumer<Integer> work1 = createMock(INT_CLOSURE_CLZ);
    Consumer<Integer> work2 = createMock(INT_CLOSURE_CLZ);
    Consumer<Integer> work3 = createMock(INT_CLOSURE_CLZ);

    @SuppressWarnings("unchecked") // Needed because type information lost in vargs.
        Consumer<Integer> wrapper = Consumers.combine(ImmutableList.of(work1, work2, work3));

    work1.accept(1);
    expectLastCall().andThrow(new Thrown());

    work1.accept(2);
    work2.accept(2);
    expectLastCall().andThrow(new Thrown());

    work1.accept(3);
    work2.accept(3);
    work3.accept(3);
    expectLastCall().andThrow(new Thrown());

    control.replay();

    try {
      wrapper.accept(1);
      fail("Should have thrown.");
    } catch (Thrown e) {
      // Expected.
    }

    try {
      wrapper.accept(2);
      fail("Should have thrown.");
    } catch (Thrown e) {
      // Expected.
    }

    try {
      wrapper.accept(3);
      fail("Should have thrown.");
    } catch (Thrown e) {
      // Expected.
    }
  }

  @Test
  public void testFilter() {
    Predicate<Integer> filter = createMock(new Clazz<Predicate<Integer>>() { });
    Consumer<Integer> work = createMock(INT_CLOSURE_CLZ);

    expect(filter.apply(1)).andReturn(true);
    work.accept(1);

    expect(filter.apply(2)).andReturn(false);

    Consumer<Integer> filtered = Consumers.filter(filter, work);

    control.replay();

    filtered.accept(1);
    filtered.accept(2);
  }
}
