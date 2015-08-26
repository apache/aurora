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
package org.apache.aurora.common.testing.easymock;

import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;

public class IterableEqualsTest extends EasyMockTest {
  private static final List<Integer> TEST = ImmutableList.of(1, 2, 3, 2);
  private static final String OK = "ok";
  private Thing thing;

  public interface Thing {
    String testIterable(Iterable<Integer> input);
    String testCollection(Collection<Integer> input);
    String testList(List<Integer> input);
  }

  @Before
  public void setUp() {
    thing = createMock(Thing.class);
  }

  @Test
  public void testIterableEquals() {
    expect(thing.testIterable(IterableEquals.eqIterable(TEST))).andReturn(OK);
    control.replay();
    thing.testIterable(ImmutableList.of(3, 2, 2, 1));
  }

  @Test
  public void testCollectionEquals() {
    expect(thing.testCollection(IterableEquals.eqCollection(TEST))).andReturn(OK);
    control.replay();
    thing.testCollection(ImmutableList.of(3, 2, 2, 1));
  }

  @Test
  public void testListEquals() {
    expect(thing.testList(IterableEquals.eqList(TEST))).andReturn(OK);
    control.replay();
    thing.testList(ImmutableList.of(3, 2, 2, 1));
  }
}
