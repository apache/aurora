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
package org.apache.aurora.scheduler.storage.entities;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IHostAttributesTest {

  @Test
  public void testObjectDetachment() {
    // AURORA-729: Immutable wrapper objects are not always immutable.
    Attribute attribute = new Attribute()
        .setName("attr")
        .setValues(ImmutableSet.of("a", "b", "c"));
    HostAttributes mutable = new HostAttributes()
        .setHost("a")
        .setAttributes(ImmutableSet.of(attribute));
    IHostAttributes immutable1 = IHostAttributes.build(mutable);
    IHostAttributes immutable2 = IHostAttributes.build(mutable);
    assertEquals(immutable1, immutable2);
    assertEquals(immutable2, immutable1);
    attribute.setValues(ImmutableSet.of("a"));
    assertEquals(immutable1, immutable2);
    assertEquals(immutable2, immutable1);
  }
}
