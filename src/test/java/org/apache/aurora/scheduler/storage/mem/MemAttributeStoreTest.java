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
package org.apache.aurora.scheduler.storage.mem;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MemAttributeStoreTest {

  private static final IHostAttributes ATTRS = IHostAttributes.build(
      new HostAttributes()
          .setHost("hostA")
          .setSlaveId("slaveA")
          .setAttributes(ImmutableSet.of(
              makeAttribute("host", "hostA"),
              makeAttribute("rack", "rackA")))
  );

  private AttributeStore.Mutable store;

  @Before
  public void setUp() {
    store = new MemAttributeStore();
  }

  @Test
  public void testAttributeChange() {
    store.saveHostAttributes(ATTRS);
    assertEquals(Optional.of(defaultMode(ATTRS)), store.getHostAttributes(ATTRS.getHost()));
    HostAttributes builder = ATTRS.newBuilder();
    builder.addToAttributes(makeAttribute("foo", "bar"));
    IHostAttributes updated = IHostAttributes.build(builder);
    store.saveHostAttributes(updated);
    assertEquals(Optional.of(defaultMode(updated)), store.getHostAttributes(ATTRS.getHost()));
  }

  private static Attribute makeAttribute(String name, String... values) {
    return new Attribute()
        .setName(name)
        .setValues(ImmutableSet.<String>builder().add(values).build());
  }

  private static IHostAttributes defaultMode(IHostAttributes attrs) {
    return IHostAttributes.build(attrs.newBuilder().setMode(MaintenanceMode.NONE));
  }
}
