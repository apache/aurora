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
package org.apache.aurora.scheduler.storage;

import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.DRAINED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class AbstractAttributeStoreTest {

  private static final String HOST_A = "hostA";
  private static final String HOST_B = "hostB";
  private static final String SLAVE_A = "slaveA";
  private static final String SLAVE_B = "slaveB";
  private static final Attribute ATTR1 = new Attribute("attr1", ImmutableSet.of("a", "b", "c"));
  private static final Attribute ATTR2 = new Attribute("attr2", ImmutableSet.of("d", "e", "f"));
  private static final Attribute ATTR3 = new Attribute("attr3", ImmutableSet.of("a", "d", "g"));
  protected static final IHostAttributes HOST_A_ATTRS =
      IHostAttributes.build(new HostAttributes(HOST_A, ImmutableSet.of(ATTR1, ATTR2))
          .setSlaveId(SLAVE_A)
          .setAttributes(ImmutableSet.of())
          .setMode(MaintenanceMode.NONE));
  protected static final IHostAttributes HOST_B_ATTRS =
      IHostAttributes.build(new HostAttributes(HOST_B, ImmutableSet.of(ATTR2, ATTR3))
          .setSlaveId(SLAVE_B)
          .setAttributes(ImmutableSet.of())
          .setMode(MaintenanceMode.DRAINING));

  protected Injector injector;
  private Storage storage;

  @Before
  public void setUp() {
    injector = Guice.createInjector(getStorageModule());
    storage = injector.getInstance(Storage.class);
    storage.prepare();
  }

  protected abstract Module getStorageModule();

  @Test
  public void testSaveAttributes() {
    assertEquals(Optional.empty(), read(HOST_A));

    // Initial save returns true since it changes previous attributes
    assertTrue(insert(HOST_A_ATTRS));
    assertEquals(Optional.of(HOST_A_ATTRS), read(HOST_A));

    // Second save returns false since it does not change previous attributes
    assertFalse(insert(HOST_A_ATTRS));
  }

  @Test
  public void testCrud() {
    assertEquals(Optional.empty(), read(HOST_A));
    assertEquals(ImmutableSet.of(), readAll());

    assertTrue(insert(HOST_A_ATTRS));
    assertEquals(Optional.of(HOST_A_ATTRS), read(HOST_A));
    assertEquals(ImmutableSet.of(HOST_A_ATTRS), readAll());

    assertTrue(insert(HOST_B_ATTRS));
    assertFalse(insert(HOST_B_ATTRS));  // Double insert should be allowed.
    assertEquals(Optional.of(HOST_B_ATTRS), read(HOST_B));
    assertEquals(ImmutableSet.of(HOST_A_ATTRS, HOST_B_ATTRS), readAll());

    IHostAttributes updatedA = IHostAttributes.build(
        HOST_A_ATTRS.newBuilder().setAttributes(ImmutableSet.of(ATTR1, ATTR3)));
    assertTrue(insert(updatedA));
    assertEquals(Optional.of(updatedA), read(HOST_A));
    assertEquals(ImmutableSet.of(updatedA, HOST_B_ATTRS), readAll());

    IHostAttributes updatedMode = IHostAttributes.build(updatedA.newBuilder().setMode(DRAINED));
    assertTrue(insert(updatedMode));
    assertEquals(Optional.of(updatedMode), read(HOST_A));
    assertEquals(ImmutableSet.of(updatedMode, HOST_B_ATTRS), readAll());

    truncate();
    assertEquals(Optional.empty(), read(HOST_A));
    assertEquals(ImmutableSet.of(), readAll());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyAttributeValues() {
    IHostAttributes attributes = IHostAttributes.build(HOST_A_ATTRS.newBuilder()
        .setAttributes(ImmutableSet.of(new Attribute("attr1", ImmutableSet.of()))));
    insert(attributes);
  }

  @Test
  public void testNoAttributes() {
    IHostAttributes attributes = IHostAttributes.build(
        HOST_A_ATTRS.newBuilder().setAttributes(ImmutableSet.of()));
    assertTrue(insert(attributes));
    assertEquals(Optional.of(attributes), read(HOST_A));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNoMode() {
    HostAttributes noMode = HOST_A_ATTRS.newBuilder();
    noMode.unsetMode();

    insert(IHostAttributes.build(noMode));
  }

  @Test
  public void testSaveAttributesEmpty() {
    HostAttributes attributes = HOST_A_ATTRS.newBuilder();
    attributes.unsetAttributes();

    assertTrue(insert(IHostAttributes.build(attributes)));
    assertEquals(Optional.of(IHostAttributes.build(attributes)), read(HOST_A));
  }

  @Test
  public void testSlaveIdChanges() {
    assertTrue(insert(HOST_A_ATTRS));
    IHostAttributes updated = IHostAttributes.build(HOST_A_ATTRS.newBuilder().setSlaveId(SLAVE_B));
    assertTrue(insert(updated));
    assertEquals(Optional.of(updated), read(HOST_A));
  }

  @Test
  public void testUpdateAttributesWithRelations() {
    // Test for regression of AURORA-1379, where host attribute mutation performed a delete,
    // violating foreign key constraints.
    assertTrue(insert(HOST_A_ATTRS));

    ScheduledTask builder = TaskTestUtil.makeTask("a", JobKeys.from("role", "env", "job"))
        .newBuilder();
    builder.getAssignedTask()
        .setSlaveHost(HOST_A_ATTRS.getHost())
        .setSlaveId(HOST_A_ATTRS.getSlaveId());
    final IScheduledTask taskA = IScheduledTask.build(builder);

    storage.write((NoResult.Quiet)
        storeProvider -> storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.of(taskA)));

    HostAttributes attributeBuilder = HOST_A_ATTRS.newBuilder().setMode(DRAINED);
    attributeBuilder.addToAttributes(new Attribute("newAttr", ImmutableSet.of("a", "b")));
    IHostAttributes hostAUpdated = IHostAttributes.build(attributeBuilder);
    assertTrue(insert(hostAUpdated));
    assertEquals(Optional.of(hostAUpdated), read(HOST_A));
  }

  protected boolean insert(IHostAttributes attributes) {
    return storage.write(
        storeProvider -> storeProvider.getAttributeStore().saveHostAttributes(attributes));
  }

  private Optional<IHostAttributes> read(String host) {
    return storage.read(storeProvider -> storeProvider.getAttributeStore().getHostAttributes(host));
  }

  private Set<IHostAttributes> readAll() {
    return storage.read(storeProvider -> storeProvider.getAttributeStore().getHostAttributes());
  }

  protected void truncate() {
    storage.write(
        (NoResult.Quiet) storeProvider -> storeProvider.getAttributeStore().deleteHostAttributes());
  }
}
