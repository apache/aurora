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
package org.apache.aurora.scheduler.storage.db;

import java.io.IOException;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.MaintenanceMode.DRAINED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DbAttributeStoreTest {

  private static final String HOST_A = "hostA";
  private static final String HOST_B = "hostB";
  private static final String SLAVE_A = "slaveA";
  private static final String SLAVE_B = "slaveB";
  private static final Attribute ATTR1 = new Attribute("attr1", ImmutableSet.of("a", "b", "c"));
  private static final Attribute ATTR2 = new Attribute("attr2", ImmutableSet.of("d", "e", "f"));
  private static final Attribute ATTR3 = new Attribute("attr3", ImmutableSet.of("a", "d", "g"));
  private static final IHostAttributes HOST_A_ATTRS =
      IHostAttributes.build(new HostAttributes(HOST_A, ImmutableSet.of(ATTR1, ATTR2))
          .setSlaveId(SLAVE_A)
          .setMode(MaintenanceMode.NONE));
  private static final IHostAttributes HOST_B_ATTRS =
      IHostAttributes.build(new HostAttributes(HOST_B, ImmutableSet.of(ATTR2, ATTR3))
          .setSlaveId(SLAVE_B)
          .setMode(MaintenanceMode.DRAINING));

  private Storage storage;

  @Before
  public void setUp() throws IOException {
    storage = DbUtil.createStorage();
  }

  @Test
  public void testCrud() {
    assertEquals(Optional.<IHostAttributes>absent(), read(HOST_A));
    assertEquals(ImmutableSet.<IHostAttributes>of(), readAll());

    insert(HOST_A_ATTRS);
    assertEquals(Optional.of(HOST_A_ATTRS), read(HOST_A));
    assertEquals(ImmutableSet.of(HOST_A_ATTRS), readAll());

    insert(HOST_B_ATTRS);
    insert(HOST_B_ATTRS);  // Double insert should be allowed.
    assertEquals(Optional.of(HOST_B_ATTRS), read(HOST_B));
    assertEquals(ImmutableSet.of(HOST_A_ATTRS, HOST_B_ATTRS), readAll());

    IHostAttributes updatedA = IHostAttributes.build(
        HOST_A_ATTRS.newBuilder().setAttributes(ImmutableSet.of(ATTR1, ATTR3)));
    insert(updatedA);
    assertEquals(Optional.of(updatedA), read(HOST_A));
    assertEquals(ImmutableSet.of(updatedA, HOST_B_ATTRS), readAll());

    truncate();
    assertEquals(Optional.<IHostAttributes>absent(), read(HOST_A));
    assertEquals(ImmutableSet.<IHostAttributes>of(), readAll());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyAttributeValues() {
    IHostAttributes attributes = IHostAttributes.build(HOST_A_ATTRS.newBuilder()
        .setAttributes(ImmutableSet.of(new Attribute("attr1", ImmutableSet.<String>of()))));
    insert(attributes);
  }

  @Test
  public void testNoAttributes() {
    IHostAttributes attributes = IHostAttributes.build(
        HOST_A_ATTRS.newBuilder().setAttributes(ImmutableSet.<Attribute>of()));
    insert(attributes);
    assertEquals(Optional.of(attributes), read(HOST_A));
  }

  @Test
  public void testSetMaintenanceMode() {
    HostAttributes noMode = HOST_A_ATTRS.newBuilder();
    noMode.unsetMode();

    insert(IHostAttributes.build(noMode));
    // Default mode NONE should be automatically applied.
    assertEquals(Optional.of(HOST_A_ATTRS), read(HOST_A));

    IHostAttributes updatedA = IHostAttributes.build(noMode.deepCopy().setMode(DRAINED));
    // Inserting the updated value should ignore the mode.
    insert(updatedA);
    assertEquals(Optional.of(HOST_A_ATTRS), read(HOST_A));

    // Instead, the mode must be explicitly set to be changed.
    assertTrue(setMode(HOST_A, DRAINED));
    assertEquals(Optional.of(updatedA), read(HOST_A));

    assertFalse(setMode(HOST_B, DRAINED));
  }

  @Test
  public void testSaveAttributesNotSet() {
    HostAttributes attributes = HOST_A_ATTRS.newBuilder();
    attributes.unsetAttributes();

    insert(IHostAttributes.build(attributes));
    assertEquals(
        Optional.of(IHostAttributes.build(attributes.setAttributes(ImmutableSet.<Attribute>of()))),
        read(HOST_A));
  }

  @Test
  public void testSlaveIdChanges() {
    insert(HOST_A_ATTRS);
    IHostAttributes updated = IHostAttributes.build(HOST_A_ATTRS.newBuilder().setSlaveId(SLAVE_B));
    insert(updated);
    assertEquals(Optional.of(updated), read(HOST_A));
  }

  private void insert(final IHostAttributes attributes) {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getAttributeStore().saveHostAttributes(attributes);
      }
    });
  }

  private boolean setMode(final String host, final MaintenanceMode mode) {
    return storage.write(new MutateWork.Quiet<Boolean>() {
      @Override
      public Boolean apply(MutableStoreProvider storeProvider) {
        return storeProvider.getAttributeStore().setMaintenanceMode(host, mode);
      }
    });
  }

  private Optional<IHostAttributes> read(final String host) {
    return storage.consistentRead(new Work.Quiet<Optional<IHostAttributes>>() {
      @Override
      public Optional<IHostAttributes> apply(StoreProvider storeProvider) {
        return storeProvider.getAttributeStore().getHostAttributes(host);
      }
    });
  }

  private Set<IHostAttributes> readAll() {
    return storage.consistentRead(new Work.Quiet<Set<IHostAttributes>>() {
      @Override
      public Set<IHostAttributes> apply(StoreProvider storeProvider) {
        return storeProvider.getAttributeStore().getHostAttributes();
      }
    });
  }

  private void truncate() {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getAttributeStore().deleteHostAttributes();
      }
    });
  }
}
