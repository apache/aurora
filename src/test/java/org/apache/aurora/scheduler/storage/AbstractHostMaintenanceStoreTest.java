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

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.CountSlaPolicy;
import org.apache.aurora.gen.HostMaintenanceRequest;
import org.apache.aurora.gen.PercentageSlaPolicy;
import org.apache.aurora.gen.SlaPolicy;
import org.apache.aurora.scheduler.storage.entities.IHostMaintenanceRequest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class AbstractHostMaintenanceStoreTest {

  private static final String HOST_A = "hostA";
  private static final String HOST_B = "hostB";

  private static final SlaPolicy PERCENTAGE_SLA_POLICY = SlaPolicy.percentageSlaPolicy(
      new PercentageSlaPolicy()
        .setPercentage(95)
        .setDurationSecs(1800)
  );

  private static final SlaPolicy COUNT_SLA_POLICY = SlaPolicy.countSlaPolicy(
      new CountSlaPolicy()
          .setCount(3)
          .setDurationSecs(1800)
  );

  private static final IHostMaintenanceRequest HOST_A_MAINTENANCE_REQUEST =
      IHostMaintenanceRequest.build(new HostMaintenanceRequest()
          .setHost(HOST_A)
          .setDefaultSlaPolicy(PERCENTAGE_SLA_POLICY)
          .setCreatedTimestampMs(0)
          .setTimeoutSecs(1800)
      );

  private static final IHostMaintenanceRequest HOST_B_MAINTENANCE_REQUEST =
      IHostMaintenanceRequest.build(new HostMaintenanceRequest()
          .setHost(HOST_B)
          .setDefaultSlaPolicy(COUNT_SLA_POLICY)
          .setCreatedTimestampMs(0)
          .setTimeoutSecs(1800)
      );

  private Storage storage;

  @Before
  public void setUp() {
    storage = createStorage();
  }

  protected abstract Storage createStorage();

  @Test
  public void testReadHostMaintenanceRequestNonExistant() {
    assertEquals(Optional.empty(), read(HOST_A));
  }

  @Test
  public void testReadHostMaintenanceRequest() {
    assertEquals(Optional.empty(), read(HOST_A));

    insert(HOST_A_MAINTENANCE_REQUEST);
    assertEquals(Optional.of(HOST_A_MAINTENANCE_REQUEST), read(HOST_A));
  }

  @Test
  public void testReadAllHostMaintenanceRequest() {
    assertEquals(Collections.emptySet(), readAll());

    insert(HOST_A_MAINTENANCE_REQUEST);
    assertEquals(ImmutableSet.of(HOST_A_MAINTENANCE_REQUEST), readAll());

    insert(HOST_B_MAINTENANCE_REQUEST);
    assertEquals(
        ImmutableSet.of(HOST_A_MAINTENANCE_REQUEST, HOST_B_MAINTENANCE_REQUEST), readAll());
  }

  @Test
  public void testSaveHostMaintenanceRequest() {
    insert(HOST_A_MAINTENANCE_REQUEST);
    assertEquals(Optional.of(HOST_A_MAINTENANCE_REQUEST), read(HOST_A));
  }

  @Test
  public void testSaveHostMaintenanceRequestUpdates() {
    insert(HOST_A_MAINTENANCE_REQUEST);
    insert(HOST_B_MAINTENANCE_REQUEST);
    assertEquals(
        ImmutableSet.of(HOST_A_MAINTENANCE_REQUEST, HOST_B_MAINTENANCE_REQUEST), readAll());

    IHostMaintenanceRequest updatedA = IHostMaintenanceRequest.build(
        HOST_A_MAINTENANCE_REQUEST.newBuilder().setTimeoutSecs(0));

    insert(updatedA);
    assertEquals(
        ImmutableSet.of(updatedA, HOST_B_MAINTENANCE_REQUEST), readAll());
  }

  @Test
  public void testRemoveHostMaintenanceRequestNotExistant() {
    assertEquals(Collections.emptySet(), readAll());

    assertEquals(Optional.empty(), read(HOST_A));
  }

  @Test
  public void testRemoveHostMaintenanceRequest() {
    assertEquals(Optional.empty(), read(HOST_A));

    insert(HOST_A_MAINTENANCE_REQUEST);
    assertEquals(Optional.of(HOST_A_MAINTENANCE_REQUEST), read(HOST_A));

    truncate(HOST_A);
    assertEquals(Optional.empty(), read(HOST_A));
  }

  private void insert(IHostMaintenanceRequest hostMaintenanceRequest) {
    storage.write(
        store -> {
          store.getHostMaintenanceStore().saveHostMaintenanceRequest(hostMaintenanceRequest);
          return null;
        });
  }

  private Optional<IHostMaintenanceRequest> read(String host) {
    return storage.read(store -> store.getHostMaintenanceStore().getHostMaintenanceRequest(host));
  }

  private Set<IHostMaintenanceRequest> readAll() {
    return storage.read(store -> store.getHostMaintenanceStore().getHostMaintenanceRequests());
  }

  private void truncate(String host) {
    storage.write(
        (Storage.MutateWork.NoResult.Quiet) store -> store
            .getHostMaintenanceStore()
            .removeHostMaintenanceRequest(host));
  }
}
