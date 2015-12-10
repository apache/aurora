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
import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.testing.StorageEntityUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DbQuotaStoreTest {

  private static final String ROLE_A = "roleA";
  private static final String ROLE_B = "roleB";
  private static final IResourceAggregate QUOTA_A =
      IResourceAggregate.build(new ResourceAggregate(1.0D, 2, 3));
  private static final IResourceAggregate QUOTA_B =
      IResourceAggregate.build(new ResourceAggregate(2.0D, 4, 6));

  private Storage storage;

  @Before
  public void setUp() throws IOException {
    storage = DbUtil.createStorage();
  }

  @Test
  public void testCrud() {
    assertEquals(Optional.absent(), select(ROLE_A));
    assertQuotas(ImmutableMap.of());

    save(ROLE_A, StorageEntityUtil.assertFullyPopulated(QUOTA_A));
    save(ROLE_B, QUOTA_B);

    assertEquals(Optional.of(QUOTA_A), select(ROLE_A));
    assertEquals(Optional.of(QUOTA_B), select(ROLE_B));
    assertQuotas(ImmutableMap.of(ROLE_A, QUOTA_A, ROLE_B, QUOTA_B));

    delete(ROLE_B);
    assertEquals(Optional.of(QUOTA_A), select(ROLE_A));
    assertEquals(Optional.absent(), select(ROLE_B));
    assertQuotas(ImmutableMap.of(ROLE_A, QUOTA_A));

    deleteAll();
    assertEquals(Optional.absent(), select(ROLE_A));
    assertEquals(Optional.absent(), select(ROLE_B));
    assertQuotas(ImmutableMap.of());
  }

  @Test
  public void testDeleteNonExistent() {
    assertEquals(Optional.absent(), select(ROLE_A));
    assertQuotas(ImmutableMap.of());
    delete(ROLE_A);
    assertEquals(Optional.absent(), select(ROLE_A));
    assertQuotas(ImmutableMap.of());
  }

  @Test
  public void testUpsert() {
    save(ROLE_A, QUOTA_A);
    save(ROLE_A, QUOTA_B);
    assertEquals(Optional.of(QUOTA_B), select(ROLE_A));
    assertQuotas(ImmutableMap.of(ROLE_A, QUOTA_B));
  }

  private void save(String role, IResourceAggregate quota) {
    storage.write(
        (NoResult.Quiet) storeProvider -> storeProvider.getQuotaStore().saveQuota(role, quota));
  }

  private Optional<IResourceAggregate> select(String role) {
    return storage.read(storeProvider -> storeProvider.getQuotaStore().fetchQuota(role));
  }

  private void assertQuotas(Map<String, IResourceAggregate> quotas) {
    assertEquals(
        quotas,
        storage.read(storeProvider -> storeProvider.getQuotaStore().fetchQuotas())
    );
  }

  private void delete(String role) {
    storage.write(
        (NoResult.Quiet) storeProvider -> storeProvider.getQuotaStore().removeQuota(role));
  }

  private void deleteAll() {
    storage.write((NoResult.Quiet) storeProvider -> storeProvider.getQuotaStore().deleteQuotas());
  }
}
