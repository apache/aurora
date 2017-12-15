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

import java.io.IOException;
import java.util.Optional;

import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public abstract class AbstractSchedulerStoreTest {

  private Storage storage;

  @Before
  public void setUp() throws IOException {
    storage = createStorage();
  }

  protected abstract Storage createStorage();

  @Test
  public void testSchedulerStore() {
    assertEquals(Optional.empty(), select());
    save("a");
    assertEquals(Optional.of("a"), select());
    save("b");
    assertEquals(Optional.of("b"), select());
  }

  private void save(String id) {
    storage.write(
        (NoResult.Quiet) storeProvider -> storeProvider.getSchedulerStore().saveFrameworkId(id));
  }

  private Optional<String> select() {
    return storage.read(storeProvider -> storeProvider.getSchedulerStore().fetchFrameworkId());
  }
}
