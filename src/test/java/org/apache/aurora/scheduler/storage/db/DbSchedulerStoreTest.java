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

import com.google.common.base.Optional;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.twitter.common.inject.Bindings;

import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DbSchedulerStoreTest {

  private Storage storage;

  @Before
  public void setUp() throws IOException {
    Injector injector = Guice.createInjector(new DbModule(Bindings.KeyFactory.PLAIN));
    storage = injector.getInstance(Storage.class);
    storage.prepare();
  }

  @Test
  public void testSchedulerStore() {
    assertEquals(Optional.<String>absent(), select());
    save("a");
    assertEquals(Optional.of("a"), select());
    save("b");
    assertEquals(Optional.of("b"), select());
  }

  private void save(final String id) {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getSchedulerStore().saveFrameworkId(id);
      }
    });
  }

  private Optional<String> select() {
    return storage.consistentRead(new Work.Quiet<Optional<String>>() {
      @Override
      public Optional<String> apply(StoreProvider storeProvider) {
        return storeProvider.getSchedulerStore().fetchFrameworkId();
      }
    });
  }
}
