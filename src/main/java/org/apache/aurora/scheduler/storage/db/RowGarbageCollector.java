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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractScheduledService;

import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import static java.util.Objects.requireNonNull;

/**
 * A periodic cleanup routine for unreferenced database relations.
 */
class RowGarbageCollector extends AbstractScheduledService {

  private static final Logger LOG = Logger.getLogger(RowGarbageCollector.class.getName());

  // Note: these are deliberately ordered to remove 'parent' references first, but since
  // this is an iterative process, it is not strictly necessary.
  private static final List<Class<? extends GarbageCollectedTableMapper>> TABLES =
      ImmutableList.of(TaskConfigMapper.class, JobKeyMapper.class);

  private final Scheduler iterationScheduler;
  private final SqlSessionFactory sessionFactory;

  // Note: Storage is only used to acquire the same application-level lock used by other storage
  // mutations.  This sidesteps the issue of DB deadlocks (e.g. AURORA-1401).
  private final Storage storage;

  @Inject
  RowGarbageCollector(
      Scheduler iterationScheduler,
      SqlSessionFactory sessionFactory,
      Storage storage) {

    this.iterationScheduler = requireNonNull(iterationScheduler);
    this.sessionFactory = requireNonNull(sessionFactory);
    this.storage = requireNonNull(storage);
  }

  @Override
  protected Scheduler scheduler() {
    return iterationScheduler;
  }

  @VisibleForTesting
  @Override
  public void runOneIteration() {
    LOG.info("Scanning database tables for unreferenced rows.");

    final AtomicLong deletedCount = new AtomicLong();
    for (Class<? extends GarbageCollectedTableMapper> tableClass : TABLES) {
      storage.write((NoResult.Quiet) (Storage.MutableStoreProvider storeProvider) -> {
        try (SqlSession session = sessionFactory.openSession(true)) {
          GarbageCollectedTableMapper table = session.getMapper(tableClass);
          for (long rowId : table.selectAllRowIds()) {
            try {
              table.deleteRow(rowId);
              deletedCount.incrementAndGet();
            } catch (PersistenceException e) {
              // Expected for rows that are still referenced.
            }
          }
        }
      });
    }
    LOG.info("Deleted " + deletedCount.get() + " unreferenced rows.");
  }
}
