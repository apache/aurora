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
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractScheduledService;

import org.apache.ibatis.exceptions.PersistenceException;
import org.mybatis.guice.transactional.Transactional;

import static java.util.Objects.requireNonNull;

/**
 * A periodic cleanup routine for unreferenced database relations.
 */
class RowGarbageCollector extends AbstractScheduledService {

  private static final Logger LOG = Logger.getLogger(RowGarbageCollector.class.getName());

  private final Scheduler iterationScheduler;
  private final List<GarbageCollectedTableMapper> tables;

  @Inject
  RowGarbageCollector(
      Scheduler iterationScheduler,
      TaskConfigMapper taskConfigMapper,
      JobKeyMapper jobKeyMapper) {

    this.iterationScheduler = requireNonNull(iterationScheduler);
    this.tables = ImmutableList.of(taskConfigMapper, jobKeyMapper);
  }

  @Override
  protected Scheduler scheduler() {
    return iterationScheduler;
  }

  @VisibleForTesting
  @Transactional
  @Override
  public void runOneIteration() {
    // Note: ordering of table scans is important here to remove 'parent' references first.
    LOG.info("Scanning database tables for unreferenced rows.");

    long deletedCount = 0;
    for (GarbageCollectedTableMapper table : tables) {
      for (long rowId : table.selectAllRowIds()) {
        try {
          table.deleteRow(rowId);
          deletedCount++;
        } catch (PersistenceException e) {
          // Expected for rows that are still referenced.
        }
      }
    }
    LOG.info("Deleted " + deletedCount + " unreferenced rows.");
  }
}
