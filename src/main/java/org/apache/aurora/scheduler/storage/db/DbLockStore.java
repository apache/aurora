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

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.google.inject.Inject;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.db.views.LockRow;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;

import static java.util.Objects.requireNonNull;

import static com.twitter.common.inject.TimedInterceptor.Timed;

/**
 * A relational database-backed lock store.
 */
class DbLockStore implements LockStore.Mutable {

  private final LockMapper mapper;
  private final LockKeyMapper lockKeyMapper;

  @Inject
  DbLockStore(LockMapper mapper, LockKeyMapper lockKeyMapper) {
    this.mapper = requireNonNull(mapper);
    this.lockKeyMapper = requireNonNull(lockKeyMapper);
  }

  @Timed("lock_store_save_lock")
  @Override
  public void saveLock(ILock lock) {
    lockKeyMapper.insert(lock.getKey().newBuilder());
    mapper.insert(lock.newBuilder());
  }

  @Timed("lock_store_remove_lock")
  @Override
  public void removeLock(ILockKey lockKey) {
    mapper.delete(lockKey.newBuilder());
  }

  @Timed("lock_store_delete_locks")
  @Override
  public void deleteLocks() {
    mapper.truncate();
  }

  @Timed("lock_store_fetch_locks")
  @Override
  public Set<ILock> fetchLocks() {
    return mapper.selectAll().stream().map(TO_ROW).collect(GuavaUtils.toImmutableSet());
  }

  @Timed("lock_store_fetch_lock")
  @Override
  public Optional<ILock> fetchLock(ILockKey lockKey) {
    return Optional.ofNullable(mapper.select(lockKey.newBuilder())).map(TO_ROW);
  }

  /**
   * LockRow converter to satisfy the ILock interface.
   */
  private static final Function<LockRow, ILock> TO_ROW = input -> ILock.build(input.getLock());
}
