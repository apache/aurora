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

import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.inject.Inject;

import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.db.views.LockRow;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;

import static java.util.Objects.requireNonNull;

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

  @Override
  public void saveLock(ILock lock) {
    lockKeyMapper.insert(lock.getKey().newBuilder());
    mapper.insert(lock.newBuilder());
  }

  @Override
  public void removeLock(ILockKey lockKey) {
    mapper.delete(lockKey.newBuilder());
  }

  @Override
  public void deleteLocks() {
    mapper.truncate();
  }

  /**
   * LockRow converter to satisfy the ILock interface.
   */
  private static final Function<LockRow, ILock> TO_ROW = new Function<LockRow, ILock>() {
    @Override
    public ILock apply(LockRow input) {
      return ILock.build(input.getLock());
    }
  };

  @Override
  public Set<ILock> fetchLocks() {
    return FluentIterable.from(mapper.selectAll()).transform(TO_ROW).toSet();
  }

  @Override
  public Optional<ILock> fetchLock(ILockKey lockKey) {
    return Optional.fromNullable(mapper.select(lockKey.newBuilder())).transform(TO_ROW);
  }
}
