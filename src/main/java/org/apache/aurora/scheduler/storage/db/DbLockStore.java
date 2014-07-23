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
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.inject.Inject;

import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.db.views.LockRow;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.apache.ibatis.exceptions.PersistenceException;

import static java.util.Objects.requireNonNull;

/**
 * A relational database-backed lock store.
 */
class DbLockStore implements LockStore.Mutable {

  private static final Logger LOG = Logger.getLogger(DbLockStore.class.getName());

  private final LockMapper mapper;
  private final LockKeyMapper lockKeyMapper;

  @Inject
  DbLockStore(LockMapper mapper, LockKeyMapper lockKeyMapper) {
    this.mapper = requireNonNull(mapper);
    this.lockKeyMapper = requireNonNull(lockKeyMapper);
  }

  @Override
  public void saveLock(ILock lock) {
    try {
      lockKeyMapper.insert(lock.getKey().newBuilder());
    } catch (PersistenceException e) {
      LOG.fine("DB write error for key: " + lock.getKey());
      // TODO(davmclau): We purposely swallow duplicate key exceptions here
      // but we should verify it _is_ a duplicate key error so we can
      // give better logging for unexpected errors. That is
      // made tricky by this: https://code.google.com/p/mybatis/issues/detail?id=249
      // It is currently harmless to let this fall through, as if the
      // write failed and key doesn't exist, the next write will fail anyway.
    }
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
