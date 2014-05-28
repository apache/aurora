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
package org.apache.aurora.scheduler.storage.db.views;

import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;

/**
 * The union of {@link Lock} and {@link JobKey}. This class is required because the generated
 * thrift code for {@code union} fields is incompatible with the mapping behaviour of MyBatis.
 * This class works around this incompatibility by explicitly initialising an empty lock with
 * the union field set to an empty {@link JobKey} instance and exposing a getter method for MyBatis.
 * Note that this only works because the {@link LockKey} is a union of exactly one type. If LockKey
 * is modified to support more types, we will need to rework this design.
 *
 * TODO(davmclau):
 * These intermediate classes for resolving relationships on Thrift union types should be
 * auto-generated, as the logic will be identical in each one. The generated code needs to allow
 * for exactly one field in the union to be set, returning null when the getter for the field
 * is called the first time.
 */
public class LockRow {
  private final Lock lock = new Lock();

  public LockRow() {
    LockKey lockKey = new LockKey();
    lock.setKey(lockKey);
    lockKey.setJob(new JobKey());
  }

  public Lock getLock() {
    return lock;
  }
}
