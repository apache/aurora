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
package org.apache.aurora.scheduler.app.local;

import javax.inject.Inject;

import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult.Quiet;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;

/**
 * A storage system that implements non-volatile storage operations, but is actually volatile.
 */
public class FakeNonVolatileStorage implements NonVolatileStorage {
  private final Storage delegate;

  @Inject
  FakeNonVolatileStorage(Storage delegate) {
    this.delegate = delegate;
  }

  @Override
  public void stop() {
    // No-op.
  }

  @Override
  public void start(Quiet initializationLogic) throws StorageException {
    // No-op.
  }

  @Override
  public void prepare() throws StorageException {
    delegate.prepare();
  }

  @Override
  public <T, E extends Exception> T write(MutateWork<T, E> work) throws StorageException, E {
    return delegate.write(work);
  }

  @Override
  public <T, E extends Exception> T read(Work<T, E> work) throws StorageException, E {
    return delegate.read(work);
  }
}
