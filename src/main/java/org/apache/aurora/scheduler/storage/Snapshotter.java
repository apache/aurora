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

import java.util.stream.Stream;

import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;

/**
 * Logic to convert storage contents into a snapshot, and a snapshot into a stream of storage
 * operations.
 */
public interface Snapshotter {

  /**
   * Creates a snapshot from the contents of storage.
   *
   * @param stores stores to create a snapshot from.
   * @return A snapshot that can be used to recover storage.
   */
  Snapshot from(StoreProvider stores);

  /**
   * Converts a snapshot into an equivalent linear stream of storage operations.
   *
   * @param snapshot A snapshot created by {@link #from(StoreProvider)}.
   * @return a stream of operations representing the contents of the snapshot.
   */
  Stream<Op> asStream(Snapshot snapshot);
}
