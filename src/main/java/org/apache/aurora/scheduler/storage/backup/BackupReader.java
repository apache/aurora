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
package org.apache.aurora.scheduler.storage.backup;

import java.io.File;
import java.util.stream.Stream;

import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.scheduler.storage.Snapshotter;
import org.apache.aurora.scheduler.storage.durability.Persistence;

import static java.util.Objects.requireNonNull;

/**
 * A persistence implementation to be used as a migration source.
 */
public class BackupReader implements Persistence {

  private final File backupFile;
  private final Snapshotter snapshotter;

  public BackupReader(File backupFile, Snapshotter snapshotter) {
    this.backupFile = requireNonNull(backupFile);
    this.snapshotter = requireNonNull(snapshotter);
  }

  @Override
  public Stream<Edit> recover() throws PersistenceException {
    if (!backupFile.exists()) {
      throw new PersistenceException("Backup " + backupFile + " does not exist.");
    }

    return snapshotter.asStream(Recovery.load(backupFile)).map(Edit::op);
  }

  @Override
  public void prepare() {
    // no-op
  }

  @Override
  public void persist(Stream<Op> records) {
    throw new UnsupportedOperationException("Backups are read-only");
  }
}
