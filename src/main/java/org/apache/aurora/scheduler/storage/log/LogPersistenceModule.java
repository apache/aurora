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
package org.apache.aurora.scheduler.storage.log;

import javax.inject.Singleton;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.scheduler.config.types.DataAmount;
import org.apache.aurora.scheduler.storage.durability.Persistence;
import org.apache.aurora.scheduler.storage.log.EntrySerializer.EntrySerializerImpl;
import org.apache.aurora.scheduler.storage.log.LogManager.LogEntryHashFunction;
import org.apache.aurora.scheduler.storage.log.LogManager.MaxEntrySize;
import org.apache.aurora.scheduler.storage.log.SnapshotDeduplicator.SnapshotDeduplicatorImpl;

/**
 * Bindings for scheduler distributed log based persistence.
 */
public class LogPersistenceModule extends PrivateModule {

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-dlog_max_entry_size",
        description =
            "Specifies the maximum entry size to append to the log. Larger entries will be "
                + "split across entry Frames.")
    public DataAmount maxLogEntrySize = new DataAmount(512, Data.KB);
  }

  private final Options options;

  public LogPersistenceModule(Options options) {
    this.options = options;
  }

  @Override
  protected void configure() {
    bind(new TypeLiteral<Amount<Integer, Data>>() { }).annotatedWith(MaxEntrySize.class)
        .toInstance(options.maxLogEntrySize);
    bind(LogManager.class).in(Singleton.class);
    bind(LogPersistence.class).in(Singleton.class);
    bind(Persistence.class).to(LogPersistence.class);
    expose(Persistence.class);
    expose(LogPersistence.class);

    bind(EntrySerializer.class).to(EntrySerializerImpl.class);
    // TODO(ksweeney): We don't need a cryptographic checksum here - assess performance of MD5
    // versus a faster error-detection checksum like CRC32 for large Snapshots.
    @SuppressWarnings("deprecation")
    HashFunction hashFunction = Hashing.md5();
    bind(HashFunction.class).annotatedWith(LogEntryHashFunction.class).toInstance(hashFunction);

    bind(SnapshotDeduplicator.class).to(SnapshotDeduplicatorImpl.class);

    install(new FactoryModuleBuilder()
        .implement(StreamManager.class, StreamManagerImpl.class)
        .build(StreamManagerFactory.class));
  }
}
