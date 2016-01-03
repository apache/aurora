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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.storage.CallOrderEnforcingStorage;
import org.apache.aurora.scheduler.storage.DistributedSnapshotStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.log.LogManager.MaxEntrySize;
import org.apache.aurora.scheduler.storage.log.LogStorage.Settings;

import static org.apache.aurora.scheduler.storage.log.EntrySerializer.EntrySerializerImpl;
import static org.apache.aurora.scheduler.storage.log.LogManager.LogEntryHashFunction;
import static org.apache.aurora.scheduler.storage.log.SnapshotDeduplicator.SnapshotDeduplicatorImpl;

/**
 * Bindings for scheduler distributed log based storage.
 */
public class LogStorageModule extends PrivateModule {

  @CmdLine(name = "dlog_shutdown_grace_period",
           help = "Specifies the maximum time to wait for scheduled checkpoint and snapshot "
                  + "actions to complete before forcibly shutting down.")
  private static final Arg<Amount<Long, Time>> SHUTDOWN_GRACE_PERIOD =
      Arg.create(Amount.of(2L, Time.SECONDS));

  @CmdLine(name = "dlog_snapshot_interval",
           help = "Specifies the frequency at which snapshots of local storage are taken and "
                  + "written to the log.")
  private static final Arg<Amount<Long, Time>> SNAPSHOT_INTERVAL =
      Arg.create(Amount.of(1L, Time.HOURS));

  @CmdLine(name = "dlog_max_entry_size",
           help = "Specifies the maximum entry size to append to the log. Larger entries will be "
                  + "split across entry Frames.")
  @VisibleForTesting
  public static final Arg<Amount<Integer, Data>> MAX_LOG_ENTRY_SIZE =
      Arg.create(Amount.of(512, Data.KB));

  @Override
  protected void configure() {
    bind(Settings.class)
        .toInstance(new Settings(SHUTDOWN_GRACE_PERIOD.get(), SNAPSHOT_INTERVAL.get()));

    bind(new TypeLiteral<Amount<Integer, Data>>() { }).annotatedWith(MaxEntrySize.class)
        .toInstance(MAX_LOG_ENTRY_SIZE.get());
    bind(LogManager.class).in(Singleton.class);
    bind(LogStorage.class).in(Singleton.class);

    install(CallOrderEnforcingStorage.wrappingModule(LogStorage.class));
    bind(DistributedSnapshotStore.class).to(LogStorage.class);
    expose(Storage.class);
    expose(NonVolatileStorage.class);
    expose(DistributedSnapshotStore.class);

    bind(EntrySerializer.class).to(EntrySerializerImpl.class);
    // TODO(ksweeney): We don't need a cryptographic checksum here - assess performance of MD5
    // versus a faster error-detection checksum like CRC32 for large Snapshots.
    bind(HashFunction.class).annotatedWith(LogEntryHashFunction.class).toInstance(Hashing.md5());

    bind(SnapshotDeduplicator.class).to(SnapshotDeduplicatorImpl.class);

    install(new FactoryModuleBuilder()
        .implement(StreamManager.class, StreamManagerImpl.class)
        .build(StreamManagerFactory.class));
  }
}
