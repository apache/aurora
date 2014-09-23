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

import java.lang.annotation.Annotation;

import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;

import org.apache.aurora.scheduler.log.Log;
import org.apache.aurora.scheduler.storage.CallOrderEnforcingStorage;
import org.apache.aurora.scheduler.storage.DistributedSnapshotStore;
import org.apache.aurora.scheduler.storage.log.LogManager.MaxEntrySize;
import org.apache.aurora.scheduler.storage.log.LogStorage.ShutdownGracePeriod;
import org.apache.aurora.scheduler.storage.log.LogStorage.SnapshotInterval;

import static org.apache.aurora.scheduler.storage.log.EntrySerializer.EntrySerializerImpl;

/**
 * Bindings for scheduler distributed log based storage.
 * <p/>
 * Requires bindings for:
 * <ul>
 *   <li>{@link Clock}</li>
 *   <li>{@link ShutdownRegistry}</li>
 *   <li>The concrete {@link Log} implementation.</li>
 * </ul>
 * <p/>
 */
public class LogStorageModule extends AbstractModule {

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

  @CmdLine(name = "deflate_snapshots", help = "Whether snapshots should be deflate-compressed.")
  private static final Arg<Boolean> DEFLATE_SNAPSHOTS = Arg.create(true);

  @Override
  protected void configure() {
    requireBinding(Log.class);
    requireBinding(Clock.class);
    requireBinding(ShutdownRegistry.class);

    bindInterval(ShutdownGracePeriod.class, SHUTDOWN_GRACE_PERIOD);
    bindInterval(SnapshotInterval.class, SNAPSHOT_INTERVAL);

    bind(new TypeLiteral<Amount<Integer, Data>>() { }).annotatedWith(MaxEntrySize.class)
        .toInstance(MAX_LOG_ENTRY_SIZE.get());
    bind(LogManager.class).in(Singleton.class);
    bindConstant().annotatedWith(LogManager.DeflateSnapshots.class).to(DEFLATE_SNAPSHOTS.get());
    bind(LogStorage.class).in(Singleton.class);

    install(CallOrderEnforcingStorage.wrappingModule(LogStorage.class));
    bind(DistributedSnapshotStore.class).to(LogStorage.class);

    bind(EntrySerializer.class).to(EntrySerializerImpl.class);
    // TODO(ksweeney): We don't need a cryptographic checksum here - assess performance of MD5
    // versus a faster error-detection checksum like CRC32 for large Snapshots.
    bind(HashFunction.class).annotatedWith(LogManager.LogEntryHashFunction.class)
        .toInstance(Hashing.md5());

    install(new FactoryModuleBuilder()
        .implement(StreamManager.class, StreamManagerImpl.class)
        .build(StreamManagerFactory.class));

  }

  private void bindInterval(Class<? extends Annotation> key, Arg<Amount<Long, Time>> value) {
    bind(Key.get(new TypeLiteral<Amount<Long, Time>>() { }, key)).toInstance(value.get());
  }
}
