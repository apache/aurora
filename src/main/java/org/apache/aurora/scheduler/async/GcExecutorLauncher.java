/**
 * Copyright 2013 Apache Software Foundation
 *
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
package org.apache.aurora.scheduler.async;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;
import com.twitter.common.util.Random;

import org.apache.aurora.Protobufs;
import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.comm.AdjustRetainedTasks;
import org.apache.aurora.scheduler.Driver;
import org.apache.aurora.scheduler.TaskLauncher;
import org.apache.aurora.scheduler.base.CommandUtil;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A task launcher that periodically initiates garbage collection on a host, re-using a single
 * garbage collection executor.
 */
public class GcExecutorLauncher implements TaskLauncher {
  private static final Logger LOG = Logger.getLogger(GcExecutorLauncher.class.getName());

  private final AtomicLong tasksCreated = Stats.exportLong("scheduler_gc_tasks_created");
  private final AtomicLong offersConsumed = Stats.exportLong("scheduler_gc_offers_consumed");

  @VisibleForTesting
  static final Resources TOTAL_GC_EXECUTOR_RESOURCES =
      new Resources(0.2, Amount.of(128L, Data.MB), Amount.of(16L, Data.MB), 0);

  // An epsilon is used because we are required to supply executor and task resources.
  @VisibleForTesting
  static final Resources EPSILON =
      new Resources(0.01, Amount.of(1L, Data.MB), Amount.of(1L, Data.MB), 0);

  private static final Resources GC_EXECUTOR_TASK_RESOURCES =
      Resources.subtract(TOTAL_GC_EXECUTOR_RESOURCES, EPSILON);

  @VisibleForTesting
  static final String SYSTEM_TASK_PREFIX = "system-gc-";
  private static final String EXECUTOR_NAME = "aurora.gc";

  private final GcExecutorSettings settings;
  private final Storage storage;
  private final Clock clock;
  private final Executor executor;
  private final Driver driver;
  private final Supplier<String> uuidGenerator;
  private final Cache<String, Long> pulses;

  @Inject
  GcExecutorLauncher(
      GcExecutorSettings settings,
      Storage storage,
      Clock clock,
      Executor executor,
      Driver driver) {

    this(
        settings,
        storage,
        clock,
        executor,
        driver,
        new Supplier<String>() {
          @Override
          public String get() {
            return UUID.randomUUID().toString();
          }
        });
  }

  @VisibleForTesting
  GcExecutorLauncher(
      GcExecutorLauncher.GcExecutorSettings settings,
      Storage storage,
      Clock clock,
      Executor executor,
      Driver driver,
      Supplier<String> uuidGenerator) {

    this.settings = checkNotNull(settings);
    this.storage = checkNotNull(storage);
    this.clock = checkNotNull(clock);
    this.executor = checkNotNull(executor);
    this.driver = checkNotNull(driver);
    this.uuidGenerator = checkNotNull(uuidGenerator);
    this.pulses = CacheBuilder.newBuilder()
        .expireAfterWrite(settings.getMaxGcInterval(), TimeUnit.MILLISECONDS)
        .build();
  }

  @VisibleForTesting
  TaskInfo makeGcTask(
      String sourceName,
      SlaveID slaveId,
      AdjustRetainedTasks message) {

    ExecutorInfo.Builder executorInfo = ExecutorInfo.newBuilder()
        .setExecutorId(ExecutorID.newBuilder().setValue(EXECUTOR_NAME))
        .setName(EXECUTOR_NAME)
        .setSource(sourceName)
        .addAllResources(GC_EXECUTOR_TASK_RESOURCES.toResourceList())
        .setCommand(CommandUtil.create(settings.getGcExecutorPath().get()));

    byte[] data;
    try {
      data = ThriftBinaryCodec.encode(message);
    } catch (CodingException e) {
      LOG.severe("Failed to encode retained tasks message: " + message);
      throw Throwables.propagate(e);
    }

    return TaskInfo.newBuilder().setName("system-gc")
        .setTaskId(TaskID.newBuilder().setValue(SYSTEM_TASK_PREFIX + uuidGenerator.get()))
        .setSlaveId(slaveId)
        .setData(ByteString.copyFrom(data))
        .setExecutor(executorInfo)
        .addAllResources(EPSILON.toResourceList())
        .build();
  }

  private TaskInfo makeGcTask(String hostName, SlaveID slaveId) {
    Set<IScheduledTask> tasksOnHost =
        Storage.Util.weaklyConsistentFetchTasks(storage, Query.slaveScoped(hostName));

    Map<String, ScheduleStatus> tasks = Maps.filterValues(
        Maps.transformValues(Tasks.mapById(tasksOnHost), Tasks.GET_STATUS),
        Predicates.not(Predicates.equalTo(ScheduleStatus.SANDBOX_DELETED)));
    tasksCreated.incrementAndGet();
    return makeGcTask(hostName, slaveId, new AdjustRetainedTasks().setRetainedTasks(tasks));
  }

  private boolean sufficientResources(Offer offer) {
    boolean sufficient = Resources.from(offer).greaterThanOrEqual(TOTAL_GC_EXECUTOR_RESOURCES);
    if (!sufficient) {
      LOG.warning("Offer for host " + offer.getHostname() + " is too small for a GC executor");
    }
    return sufficient;
  }

  @Override
  public boolean willUse(final Offer offer) {
    if (!settings.getGcExecutorPath().isPresent()
        || isAlive(offer.getHostname())
        || !sufficientResources(offer)) {

      return false;
    }

    pulses.put(offer.getHostname(), clock.nowMillis() + settings.getDelayMs());
    executor.execute(new Runnable() {
      @Override
      public void run() {
        driver.launchTask(offer.getId(), makeGcTask(offer.getHostname(), offer.getSlaveId()));
      }
    });
    offersConsumed.incrementAndGet();
    return true;
  }

  @Override
  public boolean statusUpdate(TaskStatus status) {
    if (status.getTaskId().getValue().startsWith(SYSTEM_TASK_PREFIX)) {
      LOG.info("Received status update for GC task: " + Protobufs.toString(status));
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void cancelOffer(OfferID offer) {
    // No-op.
  }

  private boolean isAlive(String hostname) {
    Optional<Long> timestamp = Optional.fromNullable(pulses.getIfPresent(hostname));
    return timestamp.isPresent() && clock.nowMillis() < timestamp.get();
  }

  public static class GcExecutorSettings {
    protected final Amount<Long, Time> gcInterval;
    private final Optional<String> gcExecutorPath;

    @VisibleForTesting
    GcExecutorSettings(Amount<Long, Time> gcInterval, Optional<String> gcExecutorPath) {
      this.gcInterval = checkNotNull(gcInterval);
      this.gcExecutorPath = checkNotNull(gcExecutorPath);
    }

    @VisibleForTesting
    long getMaxGcInterval() {
      return gcInterval.as(Time.MILLISECONDS);
    }

    @VisibleForTesting
    int getDelayMs() {
      return gcInterval.as(Time.MILLISECONDS).intValue();
    }

    @VisibleForTesting
    Optional<String> getGcExecutorPath() {
      return gcExecutorPath;
    }
  }

  /**
   * Wraps configuration values for the {@code GcExecutorLauncher}.
   */
  static class RandomGcExecutorSettings extends GcExecutorSettings {
    private final Random rand = new Random.SystemRandom(new java.util.Random());

    RandomGcExecutorSettings(Amount<Long, Time> gcInterval, Optional<String> gcExecutorPath) {
      super(gcInterval, gcExecutorPath);
    }

    @Override
    int getDelayMs() {
      return rand.nextInt(gcInterval.as(Time.MILLISECONDS).intValue());
    }
  }
}
