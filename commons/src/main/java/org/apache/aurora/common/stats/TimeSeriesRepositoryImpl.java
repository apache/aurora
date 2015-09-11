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
package org.apache.aurora.common.stats;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.util.Clock;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A simple in-memory repository for exported variables.
 *
 * @author John Sirois
 */
public class TimeSeriesRepositoryImpl
    extends AbstractScheduledService implements TimeSeriesRepository {

  private static final Logger LOG = Logger.getLogger(TimeSeriesRepositoryImpl.class.getName());

  /**
   * {@literal @Named} binding key for the sampling period.
   */
  public static final String SAMPLE_PERIOD =
      "com.twitter.common.stats.TimeSeriesRepositoryImpl.SAMPLE_PERIOD";

  /**
   * {@literal @Named} binding key for the maximum number of retained samples.
   */
  public static final String SAMPLE_RETENTION_PERIOD =
      "com.twitter.common.stats.TimeSeriesRepositoryImpl.SAMPLE_RETENTION_PERIOD";

  private final SlidingStats scrapeDuration = new SlidingStats("variable_scrape", "micros");

  // We store TimeSeriesImpl, which allows us to add samples.
  private final LoadingCache<String, TimeSeriesImpl> timeSeries;
  private final EvictingQueue<Number> timestamps;

  private final StatRegistry statRegistry;
  private final Amount<Long, Time> samplePeriod;
  private final int retainedSampleLimit;

  @Inject
  public TimeSeriesRepositoryImpl(
      StatRegistry statRegistry,
      @Named(SAMPLE_PERIOD) Amount<Long, Time> samplePeriod,
      @Named(SAMPLE_RETENTION_PERIOD) final Amount<Long, Time> retentionPeriod) {
    this.statRegistry = checkNotNull(statRegistry);
    this.samplePeriod = checkNotNull(samplePeriod);
    Preconditions.checkArgument(samplePeriod.getValue() > 0, "Sample period must be positive.");
    checkNotNull(retentionPeriod);
    Preconditions.checkArgument(retentionPeriod.getValue() > 0,
        "Sample retention period must be positive.");

    retainedSampleLimit = (int) (retentionPeriod.as(Time.SECONDS) / samplePeriod.as(Time.SECONDS));
    Preconditions.checkArgument(retainedSampleLimit > 0,
        "Sample retention period must be greater than sample period.");

    timeSeries = CacheBuilder.newBuilder().build(
        new CacheLoader<String, TimeSeriesImpl>() {
          @Override public TimeSeriesImpl load(final String name) {
            TimeSeriesImpl timeSeries = new TimeSeriesImpl(name);

            // Backfill so we have data for pre-accumulated timestamps.
            int numTimestamps = timestamps.size();
            if (numTimestamps != 0) {
              for (int i = 1; i < numTimestamps; i++) {
                timeSeries.addSample(0L);
              }
            }

            return timeSeries;
          }
        });

    timestamps = EvictingQueue.create(retainedSampleLimit);
  }

  private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(
      1 /* One thread. */,
      new ThreadFactoryBuilder().setNameFormat("VariableSampler-%d").setDaemon(true).build());

  @Override
  protected void startUp() throws Exception {
    JvmStats.export();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        samplePeriod.getValue(),
        samplePeriod.getValue(),
        samplePeriod.getUnit().getTimeUnit());
  }

  @Override
  protected ScheduledExecutorService executor() {
    return executor;
  }

  @Override
  protected void runOneIteration() throws Exception {
    runSampler(Clock.SYSTEM_CLOCK);
  }

  @Override
  protected void shutDown() throws Exception {
    executor.shutdown();
    LOG.info("Variable sampler shut down");
  }

  @VisibleForTesting
  synchronized void runSampler(Clock clock) {
    timestamps.add(clock.nowMillis());

    long startNanos = clock.nowNanos();
    for (RecordingStat<? extends Number> var : statRegistry.getStats()) {
      timeSeries.getUnchecked(var.getName()).addSample(var.sample());
    }
    scrapeDuration.accumulate(
        Amount.of(clock.nowNanos() - startNanos, Time.NANOSECONDS).as(Time.MICROSECONDS));
  }

  @Override
  public synchronized Set<String> getAvailableSeries() {
    return ImmutableSet.copyOf(timeSeries.asMap().keySet());
  }

  @Override
  public synchronized TimeSeries get(String name) {
    if (!timeSeries.asMap().containsKey(name)) return null;
    return timeSeries.getUnchecked(name);
  }

  @Override
  public synchronized Iterable<Number> getTimestamps() {
    return Iterables.unmodifiableIterable(timestamps);
  }

  private class TimeSeriesImpl implements TimeSeries {
    private final String name;
    private final EvictingQueue<Number> samples;

    TimeSeriesImpl(String name) {
      this.name = name;
      samples = EvictingQueue.create(retainedSampleLimit);
    }

    @Override public String getName() {
      return name;
    }

    void addSample(Number value) {
      samples.add(value);
    }

    @Override public Iterable<Number> getSamples() {
      return Iterables.unmodifiableIterable(samples);
    }
  }
}
