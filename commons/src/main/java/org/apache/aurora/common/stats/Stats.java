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

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;

import org.apache.aurora.common.base.MorePreconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages {@link Stat}s that should be exported for monitoring.
 *
 * Statistic names may only contain {@code [A-Za-z0-9_]},
 * all other chars will be logged as a warning and replaced with underscore on export.
 *
 * @author John Sirois
 */
public class Stats {

  private static final Logger LOG = LoggerFactory.getLogger(Stats.class);
  private static final Pattern NOT_NAME_CHAR = Pattern.compile("[^A-Za-z0-9_/.-]");

  private static final ConcurrentMap<String, Stat<?>> VAR_MAP = new MapMaker().makeMap();

  // Store stats in the order they were registered, so that derived variables are
  // sampled after their inputs.
  private static final Collection<RecordingStat<? extends Number>> ORDERED_NUMERIC_STATS =
      new ConcurrentLinkedQueue<>();

  private static final Cache<String, RecordingStat<? extends Number>> NUMERIC_STATS =
      CacheBuilder.newBuilder().build();

  public static String normalizeName(String name) {
    return NOT_NAME_CHAR.matcher(name).replaceAll("_");
  }

  static String validateName(String name) {
    String normalized = normalizeName(name);
    if (!name.equals(normalized)) {
      LOG.warn("Invalid stat name " + name + " exported as " + normalized);
    }
    return normalized;
  }

  /**
   * A {@link StatsProvider} that exports gauge-style stats to the global {@link Stat}s repository
   * for time series tracking.
   */
  public static final StatsProvider STATS_PROVIDER = new StatsProvider() {
    private final StatsProvider untracked = new StatsProvider() {
      @Override public AtomicLong makeCounter(String name) {
        final AtomicLong longVar = new AtomicLong();
        Stats.exportStatic(new StatImpl<Long>(name) {
          @Override public Long read() {
            return longVar.get();
          }
        });
        return longVar;
      }

      @Override public <T extends Number> Stat<T> makeGauge(String name, final Supplier<T> gauge) {
        return Stats.exportStatic(new StatImpl<T>(name) {
          @Override public T read() {
            return gauge.get();
          }
        });
      }

      @Override public StatsProvider untracked() {
        return this;
      }

      @Override public RequestTimer makeRequestTimer(String name) {
        // TODO(William Farner): Add support for this once a caller shows interest in using it.
        throw new UnsupportedOperationException();
      }
    };

    @Override public <T extends Number> Stat<T> makeGauge(String name, final Supplier<T> gauge) {
      return Stats.export(new StatImpl<T>(name) {
        @Override public T read() {
          return gauge.get();
        }
      });
    }

    @Override public AtomicLong makeCounter(String name) {
      return Stats.exportLong(name);
    }

    @Override public StatsProvider untracked() {
      return untracked;
    }

    @Override public RequestTimer makeRequestTimer(String name) {
      return new RequestStats(name);
    }
  };

  /**
   * A {@link StatRegistry} that provides stats registered with the global {@link Stat}s repository.
   */
  public static final StatRegistry STAT_REGISTRY = Stats::getNumericVariables;

  private static class ExportStat implements Callable<RecordingStat<? extends Number>> {
    private final AtomicBoolean called = new AtomicBoolean(false);

    private final RecordingStat<? extends Number> stat;
    private final String name;

    private <T extends Number> ExportStat(String name, Stat<T> stat) {
      this.name = name;
      this.stat = (stat instanceof RecordingStat)
          ? (RecordingStat<? extends Number>) stat
          : new RecordingStatImpl<>(stat);
    }

    @Override
    public RecordingStat<? extends Number> call() {
      try {
        exportStaticInternal(name, stat);
        ORDERED_NUMERIC_STATS.add(stat);
        return stat;
      } finally {
        called.set(true);
      }
    }
  }

  /**
   * Exports a stat for tracking.
   * if the stat provided implements the internal {@link RecordingStat} interface, it will be
   * registered for time series collection and returned.  If a {@link RecordingStat} with the same
   * name as the provided stat has already been exported, the previously-exported stat will be
   * returned and no additional registration will be performed.
   *
   * @param var The variable to export.
   * @param <T> The value exported by the variable.
   * @return A reference to the stat that was stored.  The stat returned may not be equal to the
   *    stat provided.  If a variable was already returned with the same
   */
  public static <T extends Number> Stat<T> export(Stat<T> var) {
    String validatedName = validateName(MorePreconditions.checkNotBlank(var.getName()));
    ExportStat exportStat = new ExportStat(validatedName, var);
    try {
      @SuppressWarnings("unchecked")
      Stat<T> exported = (Stat<T>) NUMERIC_STATS.get(validatedName, exportStat);
      return exported;
    } catch (ExecutionException e) {
      throw new IllegalStateException(
          "Unexpected error exporting stat " + validatedName, e.getCause());
    } finally {
      if (!exportStat.called.get()) {
        LOG.warn("Re-using already registered variable for key " + validatedName);
      }
    }
  }

  /**
   * Exports a string stat.
   * String-based statistics will not be registered for time series collection.
   *
   * @param var Stat to export.
   * @return A reference back to {@code var}, or the variable that was already registered under the
   *    same name as {@code var}.
   */
  static Stat<String> exportString(Stat<String> var) {
    return exportStatic(var);
  }

  /**
   * Adds a collection of stats for export.
   *
   * @param vars The variables to add.
   */
  static void exportAll(Iterable<Stat<? extends Number>> vars) {
    for (Stat<? extends Number> var : vars) {
      export(var);
    }
  }

  /**
   * Creates and exports an {@link AtomicInteger}.
   *
   * @param name The name to export the stat with.
   * @return A reference to the {@link AtomicInteger} created.
   */
  public static AtomicInteger exportInt(final String name) {
    final AtomicInteger intVar = new AtomicInteger(0);
    export(new SampledStat<Integer>(name, 0) {
      @Override public Integer doSample() { return intVar.get(); }
    });

    return intVar;
  }

  /**
   * Creates and exports an {@link AtomicLong}.
   *
   * @param name The name to export the stat with.
   * @return A reference to the {@link AtomicLong} created.
   */
  public static AtomicLong exportLong(String name) {
    final AtomicLong longVar = new AtomicLong(0L);
    export(new StatImpl<Long>(name) {
      @Override public Long read() { return longVar.get(); }
    });

    return longVar;
  }

  /**
   * Exports a 'static' statistic, which will not be registered for time series tracking.
   *
   * @param var Variable to statically export.
   * @return A reference back to the provided {@link Stat}.
   */
  static <T> Stat<T> exportStatic(Stat<T> var) {
    String validatedName = validateName(MorePreconditions.checkNotBlank(var.getName()));
    exportStaticInternal(validatedName, var);
    return var;
  }

  private static void exportStaticInternal(String name, Stat<?> stat) {
    if (VAR_MAP.put(name, stat) != null) {
      LOG.warn("Warning - exported variable collision on " + name);
    }
  }

  /**
   * Fetches all registered stat.
   *
   * @return An iterable of all registered stats.
   */
  public static Iterable<Stat<?>> getVariables() {
    return ImmutableList.copyOf(VAR_MAP.values());
  }

  static Iterable<RecordingStat<? extends Number>> getNumericVariables() {
    return ImmutableList.copyOf(ORDERED_NUMERIC_STATS);
  }

  @VisibleForTesting
  public static void flush() {
    VAR_MAP.clear();
    ORDERED_NUMERIC_STATS.clear();
    NUMERIC_STATS.invalidateAll();
  }

  @VisibleForTesting
  public static <T> Stat<T> getVariable(String name) {
    MorePreconditions.checkNotBlank(name);
    @SuppressWarnings("unchecked")
    Stat<T> stat = (Stat<T>) VAR_MAP.get(name);
    return stat;
  }
}
