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

package org.apache.aurora.scheduler.storage.db;

import java.util.Properties;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.apache.aurora.common.stats.SlidingStats;
import org.apache.aurora.common.util.Clock;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Plugin;
import org.apache.ibatis.plugin.Signature;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * A Mybatis Executor invocation interceptor that exports timing information for update and query
 * mapped statements.
 *
 * Currently intercepting the following invocations:
 * 1. update(MappedStatement ms, Object parameter)
 * 2. query(MappedStatement ms, Object parameter, RowBounds rowBounds,
 *      ResultHandler resultHandler, CacheKey cacheKey, BoundSql boundSql)
 * 3. query(MappedStatement ms, Object parameter, RowBounds rowBounds,
 *      ResultHandler resultHandler)
 * 4. queryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds)
 *
 * more signatures can be added from: org.apache.ibatis.executors
 */
@Intercepts({
    @Signature(
        type = Executor.class,
        method = "update",
        args = {MappedStatement.class, Object.class}),
    @Signature(type = Executor.class,
        method = "query",
        args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class,
            CacheKey.class, BoundSql.class}),
    @Signature(type = Executor.class,
        method = "query",
        args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class}),
    @Signature(type = Executor.class,
        method = "queryCursor",
        args = {MappedStatement.class, Object.class, RowBounds.class})
    })
public class InstrumentingInterceptor implements Interceptor {
  private static final String INVALID_INVOCATION_METRIC_NAME = "invalid_invocations";
  private static final String STATS_NAME_PREFIX = "mybatis.";
  private static final Logger LOG = LoggerFactory.getLogger(InstrumentingInterceptor.class);
  private final Clock clock;
  private final LoadingCache<String, SlidingStats> stats;

  @Inject
  public InstrumentingInterceptor(Clock clock) {
    this(clock, (String name) -> new SlidingStats(name, "nanos"));
  }

  @VisibleForTesting
  public InstrumentingInterceptor(Clock clock, Function<String, SlidingStats> statsFactory) {
    this.clock = requireNonNull(clock);

    this.stats = CacheBuilder.newBuilder().build(new CacheLoader<String, SlidingStats>() {
      @Override public SlidingStats load(String statsName) {
        return statsFactory.apply(STATS_NAME_PREFIX + statsName);
      }
    });
  }

  private String generateStatsName(Invocation invocation) {
    if (firstArgumentIsMappedStatement(invocation)) {
      MappedStatement statement = (MappedStatement) invocation.getArgs()[0];
      return statement.getId();
    }

    LOG.warn("Received invocation for unknown or invalid target. Invocation target: {}. "
            + "Invocation method: {}. Using metric name '{}' instead.",
        invocation.getTarget(),
        invocation.getMethod(),
        INVALID_INVOCATION_METRIC_NAME);
    return INVALID_INVOCATION_METRIC_NAME;
  }

  private boolean firstArgumentIsMappedStatement(Invocation invocation) {
    return invocation != null
        && invocation.getArgs() != null
        && invocation.getArgs()[0] instanceof MappedStatement;
  }

  @Override
  public Object intercept(@Nonnull Invocation invocation) throws Throwable {
    long start = clock.nowNanos();
    try {
      return invocation.proceed();
    } finally {
      String statsName = generateStatsName(invocation);
      SlidingStats stat = stats.get(statsName);
      stat.accumulate(clock.nowNanos() - start);
    }
  }

  @Override
  public Object plugin(Object target) {
    return Plugin.wrap(target, this);
  }

  @Override
  public void setProperties(Properties properties) {
    // intentionally left empty as instructed in http://www.mybatis.org/mybatis-3/configuration.html
  }
}
