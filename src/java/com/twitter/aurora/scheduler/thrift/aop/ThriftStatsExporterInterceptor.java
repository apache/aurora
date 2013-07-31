package com.twitter.aurora.scheduler.thrift.aop;

import java.lang.reflect.Method;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import com.twitter.common.stats.SlidingStats;
import com.twitter.common.stats.Stats;

/**
 * A method interceptor that exports counterStats about thrift calls.
 */
class ThriftStatsExporterInterceptor implements MethodInterceptor {

  private final LoadingCache<Method, SlidingStats> stats =
      CacheBuilder.newBuilder().build(new CacheLoader<Method, SlidingStats>() {
        @Override public SlidingStats load(Method method) {
          return new SlidingStats(
              Stats.normalizeName(String.format("scheduler_thrift_%s", method.getName())),
              "nanos");
        }
      });

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    SlidingStats stat = stats.get(invocation.getMethod());
    long start = System.nanoTime();
    try {
      return invocation.proceed();
    } finally {
      stat.accumulate(System.nanoTime() - start);
    }
  }
}
