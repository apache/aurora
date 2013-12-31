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
package org.apache.aurora.scheduler.thrift.aop;

import java.lang.reflect.Method;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import com.twitter.common.stats.SlidingStats;
import com.twitter.common.stats.Stats;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

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
