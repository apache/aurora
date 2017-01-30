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
package org.apache.aurora.scheduler.thrift.aop;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.aurora.common.stats.SlidingStats;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.scheduler.thrift.aop.ThriftWorkload.ThriftWorkloadCounter;

/**
 * A method interceptor that exports counterStats about thrift calls.
 */
class ThriftStatsExporterInterceptor implements MethodInterceptor {

  @VisibleForTesting
  static final String TIMING_STATS_NAME_TEMPLATE = "scheduler_thrift_%s";
  @VisibleForTesting
  static final String WORKLOAD_STATS_NAME_TEMPLATE = "scheduler_thrift_workload_%s";

  private final LoadingCache<Method, SlidingStats> timingStats =
      CacheBuilder.newBuilder().build(new CacheLoader<Method, SlidingStats>() {
        @Override
        public SlidingStats load(Method method) {
          return new SlidingStats(
              Stats.normalizeName(String.format(TIMING_STATS_NAME_TEMPLATE, method.getName())),
              "nanos");
        }
      });

  private final LoadingCache<Method, AtomicLong> workloadStats =
      CacheBuilder.newBuilder().build(new CacheLoader<Method, AtomicLong>() {
        @Override
        public AtomicLong load(Method method) {
          return Stats.exportLong(
              Stats.normalizeName(String.format(WORKLOAD_STATS_NAME_TEMPLATE, method.getName())));
        }
      });

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    Method method = invocation.getMethod();
    SlidingStats stat = timingStats.getUnchecked(method);
    long start = System.nanoTime();
    Response response = null;
    try {
      response = (Response) invocation.proceed();
    } finally {
      stat.accumulate(System.nanoTime() - start);
      if (response != null
          && response.getResponseCode() == ResponseCode.OK
          && method.isAnnotationPresent(ThriftWorkload.class)) {

        ThriftWorkloadCounter counter = method.getAnnotation(ThriftWorkload.class)
            .value()
            .newInstance();
        workloadStats.getUnchecked(method).addAndGet(counter.apply(response.getResult()));
      }
    }
    return response;
  }
}
