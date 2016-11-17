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
package org.apache.aurora.benchmark;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.aurora.benchmark.fakes.FakeStatsProvider;
import org.apache.aurora.common.inject.Bindings;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.ReadOnlyScheduler;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.TaskQuery;
import org.apache.aurora.scheduler.async.AsyncModule;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.state.LockManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.db.DbModule;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.thrift.ThriftModule;
import org.apache.thrift.TException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

public class ThriftApiBenchmarks {

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
  @Fork(1)
  @State(Scope.Thread)
  public static class GetRoleSummaryBenchmark {
    private ReadOnlyScheduler.Iface api;

    @Param({
        "{\"roles\": 1}",
        "{\"roles\": 10}",
        "{\"roles\": 100}",
        "{\"roles\": 500}",
        "{\"jobs\": 1}",
        "{\"jobs\": 10}",
        "{\"jobs\": 100}",
        "{\"jobs\": 500}",
        "{\"instances\": 1}",
        "{\"instances\": 10}",
        "{\"instances\": 100}",
        "{\"instances\": 1000}",
        "{\"instances\": 10000}"})
    private String testConfiguration;

    @Setup
    public void setUp() {
      api = createPopulatedApi(testConfiguration);
    }

    @Benchmark
    public Response run() throws TException {
      return api.getRoleSummary();
    }
  }

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
  @Fork(1)
  @State(Scope.Thread)
  public static class GetAllTasksBenchmark {
    private ReadOnlyScheduler.Iface api;

    @Param({
        "{\"roles\": 1}",
        "{\"roles\": 10}",
        "{\"roles\": 100}",
        "{\"roles\": 500}",
        "{\"jobs\": 1}",
        "{\"jobs\": 10}",
        "{\"jobs\": 100}",
        "{\"jobs\": 500}",
        "{\"instances\": 1}",
        "{\"instances\": 10}",
        "{\"instances\": 100}",
        "{\"instances\": 1000}",
        "{\"instances\": 10000}"})
    private String testConfiguration;

    @Setup
    public void setUp() {
      api = createPopulatedApi(testConfiguration);
    }

    @Benchmark
    public Response run() throws TException {
      return api.getTasksStatus(new TaskQuery());
    }
  }

  private static ReadOnlyScheduler.Iface createPopulatedApi(String testConfiguration) {
    TestConfiguration config = new Gson().fromJson(testConfiguration, TestConfiguration.class);

    Injector injector = createStorageInjector();
    ReadOnlyScheduler.Iface api = injector.getInstance(ReadOnlyScheduler.Iface.class);

    Storage storage = injector.getInstance(Storage.class);
    storage.prepare();

    bulkLoadTasks(storage, config);
    return api;
  }

  private static Injector createStorageInjector() {
    return Guice.createInjector(
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(Clock.class).toInstance(Clock.SYSTEM_CLOCK);
            bind(CronPredictor.class).toInstance(createThrowingFake(CronPredictor.class));
            bind(QuotaManager.class).toInstance(createThrowingFake(QuotaManager.class));
            bind(LockManager.class).toInstance(createThrowingFake(LockManager.class));
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
            bind(ConfigurationManager.class).toInstance(TaskTestUtil.CONFIGURATION_MANAGER);
          }
        },
        new AsyncModule(),
        DbModule.productionModule(Bindings.KeyFactory.PLAIN),
        new ThriftModule.ReadOnly());
  }

  private static void bulkLoadTasks(Storage storage, final TestConfiguration config) {
    // Ideally we would use the API to populate the storage, but wiring in the writable thrift
    // interface requires considerably more binding setup.
    storage.write((Storage.MutateWork.NoResult.Quiet) storeProvider -> {
      for (int roleId = 0; roleId < config.roles; roleId++) {
        String role = "role" + roleId;
        for (int envId = 0; envId < config.envs; envId++) {
          String env = "env" + envId;
          for (int jobId = 0; jobId < config.jobs; jobId++) {
            String job = "job" + jobId;
            ImmutableSet.Builder<IScheduledTask> tasks = ImmutableSet.builder();
            tasks.addAll(new Tasks.Builder()
                .setRole(role)
                .setEnv(env)
                .setJob(job)
                .setScheduleStatus(ScheduleStatus.RUNNING)
                .build(config.instances));
            tasks.addAll(new Tasks.Builder()
                .setRole(role)
                .setEnv(env)
                .setJob(job)
                .setScheduleStatus(ScheduleStatus.FINISHED)
                .build(config.deadTasks));
            storeProvider.getUnsafeTaskStore().saveTasks(tasks.build());
          }
        }
      }
    });
  }

  private static <T> T createThrowingFake(Class<T> clazz) {
    InvocationHandler handler = (o, method, objects) -> {
      throw new UnsupportedOperationException("This fake has no behavior.");
    };

    @SuppressWarnings("unchecked")
    T proxy = (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class[]{clazz}, handler);
    return proxy;
  }

  private static class TestConfiguration {
    private int roles = 1;
    private int envs = 5;
    private int jobs = 1;
    private int instances = 100;
    private int deadTasks = 100;
  }
}
