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
package org.apache.aurora.scheduler.stats;

import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceTestUtil;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.mem.MemStorageModule;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.Resource.diskMb;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.gen.Resource.ramMb;
import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.FAILED;
import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.KILLING;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RESTARTING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.bag;
import static org.apache.aurora.scheduler.stats.ResourceCounter.Metric;
import static org.apache.aurora.scheduler.stats.ResourceCounter.MetricType.DEDICATED_CONSUMED;
import static org.apache.aurora.scheduler.stats.ResourceCounter.MetricType.FREE_POOL_CONSUMED;
import static org.apache.aurora.scheduler.stats.ResourceCounter.MetricType.QUOTA_CONSUMED;
import static org.apache.aurora.scheduler.stats.ResourceCounter.MetricType.TOTAL_CONSUMED;
import static org.junit.Assert.assertEquals;

public class ResourceCounterTest {

  private static final Metric ZERO = new Metric(TOTAL_CONSUMED, ResourceBag.EMPTY);
  private static final long GB = 1024;
  private static final Optional<String> NOT_DEDICATED = Optional.absent();

  private static final boolean PRODUCTION = true;
  private static final boolean NONPRODUCTION = false;

  private Storage storage;
  private ResourceCounter resourceCounter;

  @Before
  public void setUp() throws Exception {
    storage = MemStorageModule.newEmptyStorage();
    resourceCounter = new ResourceCounter(storage);
  }

  @Test
  public void testNoTasks() {
    assertEquals(
        ZERO,
        resourceCounter.computeQuotaAllocationTotals());

    Map<IJobKey, Metric> aggregates = resourceCounter.computeAggregates(
        Query.unscoped(),
        Predicates.alwaysTrue(),
        ITaskConfig::getJob);
    assertEquals(ImmutableMap.of(), aggregates);

    for (Metric metric : resourceCounter.computeConsumptionTotals()) {
      assertEquals(ZERO.getBag(), metric.getBag());
    }
  }

  @Test
  public void testComputeConsumptionTotals() {
    insertTasks(
        task("bob", "jobA", "a", 1, GB, GB, PRODUCTION,    RUNNING,    NOT_DEDICATED),
        task("bob", "jobB", "b", 1, GB, GB, PRODUCTION,    RUNNING,    NOT_DEDICATED),
        task("tim", "jobC", "c", 1, GB, GB, PRODUCTION,    PENDING,    NOT_DEDICATED),
        task("tim", "jobD", "d", 1, GB, GB, PRODUCTION,    KILLING,    NOT_DEDICATED),
        task("bob", "jobE", "e", 1, GB, GB, NONPRODUCTION, ASSIGNED,   NOT_DEDICATED),
        task("tom", "jobF", "f", 1, GB, GB, NONPRODUCTION, RUNNING,    NOT_DEDICATED),
        task("tom", "jobG", "g", 1, GB, GB, NONPRODUCTION, RESTARTING, Optional.of("database")),
        task("lil", "jobH", "h", 1, GB, GB, PRODUCTION,    RUNNING,    Optional.of("queue")),
        task("lil", "jobI", "i", 1, GB, GB, PRODUCTION,    FINISHED,   NOT_DEDICATED)
    );

    Set<Metric> expected = ImmutableSet.of(
        new Metric(TOTAL_CONSUMED,     bag(8, 8 * GB, 8 * GB)),
        new Metric(DEDICATED_CONSUMED, bag(2, 2 * GB, 2 * GB)),
        new Metric(QUOTA_CONSUMED,     bag(4, 4 * GB, 4 * GB)),
        new Metric(FREE_POOL_CONSUMED, bag(2, 2 * GB, 2 * GB)));

    assertEquals(expected, ImmutableSet.copyOf(resourceCounter.computeConsumptionTotals()));
  }

  @Test
  public void testComputeQuotaAllocationTotals() {
    storage.write((NoResult.Quiet) storeProvider -> {
      storeProvider.getQuotaStore().saveQuota("a", ResourceTestUtil.aggregate(1, 1, 1));
      storeProvider.getQuotaStore().saveQuota("b", ResourceTestUtil.aggregate(2, 3, 4));
    });

    assertEquals(
        new Metric(TOTAL_CONSUMED, bag(3, 4, 5)),
        resourceCounter.computeQuotaAllocationTotals());
  }

  @Test
  public void testComputeAggregates() {
    insertTasks(
        task("bob", "jobA", "a",  1, GB, GB, PRODUCTION,    RUNNING,    NOT_DEDICATED),
        task("bob", "jobB", "b",  1, GB, GB, PRODUCTION,    FAILED,     NOT_DEDICATED),
        task("bob", "jobB", "b2", 1, GB, GB, PRODUCTION,    FAILED,     NOT_DEDICATED),
        task("bob", "jobC", "c",  1, GB, GB, NONPRODUCTION, RUNNING,    NOT_DEDICATED),
        task("tim", "jobD", "d",  1, GB, GB, PRODUCTION,    RUNNING,    NOT_DEDICATED),
        task("bob", "jobE", "e",  1, GB, GB, NONPRODUCTION, ASSIGNED,   NOT_DEDICATED),
        task("lil", "jobF", "f",  1, GB, GB, PRODUCTION,    RUNNING,    Optional.of("queue")),
        task("lil", "jobG", "g",  1, GB, GB, PRODUCTION,    FINISHED,   NOT_DEDICATED)
    );

    assertEquals(
        ImmutableMap.of(
            JobKeys.from("bob", "test", "jobA"), new Metric(TOTAL_CONSUMED, bag(1, 1 * GB, 1 * GB)),
            JobKeys.from("bob", "test", "jobB"), new Metric(TOTAL_CONSUMED, bag(2, 2 * GB, 2 * GB))
        ),
        resourceCounter.computeAggregates(
            Query.roleScoped("bob"),
            ITaskConfig::isProduction,
            ITaskConfig::getJob)
    );
  }

  private static IScheduledTask task(
      String role,
      String job,
      String id,
      int numCpus,
      long ramMb,
      long diskMb,
      boolean production,
      ScheduleStatus status,
      Optional<String> dedicated) {

    ScheduledTask task = TaskTestUtil.makeTask(id, JobKeys.from(role, "test", job)).newBuilder();
    TaskConfig config = task.getAssignedTask().getTask()
        .setResources(ImmutableSet.of(
                numCpus(numCpus),
                ramMb(ramMb),
                diskMb(diskMb)))
        .setProduction(production);

    if (dedicated.isPresent()) {
      config.addToConstraints(new Constraint(
          ConfigurationManager.DEDICATED_ATTRIBUTE,
          TaskConstraint.value(new ValueConstraint(false, ImmutableSet.of(dedicated.get())))));
    }

    task.setStatus(status);
    return IScheduledTask.build(task);
  }

  private void insertTasks(final IScheduledTask... tasks) {
    storage.write((NoResult.Quiet)
        storeProvider -> storeProvider.getUnsafeTaskStore().saveTasks(ImmutableSet.copyOf(tasks)));
  }
}
