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
package org.apache.aurora.scheduler.storage.mem;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MemCronJobStoreTest {
  private static final IJobConfiguration JOB_A = makeJob("a");
  private static final IJobConfiguration JOB_B = makeJob("b");

  private static final IJobKey KEY_A = JOB_A.getKey();
  private static final IJobKey KEY_B = JOB_B.getKey();

  private CronJobStore.Mutable store;

  @Before
  public void setUp() {
    store = new MemJobStore();
  }

  @Test
  public void testJobStore() {
    assertNull(store.fetchJob(JobKeys.from("nobody", "nowhere", "noname")).orNull());
    assertEquals(ImmutableSet.<IJobConfiguration>of(), store.fetchJobs());

    store.saveAcceptedJob(JOB_A);
    assertEquals(JOB_A, store.fetchJob(KEY_A).orNull());
    assertEquals(ImmutableSet.of(JOB_A), store.fetchJobs());

    store.saveAcceptedJob(JOB_B);
    assertEquals(JOB_B, store.fetchJob(KEY_B).orNull());
    assertEquals(ImmutableSet.of(JOB_A, JOB_B), store.fetchJobs());

    store.removeJob(KEY_B);
    assertEquals(ImmutableSet.of(JOB_A), store.fetchJobs());

    store.deleteJobs();
    assertEquals(ImmutableSet.<IJobConfiguration>of(), store.fetchJobs());
  }

  @Test
  public void testJobStoreSameEnvironment() {
    IJobConfiguration templateConfig = makeJob("labrat");
    JobConfiguration prodBuilder = templateConfig.newBuilder();
    prodBuilder.getKey().setEnvironment("prod");
    IJobConfiguration prod = IJobConfiguration.build(prodBuilder);
    JobConfiguration stagingBuilder = templateConfig.newBuilder();
    stagingBuilder.getKey().setEnvironment("staging");
    IJobConfiguration staging = IJobConfiguration.build(stagingBuilder);

    store.saveAcceptedJob(prod);
    store.saveAcceptedJob(staging);

    assertNull(store.fetchJob(
        IJobKey.build(templateConfig.getKey().newBuilder().setEnvironment("test"))).orNull());
    assertEquals(prod, store.fetchJob(prod.getKey()).orNull());
    assertEquals(staging, store.fetchJob(staging.getKey()).orNull());

    store.removeJob(prod.getKey());
    assertNull(store.fetchJob(prod.getKey()).orNull());
    assertEquals(staging, store.fetchJob(staging.getKey()).orNull());
  }

  private static IJobConfiguration makeJob(String name) {
    return IJobConfiguration.build(
        new JobConfiguration().setKey(JobKeys.from("role-" + name, "env-" + name, name)
            .newBuilder()));
  }
}
