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
package org.apache.aurora.scheduler.quota;

import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.junit.Test;

import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.INSUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.quota.QuotaCheckResult.Result.SUFFICIENT_QUOTA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QuotaCheckResultTest {

  @Test
  public void testGreaterOrEqualPass() {
    IResourceAggregate quota = IResourceAggregate.build(new ResourceAggregate()
        .setNumCpus(1.0)
        .setRamMb(256L)
        .setDiskMb(512L));
    IResourceAggregate request = IResourceAggregate.build(new ResourceAggregate()
        .setNumCpus(1.0)
        .setRamMb(256L)
        .setDiskMb(512L));
    assertEquals(SUFFICIENT_QUOTA, QuotaCheckResult.greaterOrEqual(quota, request).getResult());
  }

  @Test
  public void testGreaterOrEqualFailsCpu() {
    IResourceAggregate quota = IResourceAggregate.build(new ResourceAggregate()
        .setNumCpus(1.0)
        .setRamMb(256L)
        .setDiskMb(512L));
    IResourceAggregate request = IResourceAggregate.build(new ResourceAggregate()
        .setNumCpus(2.0)
        .setRamMb(256L)
        .setDiskMb(512L));
    QuotaCheckResult result = QuotaCheckResult.greaterOrEqual(quota, request);
    assertEquals(INSUFFICIENT_QUOTA, result.getResult());
    assertTrue(result.getDetails().get().contains("CPU"));
  }

  @Test
  public void testGreaterOrEqualFailsRam() {
    IResourceAggregate quota = IResourceAggregate.build(new ResourceAggregate()
        .setNumCpus(1.0)
        .setRamMb(256L)
        .setDiskMb(512L));
    IResourceAggregate request = IResourceAggregate.build(new ResourceAggregate()
        .setNumCpus(1.0)
        .setRamMb(512L)
        .setDiskMb(512L));
    QuotaCheckResult result = QuotaCheckResult.greaterOrEqual(quota, request);
    assertEquals(INSUFFICIENT_QUOTA, result.getResult());
    assertTrue(result.getDetails().get().length() > 0);
    assertTrue(result.getDetails().get().contains("RAM"));
  }

  @Test
  public void testGreaterOrEqualFailsDisk() {
    IResourceAggregate quota = IResourceAggregate.build(new ResourceAggregate()
        .setNumCpus(1.0)
        .setRamMb(256L)
        .setDiskMb(512L));
    IResourceAggregate request = IResourceAggregate.build(new ResourceAggregate()
        .setNumCpus(1.0)
        .setRamMb(256L)
        .setDiskMb(1024L));
    QuotaCheckResult result = QuotaCheckResult.greaterOrEqual(quota, request);
    assertEquals(INSUFFICIENT_QUOTA, result.getResult());
    assertTrue(result.getDetails().get().length() > 0);
    assertTrue(result.getDetails().get().contains("DISK"));
  }
}
