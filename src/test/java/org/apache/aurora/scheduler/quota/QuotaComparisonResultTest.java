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

import org.apache.aurora.gen.Quota;
import org.apache.aurora.scheduler.storage.entities.IQuota;

import org.junit.Test;

import static org.apache.aurora.scheduler.quota.QuotaComparisonResult.Result.INSUFFICIENT_QUOTA;
import static org.apache.aurora.scheduler.quota.QuotaComparisonResult.Result.SUFFICIENT_QUOTA;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QuotaComparisonResultTest {

  @Test
  public void testGreaterOrEqualPass() {
    IQuota quota = IQuota.build(new Quota()
            .setNumCpus(1.0)
            .setRamMb(256L)
            .setDiskMb(512L));
    IQuota request = IQuota.build(new Quota()
            .setNumCpus(1.0)
            .setRamMb(256L)
            .setDiskMb(512L));
    assertEquals(SUFFICIENT_QUOTA, QuotaComparisonResult.greaterOrEqual(quota, request).result());
  }

  @Test
  public void testGreaterOrEqualFailsCpu() {
    IQuota quota = IQuota.build(new Quota()
            .setNumCpus(1.0)
            .setRamMb(256L)
            .setDiskMb(512L));
    IQuota request = IQuota.build(new Quota()
            .setNumCpus(2.0)
            .setRamMb(256L)
            .setDiskMb(512L));
    QuotaComparisonResult result = QuotaComparisonResult.greaterOrEqual(quota, request);
    assertEquals(INSUFFICIENT_QUOTA, result.result());
    assertTrue(result.details().contains("CPU"));
  }

  @Test
  public void testGreaterOrEqualFailsRam() {
    IQuota quota = IQuota.build(new Quota()
            .setNumCpus(1.0)
            .setRamMb(256L)
            .setDiskMb(512L));
    IQuota request = IQuota.build(new Quota()
            .setNumCpus(1.0)
            .setRamMb(512L)
            .setDiskMb(512L));
    QuotaComparisonResult result = QuotaComparisonResult.greaterOrEqual(quota, request);
    assertEquals(INSUFFICIENT_QUOTA, result.result());
    assertTrue(result.details().length() > 0);
    assertTrue(result.details().contains("RAM"));
  }

  @Test
  public void testGreaterOrEqualFailsDisk() {
    IQuota quota = IQuota.build(new Quota()
            .setNumCpus(1.0)
            .setRamMb(256L)
            .setDiskMb(512L));
    IQuota request = IQuota.build(new Quota()
            .setNumCpus(1.0)
            .setRamMb(256L)
            .setDiskMb(1024L));
    QuotaComparisonResult result = QuotaComparisonResult.greaterOrEqual(quota, request);
    assertEquals(INSUFFICIENT_QUOTA, result.result());
    assertTrue(result.details().length() > 0);
    assertTrue(result.details().contains("DISK"));
  }
}
