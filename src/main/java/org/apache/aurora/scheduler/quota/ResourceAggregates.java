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

import com.google.common.collect.Ordering;

import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;

/**
 * Convenience class for normalizing resource measures between tasks and offers.
 */
public final class ResourceAggregates {
  private static final IResourceAggregate EMPTY_RESOURCE_AGGREGATE =
      IResourceAggregate.build(new ResourceAggregate(0, 0, 0));

  private ResourceAggregates() {
    // Utility class.
  }

  /**
   * Returns a quota with all resource vectors zeroed.
   *
   * @return A resource aggregate with all resource vectors zeroed.
   */
  public static IResourceAggregate none() {
    return EMPTY_RESOURCE_AGGREGATE;
  }

  /**
   * a * m
   */
  public static IResourceAggregate scale(IResourceAggregate a, int m) {
    return IResourceAggregate.build(new ResourceAggregate()
        .setNumCpus(a.getNumCpus() * m)
        .setRamMb(a.getRamMb() * m)
        .setDiskMb(a.getDiskMb() * m));
  }

  /**
   * a / b
   * <p>
   * This calculates how many times {@code b} "fits into" {@code a}.  Behavior is undefined when
   * {@code b} contains resources with a value of zero.
   */
  public static int divide(IResourceAggregate a, IResourceAggregate b) {
    return Ordering.natural().min(
        a.getNumCpus() / b.getNumCpus(),
        (double) a.getRamMb() / b.getRamMb(),
        (double) a.getDiskMb() / b.getDiskMb()
    ).intValue();
  }
}
