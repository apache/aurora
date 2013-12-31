/*
 * Copyright 2013 Twitter, Inc.
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
package org.apache.aurora.scheduler;

import java.util.Arrays;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static org.apache.mesos.Protos.Offer;

/**
 * Resource containing class that is aware of executor overhead.
 */
public final class ResourceSlot {

  private final Resources resources;

  /**
   * CPU allocated for each executor.
   */
  @VisibleForTesting
  static final double EXECUTOR_CPUS = 0.25;

  /**
   * RAM required for the executor.  Executors in the wild have been observed using 48-54MB RSS,
   * setting to 128MB to be extra vigilant initially.
   */
  @VisibleForTesting
  static final Amount<Long, Data> EXECUTOR_RAM = Amount.of(128L, Data.MB);

  private ResourceSlot(Resources r) {
    this.resources = r;
  }

  public static ResourceSlot from(ITaskConfig task) {
    double totalCPU = task.getNumCpus() + EXECUTOR_CPUS;
    Amount<Long, Data> totalRAM = Amount.of(task.getRamMb() + EXECUTOR_RAM.as(Data.MB), Data.MB);
    Amount<Long, Data> disk = Amount.of(task.getDiskMb(), Data.MB);
    return new ResourceSlot(
        new Resources(totalCPU, totalRAM, disk, task.getRequestedPorts().size()));
  }

  public static ResourceSlot from(Offer offer) {
    return new ResourceSlot(Resources.from(offer));
  }

  public double getNumCpus() {
    return resources.getNumCpus();
  }

  public Amount<Long, Data> getRam() {
    return resources.getRam();
  }

  public Amount<Long, Data> getDisk() {
    return resources.getDisk();
  }

  public int getNumPorts() {
    return resources.getNumPorts();
  }

  @VisibleForTesting
  public static ResourceSlot from(double cpu,
                                  Amount<Long, Data> ram,
                                  Amount<Long, Data> disk,
                                  int ports) {
    double totalCPU = cpu + EXECUTOR_CPUS;
    Amount<Long, Data> totalRAM = Amount.of(ram.as(Data.MB) + EXECUTOR_RAM.as(Data.MB), Data.MB);

    return new ResourceSlot(new Resources(totalCPU, totalRAM, disk, ports));
  }

  public static ResourceSlot sum(ResourceSlot... rs) {
    return sum(Arrays.asList(rs));
  }

  public static ResourceSlot sum(Iterable<ResourceSlot> rs) {
    Resources r = Resources.sum(Iterables.transform(rs, new Function<ResourceSlot, Resources>() {
      @Override public Resources apply(ResourceSlot input) {
        return input.resources;
      }
    }));

    return new ResourceSlot(r);
  }

  public static final Ordering<ResourceSlot> ORDER = new Ordering<ResourceSlot>() {
    @Override public int compare(ResourceSlot left, ResourceSlot right) {
      return Resources.RESOURCE_ORDER.compare(left.resources, right.resources);
    }
  };
}
