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
package org.apache.aurora.scheduler;

import java.util.Arrays;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static org.apache.mesos.Protos.Offer;

/**
 * Resource containing class that is aware of executor overhead.
 */
public final class ResourceSlot {
  // TODO(zmanji): Remove this class and overhead in 0.8.0 (AURORA-906)

  private final Resources resources;

  /**
   * Extra CPU allocated for each executor.
   */
  @VisibleForTesting
  @CmdLine(name = "thermos_executor_cpu",
      help = "The number of CPU cores to allocate for each instance of the executor.")
  public static final Arg<Double> EXECUTOR_OVERHEAD_CPUS = Arg.create(0.25);

  /**
   * Extra RAM allocated for the executor.
   */
  @VisibleForTesting
  @CmdLine(name = "thermos_executor_ram",
      help = "The amount of RAM to allocate for each instance of the executor.")
  public static final Arg<Amount<Long, Data>> EXECUTOR_OVERHEAD_RAM =
      Arg.create(Amount.of(128L, Data.MB));

  private ResourceSlot(Resources r) {
    this.resources = r;
  }

  public static ResourceSlot from(ITaskConfig task) {
    return from(
        task.getNumCpus(),
        Amount.of(task.getRamMb(), Data.MB),
        Amount.of(task.getDiskMb(), Data.MB),
        task.getRequestedPorts().size());
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
  public static ResourceSlot from(
      double cpu,
      Amount<Long, Data> ram,
      Amount<Long, Data> disk,
      int ports) {
    double totalCPU = cpu + EXECUTOR_OVERHEAD_CPUS.get();
    Amount<Long, Data> totalRAM =
        Amount.of(ram.as(Data.MB) + EXECUTOR_OVERHEAD_RAM.get().as(Data.MB), Data.MB);

    return new ResourceSlot(new Resources(totalCPU, totalRAM, disk, ports));
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ResourceSlot)) {
      return false;
    }

    ResourceSlot other = (ResourceSlot) o;
    return resources.equals(other.resources);
  }

  @Override
  public int hashCode() {
    return resources.hashCode();
  }

  public static ResourceSlot sum(ResourceSlot... rs) {
    return sum(Arrays.asList(rs));
  }

  public static ResourceSlot sum(Iterable<ResourceSlot> rs) {
    Resources r = Resources.sum(Iterables.transform(rs, new Function<ResourceSlot, Resources>() {
      @Override
      public Resources apply(ResourceSlot input) {
        return input.resources;
      }
    }));

    return new ResourceSlot(r);
  }

  public static final Ordering<ResourceSlot> ORDER = new Ordering<ResourceSlot>() {
    @Override
    public int compare(ResourceSlot left, ResourceSlot right) {
      return Resources.RESOURCE_ORDER.compare(left.resources, right.resources);
    }
  };
}
