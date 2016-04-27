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
package org.apache.aurora.scheduler.resources;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;

import static org.apache.aurora.gen.Resource.diskMb;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.gen.Resource.ramMb;

/**
 * Convenience methods for working with resources.
 */
public final class ResourceTestUtil {

  private ResourceTestUtil() {
    // Utility class.
  }

  public static IResourceAggregate aggregate(double numCpus, long ramMb, long diskMb) {
    return IResourceAggregate.build(new ResourceAggregate(numCpus, ramMb, diskMb, ImmutableSet.of(
        numCpus(numCpus),
        ramMb(ramMb),
        diskMb(diskMb)
    )));
  }

  public static IResourceAggregate nonBackfilledAggregate(double numCpus, long ramMb, long diskMb) {
    return IResourceAggregate.build(new ResourceAggregate()
        .setNumCpus(numCpus)
        .setRamMb(ramMb)
        .setDiskMb(diskMb));
  }
}
