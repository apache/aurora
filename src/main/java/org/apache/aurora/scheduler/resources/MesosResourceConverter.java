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

import java.util.Set;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.aurora.scheduler.base.Numbers;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.Resource;

import static org.apache.aurora.scheduler.base.Numbers.RANGE_TRANSFORM;

/**
 * Converts Mesos resource values to be consumed in Aurora.
 */
public interface MesosResourceConverter {

  /**
   * Gets Mesos resource quantity.
   *
   * @param resource Mesos resource to quantify.
   * @return Mesos resource quantity.
   */
  Double quantify(Resource resource);

  /**
   * Allocates offer resources to resource request from {@code resourceRequest}.
   *
   * @param offerResources Offer resources to allocate.
   * @param resourceRequest Resource request.
   * @param isRevocable Flag indicating if allocated resources must be marked as Mesos-revocable.
   * @return Allocated Mesos resources.
   */
  Iterable<Resource> toMesosResource(
      Iterable<Resource.Builder> offerResources,
      Supplier<?> resourceRequest,
      boolean isRevocable);

  ScalarConverter SCALAR = new ScalarConverter();
  RangeConverter RANGES = new RangeConverter();

  class ScalarConverter implements MesosResourceConverter {
    /**
     * Helper function to check if a resource value is small enough to be considered zero.
     */
    private static boolean nearZero(double value) {
      return Math.abs(value) < 1e-6;
    }

    @Override
    public Double quantify(Resource resource) {
      return resource.getScalar().getValue();
    }

    @Override
    public Iterable<Resource> toMesosResource(
        Iterable<Resource.Builder> offerResources,
        Supplier<?> resourceRequest,
        boolean isRevocable) {

      double remaining = (Double) resourceRequest.get();
      ImmutableList.Builder<Resource> result = ImmutableList.builder();
      for (Resource.Builder offerResource : offerResources) {
        if (nearZero(remaining)) {
          break;
        }

        final double available = offerResource.getScalar().getValue();
        if (nearZero(available)) {
          // Skip resource slot that is already used up.
          continue;
        }

        final double used = Math.min(remaining, available);
        remaining -= used;
        Resource.Builder newResource =
            Resource.newBuilder(offerResource.build())
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(used).build());

        if (isRevocable) {
          newResource.setRevocable(Resource.RevocableInfo.newBuilder());
        }

        result.add(newResource.build());
        offerResource.getScalarBuilder().setValue(available - used);
      }
      if (!nearZero(remaining)) {
        // NOTE: this will not happen as long as Veto logic from TaskAssigner.maybeAssign is
        // consistent.
        // Maybe we should consider implementing resource veto with this class to ensure that.
        throw new ResourceManager.InsufficientResourcesException(
            "Insufficient resource when allocating from offer");
      }
      return result.build();
    }
  }

  class RangeConverter implements MesosResourceConverter {
    @Override
    public Double quantify(Resource resource) {
      return resource.getRanges().getRangeList().stream()
          .map(range -> 1 + range.getEnd() - range.getBegin())
          .reduce((l, r) -> l + r)
          .map(Long::doubleValue)
          .orElse(0.0);
    }

    @Override
    public Iterable<Resource> toMesosResource(
        Iterable<Resource.Builder> offerResources,
        Supplier<?> resourceRequest,
        boolean isRevocable) {

      @SuppressWarnings("unchecked")
      Set<Integer> leftOver = Sets.newHashSet((Set<Integer>) resourceRequest.get());
      ImmutableList.Builder<Resource> result = ImmutableList.builder();
      for (Resource.Builder r : offerResources) {
        Set<Integer> fromResource = Sets.newHashSet(Iterables.concat(
            Iterables.transform(r.getRanges().getRangeList(), Numbers.RANGE_TO_MEMBERS)));
        Set<Integer> available = Sets.newHashSet(Sets.intersection(leftOver, fromResource));
        if (available.isEmpty()) {
          continue;
        }

        Resource.Builder newResource = Protos.Resource.newBuilder(r.build())
            .setRanges(Protos.Value.Ranges.newBuilder()
                .addAllRange(Iterables.transform(Numbers.toRanges(available), RANGE_TRANSFORM)));

        if (isRevocable) {
          newResource.setRevocable(Resource.RevocableInfo.newBuilder());
        }

        result.add(newResource.build());
        leftOver.removeAll(available);
        if (leftOver.isEmpty()) {
          break;
        }
      }
      if (!leftOver.isEmpty()) {
        // NOTE: this will not happen as long as Veto logic from TaskAssigner.maybeAssign is
        // consistent.
        // Maybe we should consider implementing resource veto with this class to ensure that.
        throw new ResourceManager.InsufficientResourcesException(
            "Insufficient resource for range type when allocating from offer");
      }
      return result.build();
    }
  }
}
