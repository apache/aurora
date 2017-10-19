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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.gen.Resource;
import org.apache.aurora.gen.ResourceAggregate;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.Numbers;
import org.apache.aurora.scheduler.storage.entities.IResource;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.Value.Type;

import static org.apache.aurora.gen.Resource.diskMb;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.gen.Resource.ramMb;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.fromResource;

/**
 * Convenience methods for working with resources.
 */
public final class ResourceTestUtil {

  private ResourceTestUtil() {
    // Utility class.
  }

  public static ResourceBag bag(Map<ResourceType, Double> resources) {
    return new ResourceBag(resources);
  }

  public static ResourceBag bag(double numCpus, long ramMb, long diskMb) {
    return ResourceManager.bagFromAggregate(aggregate(numCpus, ramMb, diskMb));
  }

  public static IResourceAggregate aggregate(double numCpus, long ramMb, long diskMb) {
    return IResourceAggregate.build(new ResourceAggregate(numCpus, ramMb, diskMb, ImmutableSet.of(
        numCpus(numCpus),
        ramMb(ramMb),
        diskMb(diskMb)
    )));
  }

  public static ITaskConfig resetPorts(ITaskConfig config, Set<String> portNames) {
    TaskConfig builder = config.newBuilder();
    builder.getResources().removeIf(e -> fromResource(IResource.build(e)).equals(PORTS));
    portNames.forEach(e -> builder.addToResources(Resource.namedPort(e)));
    return ITaskConfig.build(builder);
  }

  public static ITaskConfig resetResource(ITaskConfig config, ResourceType type, Double value) {
    TaskConfig builder = config.newBuilder();
    builder.getResources().removeIf(e -> fromResource(IResource.build(e)).equals(type));
    builder.addToResources(IResource.newBuilder(
        type.getValue(),
        type.getAuroraResourceConverter().valueOf(value)));
    return ITaskConfig.build(builder);
  }

  public static Protos.Resource mesosScalar(ResourceType type, double value) {
    return mesosScalar(type, Optional.absent(), false, value);
  }

  public static Protos.Resource mesosScalar(ResourceType type, double value, boolean revocable) {
    return mesosScalar(type, Optional.absent(), revocable, value);
  }

  public static Protos.Resource mesosScalar(
      ResourceType type,
      Optional<String> role,
      boolean revocable,
      double value) {

    return resourceBuilder(type, role, revocable)
        .setScalar(Protos.Value.Scalar.newBuilder().setValue(value).build())
        .build();
  }

  public static Protos.Resource mesosRange(ResourceType type, Integer... values) {
    return mesosRange(type, Optional.absent(), values);
  }

  public static Protos.Resource mesosRange(
      ResourceType type,
      Optional<String> role,
      Integer... values) {

    return resourceBuilder(type, role, false)
        .setRanges(Protos.Value.Ranges.newBuilder().addAllRange(
            Iterables.transform(
                Numbers.toRanges(ImmutableSet.copyOf(values)),
                Numbers.RANGE_TRANSFORM)))
        .build();
  }

  public static Protos.Resource mesosRange(
      ResourceType type,
      Optional<String> role,
      Iterable<Integer> values) {

    return resourceBuilder(type, role, false)
        .setRanges(Protos.Value.Ranges.newBuilder().addAllRange(
            Iterables.transform(Numbers.toRanges(values), Numbers.RANGE_TRANSFORM)))
        .build();
  }

  public static Iterable<Protos.Resource> mesosScalarFromBag(ResourceBag bag) {
    return bag.streamResourceVectors()
        .map(entry -> mesosScalar(entry.getKey(), entry.getValue()))
        .collect(Collectors.toSet());
  }

  public static Protos.Offer offer(Protos.Resource... resources) {
    return offer("slave-id", resources);
  }

  public static Protos.Offer offer(String agentId, Protos.Resource... resources) {
    return Protos.Offer.newBuilder()
        .setId(Protos.OfferID.newBuilder().setValue("offer-id-" + agentId))
        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework-id"))
        .setAgentId(Protos.AgentID.newBuilder().setValue(agentId))
        .setHostname("hostname")
        .addAllResources(ImmutableSet.copyOf(resources)).build();
  }

  private static Protos.Resource.Builder resourceBuilder(
      ResourceType type,
      Optional<String> role,
      boolean revocable) {

    Protos.Resource.Builder builder = Protos.Resource.newBuilder()
        .setType(type.equals(PORTS) ? Type.RANGES : Type.SCALAR)
        .setName(type.getMesosName());

    if (revocable) {
      builder.setRevocable(Protos.Resource.RevocableInfo.getDefaultInstance());
    }

    if (role.isPresent()) {
      builder.setRole(role.get());
    }

    return builder;
  }
}
