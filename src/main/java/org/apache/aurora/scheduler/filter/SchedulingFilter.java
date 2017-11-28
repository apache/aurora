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
package org.apache.aurora.scheduler.filter;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;

import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.storage.entities.IConstraint;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static org.apache.aurora.scheduler.filter.SchedulingFilter.VetoType.CONSTRAINT_MISMATCH;
import static org.apache.aurora.scheduler.filter.SchedulingFilter.VetoType.INSUFFICIENT_RESOURCES;
import static org.apache.aurora.scheduler.filter.SchedulingFilter.VetoType.LIMIT_NOT_SATISFIED;
import static org.apache.aurora.scheduler.filter.SchedulingFilter.VetoType.MAINTENANCE;

/**
 * Determines whether a proposed scheduling assignment should be allowed.
 */
public interface SchedulingFilter {

  enum VetoGroup {
    /**
     * An empty group of {@link Veto} instances.
     */
    EMPTY,

    /**
     * Represents a group of static {@link Veto} instances. Vetoes are considered static if
     * a given offer will never be able to satisfy given task requirements.
     */
    STATIC,

    /**
     * Represents a group of dynamic {@link Veto} instances. Vetoes are considered dynamic if
     * a given offer may be able to satisfy given task requirements if cluster conditions change
     * (e.g. other tasks are killed or rescheduled).
     */
    DYNAMIC,

    /**
     * Represents a group of both static and dynamic {@link Veto} instances.
     */
    MIXED
  }

  enum VetoType {
    /**
     * Not enough resources to satisfy a proposed scheduling assignment.
     */
    INSUFFICIENT_RESOURCES("Insufficient: %s", VetoGroup.STATIC, (int) Math.pow(10, 3)),

    /**
     * Unable to satisfy proposed scheduler assignment constraints.
     */
    CONSTRAINT_MISMATCH("Constraint not satisfied: %s", VetoGroup.STATIC, (int) Math.pow(10, 4)),

    /**
     * Constraint limit is not satisfied for a proposed scheduling assignment.
     */
    LIMIT_NOT_SATISFIED("Limit not satisfied: %s", VetoGroup.DYNAMIC, (int) Math.pow(10, 4)),

    /**
     * Unable to satisfy a proposed scheduler assignment due to cluster maintenance.
     */
    MAINTENANCE("Host %s for maintenance", VetoGroup.STATIC, (int) Math.pow(10, 5)),

    /**
     * Unable to satisfy proposed scheduler assignment dedicated host constraint.
     */
    DEDICATED_CONSTRAINT_MISMATCH("Host is dedicated", VetoGroup.STATIC, (int) Math.pow(10, 6));

    private final String reasonFormat;
    private final VetoGroup group;
    private final int score;

    VetoType(String reasonFormat, VetoGroup group, int score) {
      this.reasonFormat = reasonFormat;
      this.group = group;

      // Score ranges are chosen under assumption that no more than 9 same group score vetoes
      // are applied per task/offer evaluation (e.g. 9 insufficient resource vetoes < 1 value
      // constraint mismatch). This is a fair assumption given current "break early" constraint
      // evaluation rules and a limited number of resource types (4). Should this ever change,
      // consider more spread between score groups to ensure correct NearestFit operation.
      this.score = score;
    }

    String formatReason(String reason) {
      return String.format(reasonFormat, reason);
    }

    VetoGroup getGroup() {
      return group;
    }

    int getScore() {
      return score;
    }
  }

  /**
   * Reason for a proposed scheduling assignment to be filtered out.
   * A veto also contains a score, which is an opaque indicator as to how strong a veto is.  This
   * is only intended to be used for relative ranking of vetoes for determining which veto against
   * a scheduling assignment is 'weakest'.
   */
  final class Veto {
    private final VetoType vetoType;
    private final String reason;
    private final int score;

    private Veto(VetoType vetoType, String reasonParameter, int score) {
      this.vetoType = vetoType;
      this.reason = vetoType.formatReason(reasonParameter);
      this.score = score;
    }

   /**
    * Creates a veto that represents an dedicated host constraint mismatch between the server
    * and task's configuration for an attribute.
    *
    * @return A dedicated host constraint mismatch veto.
    */
    public static Veto dedicatedHostConstraintMismatch() {
      return new Veto(
          VetoType.DEDICATED_CONSTRAINT_MISMATCH,
          "",
          VetoType.DEDICATED_CONSTRAINT_MISMATCH.getScore());
    }

    /**
     * Creates a veto that represents a mismatch between the server and task's configuration
     * for an attribute.
     *
     * @param constraint A constraint name.
     * @return A constraint mismatch veto.
     */
    public static Veto constraintMismatch(String constraint) {
      return new Veto(CONSTRAINT_MISMATCH, constraint, CONSTRAINT_MISMATCH.getScore());
    }

    /**
     * Creates a veto the represents an unsatisfied constraint limit between the server and task's
     * configuration for an attribute.
     *
     * @param limit A constraint name.
     * @return A unsatisfied limit veto.
     */
    public static Veto unsatisfiedLimit(String limit) {
      return new Veto(LIMIT_NOT_SATISFIED, limit, LIMIT_NOT_SATISFIED.getScore());
    }

    /**
     * Creates a veto that represents an inability of the server to satisfy task's configuration
     * resource requirements.
     *
     * @param resource A resource name.
     * @param score A veto score.
     * @return An insufficient resources veto.
     */
    public static Veto insufficientResources(String resource, int score) {
      return new Veto(INSUFFICIENT_RESOURCES, resource, score);
    }

    /**
     * Creates a veto that represents a lack of suitable for assignment hosts due to cluster
     * maintenance.
     *
     * @param maintenanceMode A maintenance mode.
     * @return A maintenance veto.
     */
    public static Veto maintenance(String maintenanceMode) {
      return new Veto(MAINTENANCE, maintenanceMode, MAINTENANCE.getScore());
    }

    public String getReason() {
      return reason;
    }

    public int getScore() {
      return score;
    }

    public VetoType getVetoType() {
      return vetoType;
    }

    public static VetoGroup identifyGroup(Iterable<Veto> vetoes) {
      // This code has high call frequency and is optimized for reduced heap churn.
      VetoGroup group = VetoGroup.EMPTY;
      for (Veto veto : vetoes) {
        VetoGroup currentGroup = veto.getVetoType().getGroup();

        if (group == VetoGroup.EMPTY) {
          group = currentGroup;
        } else if (!currentGroup.equals(group)) {
          return VetoGroup.MIXED;
        }
      }
      return group;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Veto)) {
        return false;
      }

      Veto other = (Veto) o;
      return Objects.equals(vetoType, other.vetoType)
          && Objects.equals(reason, other.reason)
          && Objects.equals(score, other.score);
    }

    @Override
    public int hashCode() {
      return Objects.hash(vetoType, reason, score);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("vetoType", vetoType)
          .add("reason", reason)
          .add("score", score)
          .toString();
    }
  }

  /**
   * An available resource in the cluster.
   */
  class UnusedResource {
    private final ResourceBag offer;
    private final IHostAttributes attributes;
    private final Optional<Instant> unavailabilityStart;

    @VisibleForTesting
    public UnusedResource(ResourceBag offer, IHostAttributes attributes) {
      this(offer, attributes, Optional.absent());
    }

    public UnusedResource(HostOffer offer, boolean revocable) {
      this(offer.getResourceBag(revocable), offer.getAttributes(), offer.getUnavailabilityStart());
    }

    public UnusedResource(ResourceBag offer, IHostAttributes attributes, Optional<Instant> start) {
      this.offer = offer;
      this.attributes = attributes;
      this.unavailabilityStart = start;
    }

    public ResourceBag getResourceBag() {
      return offer;
    }

    public IHostAttributes getAttributes() {
      return attributes;
    }

    public Optional<Instant> getUnavailabilityStart() {
      return unavailabilityStart;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof UnusedResource)) {
        return false;
      }

      UnusedResource other = (UnusedResource) o;
      return Objects.equals(offer, other.offer)
          && Objects.equals(attributes, other.attributes)
          && Objects.equals(unavailabilityStart, other.unavailabilityStart);
    }

    @Override
    public int hashCode() {
      return Objects.hash(offer, attributes, unavailabilityStart);
    }
  }

  /**
   * A request for resources in the cluster.
   */
  class ResourceRequest {
    private final ITaskConfig task;
    private final ResourceBag request;
    private final AttributeAggregate jobState;

    public ResourceRequest(ITaskConfig task, ResourceBag request, AttributeAggregate jobState) {
      this.task = task;
      this.request = request;
      this.jobState = jobState;
    }

    public Iterable<IConstraint> getConstraints() {
      return task.getConstraints();
    }

    public ITaskConfig getTask() {
      return task;
    }

    public ResourceBag getResourceBag() {
      return request;
    }

    public AttributeAggregate getJobState() {
      return jobState;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof ResourceRequest)) {
        return false;
      }

      ResourceRequest other = (ResourceRequest) o;
      return Objects.equals(task, other.task)
          && Objects.equals(request, other.request)
          && Objects.equals(jobState, other.jobState);
    }

    @Override
    public int hashCode() {
      return Objects.hash(task, request, jobState);
    }
  }

  /**
   * Applies a task against the filter with the given resources, and on the host.
   *
   * @param resource An available resource in the cluster.
   * @param request A resource request to match against the {@code resource}.
   * @return A set of vetoes indicating reasons the task cannot be scheduled.  If the task may be
   *    scheduled, the set will be empty.
   */
  Set<Veto> filter(UnusedResource resource, ResourceRequest request);
}
