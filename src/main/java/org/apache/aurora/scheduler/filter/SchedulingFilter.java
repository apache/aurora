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

import java.util.Objects;
import java.util.Set;

import org.apache.aurora.scheduler.ResourceSlot;
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

  enum VetoType {
    /**
     * Not enough resources to satisfy a proposed scheduling assignment.
     */
    INSUFFICIENT_RESOURCES("Insufficient: %s"),

    /**
     * Unable to satisfy proposed scheduler assignment constraints.
     */
    CONSTRAINT_MISMATCH("Constraint not satisfied: %s"),

    /**
     * Constraint limit is not satisfied for a proposed scheduling assignment.
     */
    LIMIT_NOT_SATISFIED("Limit not satisfied: %s"),

    /**
     * Unable to satisfy a proposed scheduler assignment due to cluster maintenance.
     */
    MAINTENANCE("Host %s for maintenance");

    private final String reasonFormat;

    VetoType(String reasonFormat) {
      this.reasonFormat = reasonFormat;
    }

    String formatReason(String reason) {
      return String.format(reasonFormat, reason);
    }
  }

  /**
   * Reason for a proposed scheduling assignment to be filtered out.
   * A veto also contains a score, which is an opaque indicator as to how strong a veto is.  This
   * is only intended to be used for relative ranking of vetoes for determining which veto against
   * a scheduling assignment is 'weakest'.
   */
  final class Veto {
    public static final int MAX_SCORE = 1000;

    private final VetoType vetoType;
    private final String reason;
    private final int score;

    private Veto(VetoType vetoType, String reasonParameter, int score) {
      this.vetoType = vetoType;
      this.reason = vetoType.formatReason(reasonParameter);
      this.score = Math.min(MAX_SCORE, score);
    }

    /**
     * Creates a veto that represents a mismatch between the server and task's configuration
     * for an attribute.
     *
     * @param constraint A constraint name.
     * @return A constraint mismatch veto.
     */
    public static Veto constraintMismatch(String constraint) {
      return new Veto(CONSTRAINT_MISMATCH, constraint, MAX_SCORE);
    }

    /**
     * Creates a veto the represents an unsatisfied constraint limit between the server and task's
     * configuration for an attribute.
     *
     * @param limit A constraint name.
     * @return A unsatisfied limit veto.
     */
    public static Veto unsatisfiedLimit(String limit) {
      return new Veto(LIMIT_NOT_SATISFIED, limit, MAX_SCORE);
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
      return new Veto(MAINTENANCE, maintenanceMode, MAX_SCORE);
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
      return com.google.common.base.Objects.toStringHelper(this)
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
    private final ResourceSlot offer;
    private final IHostAttributes attributes;

    public UnusedResource(ResourceSlot offer, IHostAttributes attributes) {
      this.offer = offer;
      this.attributes = attributes;
    }

    public ResourceSlot getResourceSlot() {
      return offer;
    }

    public IHostAttributes getAttributes() {
      return attributes;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof UnusedResource)) {
        return false;
      }

      UnusedResource other = (UnusedResource) o;
      return Objects.equals(getResourceSlot(), other.getResourceSlot())
          && Objects.equals(getAttributes(), other.getAttributes());
    }

    @Override
    public int hashCode() {
      return Objects.hash(offer, attributes);
    }
  }

  /**
   * A request for resources in the cluster.
   */
  class ResourceRequest {
    private final ITaskConfig task;
    private final String taskId;
    private final AttributeAggregate jobState;

    public ResourceRequest(ITaskConfig task, String taskId, AttributeAggregate jobState) {
      this.task = task;
      this.taskId = taskId;
      this.jobState = jobState;
    }

    public Iterable<IConstraint> getConstraints() {
      return task.getConstraints();
    }

    public ITaskConfig getTask() {
      return task;
    }

    public AttributeAggregate getJobState() {
      return jobState;
    }

    public String getTaskId() {
      return taskId;
    }

    public int getNumRequestedPorts() {
      return task.getRequestedPorts().size();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof ResourceRequest)) {
        return false;
      }

      ResourceRequest other = (ResourceRequest) o;
      return Objects.equals(task, other.task)
          && Objects.equals(getTaskId(), other.getTaskId())
          && Objects.equals(getJobState(), other.getJobState());
    }

    @Override
    public int hashCode() {
      return Objects.hash(task, taskId, jobState);
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
