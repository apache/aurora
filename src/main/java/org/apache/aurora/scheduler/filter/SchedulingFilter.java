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

import com.google.common.annotations.VisibleForTesting;

import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.storage.entities.IConstraint;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

/**
 * Determines whether a proposed scheduling assignment should be allowed.
 */
public interface SchedulingFilter {

  /**
   * Reason for a proposed scheduling assignment to be filtered out.
   * A veto also contains a score, which is an opaque indicator as to how strong a veto is.  This
   * is only intended to be used for relative ranking of vetoes for determining which veto against
   * a scheduling assignment is 'weakest'.
   */
  class Veto {
    public static final int MAX_SCORE = 1000;

    private final String reason;
    private final int score;
    private final boolean valueMismatch;

    private Veto(String reason, int score, boolean valueMismatch) {
      this.reason = reason;
      this.score = Math.min(MAX_SCORE, score);
      this.valueMismatch = valueMismatch;
    }

    @VisibleForTesting
    public Veto(String reason, int score) {
      this(reason, score, false);
    }

    /**
     * Creates a special veto that represents a mismatch between the server and task's configuration
     * for an attribute.
     *
     * @param reason Information about the value mismatch.
     * @return A constraint mismatch veto.
     */
    public static Veto constraintMismatch(String reason) {
      return new Veto(reason, MAX_SCORE, true);
    }

    public String getReason() {
      return reason;
    }

    public int getScore() {
      return score;
    }

    public boolean isConstraintMismatch() {
      return valueMismatch;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Veto)) {
        return false;
      }

      Veto other = (Veto) o;
      return Objects.equals(reason, other.reason)
          && Objects.equals(score, other.score)
          && Objects.equals(valueMismatch, other.valueMismatch);
    }

    @Override
    public int hashCode() {
      return Objects.hash(reason, score, valueMismatch);
    }

    @Override
    public String toString() {
      return com.google.common.base.Objects.toStringHelper(this)
          .add("reason", reason)
          .add("score", score)
          .add("valueMismatch", valueMismatch)
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

    public ResourceSlot getResourceSlot() {
      return ResourceSlot.from(task);
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
