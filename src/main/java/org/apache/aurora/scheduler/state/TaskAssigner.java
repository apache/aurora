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
package org.apache.aurora.scheduler.state;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.base.MorePreconditions;

import org.apache.aurora.scheduler.HostOffer;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.filter.SchedulingFilter.UnusedResource;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.filter.SchedulingFilter.VetoGroup;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.mesos.Protos.TaskInfo;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import static org.apache.mesos.Protos.Offer;

/**
 * Responsible for matching a task against an offer.
 */
public interface TaskAssigner {

  final class Assignment {

    public enum Result {
      /**
       * Assignment successful.
       */
      SUCCESS,

      /**
       * Assignment failed.
       */
      FAILURE,

      /**
       * Assignment failed with static mismatch (i.e. all {@link Veto} instances group
       * as {@link VetoGroup}).
       * @see VetoGroup#STATIC
       */
      FAILURE_STATIC_MISMATCH,
    }

    private static final Optional<TaskInfo> NO_TASK_INFO = Optional.absent();
    private static final ImmutableSet<Veto> NO_VETOES = ImmutableSet.of();
    private final Optional<TaskInfo> taskInfo;
    private final Set<Veto> vetoes;

    private Assignment(Optional<TaskInfo> taskInfo, Set<Veto> vetoes) {
      this.taskInfo = taskInfo;
      this.vetoes = vetoes;
    }

    /**
     * Creates a successful assignment instance.
     *
     * @param taskInfo {@link TaskInfo} to launch.
     * @return A successful {@link Assignment}.
     */
    public static Assignment success(TaskInfo taskInfo) {
      return new Assignment(Optional.of(taskInfo), NO_VETOES);
    }

    /**
     * Creates a failed assignment instance with a set of {@link Veto} applied.
     *
     * @param vetoes Set of {@link Veto} instances issued for the failed offer/task match.
     * @return A failed {@link Assignment}.
     */
    public static Assignment failure(Set<Veto> vetoes) {
      return new Assignment(NO_TASK_INFO, MorePreconditions.checkNotBlank(vetoes));
    }

    /**
     * Creates a failed assignment instance.
     *
     * @return A failed {@link Assignment}.
     */
    public static Assignment failure() {
      return new Assignment(NO_TASK_INFO, NO_VETOES);
    }

    /**
     * Generates the {@link Result} based on the assignment details.
     *
     * @return An assignment {@link Result}.
     */
    public Result getResult() {
      if (taskInfo.isPresent()) {
        return Result.SUCCESS;
      }

      return Veto.identifyGroup(vetoes) == VetoGroup.STATIC
          ? Result.FAILURE_STATIC_MISMATCH
          : Result.FAILURE;
    }

    /**
     * A {@link TaskInfo} to launch.
     *
     * @return Optional of {@link TaskInfo}.
     */
    public Optional<TaskInfo> getTaskInfo() {
      return taskInfo;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Assignment)) {
        return false;
      }

      Assignment other = (Assignment) o;

      return Objects.equal(taskInfo, other.taskInfo)
          && Objects.equal(vetoes, other.vetoes);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(taskInfo, vetoes);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("taskInfo", taskInfo)
          .add("vetoes", vetoes)
          .toString();
    }
  }

  /**
   * Tries to match a task against an offer.  If a match is found, the assigner should
   * make the appropriate changes to the task and provide an {@link Assignment} result.
   *
   * @param storeProvider Storage provider.
   * @param offer The resource offer.
   * @param resourceRequest The request for resources being scheduled.
   * @return {@link Assignment} with assignment result.
   */
  Assignment maybeAssign(
      MutableStoreProvider storeProvider,
      HostOffer offer,
      ResourceRequest resourceRequest);

  class TaskAssignerImpl implements TaskAssigner {
    private static final Logger LOG = Logger.getLogger(TaskAssignerImpl.class.getName());

    private final StateManager stateManager;
    private final SchedulingFilter filter;
    private final MesosTaskFactory taskFactory;

    @Inject
    public TaskAssignerImpl(
        StateManager stateManager,
        SchedulingFilter filter,
        MesosTaskFactory taskFactory) {

      this.stateManager = requireNonNull(stateManager);
      this.filter = requireNonNull(filter);
      this.taskFactory = requireNonNull(taskFactory);
    }

    private TaskInfo assign(
        MutableStoreProvider storeProvider,
        Offer offer,
        Set<String> requestedPorts,
        String taskId) {

      String host = offer.getHostname();
      Set<Integer> selectedPorts = Resources.getPorts(offer, requestedPorts.size());
      Preconditions.checkState(selectedPorts.size() == requestedPorts.size());

      final Iterator<String> names = requestedPorts.iterator();
      Map<String, Integer> portsByName = FluentIterable.from(selectedPorts)
          .uniqueIndex(new Function<Object, String>() {
            @Override
            public String apply(Object input) {
              return names.next();
            }
          });

      IAssignedTask assigned = stateManager.assignTask(
          storeProvider,
          taskId,
          host,
          offer.getSlaveId(),
          portsByName);
      LOG.info(String.format("Offer on slave %s (id %s) is being assigned task for %s.",
          host, offer.getSlaveId().getValue(), taskId));
      return taskFactory.createFrom(assigned, offer.getSlaveId());
    }

    @Override
    public Assignment maybeAssign(
        MutableStoreProvider storeProvider,
        HostOffer offer,
        ResourceRequest resourceRequest) {

      Set<Veto> vetoes = filter.filter(
          new UnusedResource(ResourceSlot.from(offer.getOffer()), offer.getAttributes()),
          resourceRequest);
      if (vetoes.isEmpty()) {
        return Assignment.success(assign(
            storeProvider,
            offer.getOffer(),
            resourceRequest.getRequestedPorts(),
            resourceRequest.getTaskId()));
      } else {
        LOG.fine("Slave " + offer.getOffer().getHostname()
            + " vetoed task " + resourceRequest.getTaskId() + ": " + vetoes);
        return Assignment.failure(vetoes);
      }
    }
  }
}
