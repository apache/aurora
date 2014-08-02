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
package org.apache.aurora.scheduler.updater;

/**
 * Controls that an instance updater uses to modify the job instance it is updating, or advertise
 * that it has reached a result.
 */
interface TaskController {
  /**
   * Terminates the instance, ensuring that it is not automatically restarted.
   * <p>
   * If the implementation wishes to time out on the kill attempt, it should schedule an action
   * to invoke {@link InstanceUpdater#evaluate(com.google.common.base.Optional)} after a delay.
   */
  void killTask();

  /**
   * Adds a task with the new configuration to replace the old task.
   * <p>
   * If the implementation wishes to time out on the task replacement (such as to deal with a
   * task stuck in {@link org.apache.aurora.gen.ScheduleStatus#PENDING}, it should schedule an
   * action to invoke {@link InstanceUpdater#evaluate(com.google.common.base.Optional)} after a
   * delay.
   */
  void addReplacement();

  /**
   * Requests that the updater be re-evaluated after the amount of time a task must remain in
   * {@link org.apache.aurora.gen.ScheduleStatus#RUNNING} to be considered stable. The re-evaluation
   * should be after the same amount of time as {@code minRunningTime}.
   */
  void reevaluteAfterRunningLimit();

  /**
   * Announces a result of the attempt to update the instance.
   * <p>
   * Once this callback has been made, the updater will no-op on any subsequent evaluations.
   *
   * @param status Update result.
   */
  void updateCompleted(InstanceUpdater.Result status);
}
