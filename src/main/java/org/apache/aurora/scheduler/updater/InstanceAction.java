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

import java.util.Optional;

import static org.apache.aurora.scheduler.updater.InstanceActionHandler.AddTask;
import static org.apache.aurora.scheduler.updater.InstanceActionHandler.KillTask;
import static org.apache.aurora.scheduler.updater.InstanceActionHandler.WatchRunningTask;

enum InstanceAction {
  KILL_TASK_AND_RESERVE(Optional.of(new KillTask(true))),
  KILL_TASK(Optional.of(new KillTask(false))),
  // TODO(wfarner): Build this action into the scheduler state machine instead.  Rather than
  // killing a task and re-recreating it, choose the updated or rolled-back task when we are
  // deciding to reschedule the task.
  ADD_TASK(Optional.of(new AddTask())),
  WATCH_TASK(Optional.of(new WatchRunningTask())),
  AWAIT_STATE_CHANGE(Optional.empty());

  private final Optional<InstanceActionHandler> handler;

  InstanceAction(Optional<InstanceActionHandler> handler) {
    this.handler = handler;
  }

  public Optional<InstanceActionHandler> getHandler() {
    return handler;
  }
}
