/*
 * Copyright 2013 Twitter, Inc.
 *
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
package com.twitter.aurora.scheduler.state;

/**
 * Descriptions of the different types of external work commands that task state machines may
 * trigger.
 */
enum WorkCommand {
  // Send an instruction for the runner of this task to kill the task.
  KILL,
  // Create a new state machine with a copy of this task.
  RESCHEDULE,
  // Update the task's state (schedule status) in the persistent store to match the state machine.
  UPDATE_STATE,
  // Delete this task from the persistent store.
  DELETE,
  // Increment the failure count for this task.
  INCREMENT_FAILURES
}
