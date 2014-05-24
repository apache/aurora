/**
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
package org.apache.aurora.scheduler.base;

/**
 * Exception class to signal a failure to schedule a task or job.
 */
public class ScheduleException extends Exception {
  public ScheduleException(String msg) {
    super(msg);
  }

  public ScheduleException(String msg, Throwable t) {
    super(msg, t);
  }

  public ScheduleException(Throwable t) {
    super(t);
  }
}
