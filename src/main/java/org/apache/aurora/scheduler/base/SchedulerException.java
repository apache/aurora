/**
 * Copyright 2013 Apache Software Foundation
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
 * Indicates some form of unexpected scheduler exception.
 */
public class SchedulerException extends RuntimeException {
  public SchedulerException(String message) {
    super(message);
  }
  public SchedulerException(String message, Throwable cause) {
    super(message, cause);
  }
  public SchedulerException(Throwable cause) {
    super(cause);
  }
}
