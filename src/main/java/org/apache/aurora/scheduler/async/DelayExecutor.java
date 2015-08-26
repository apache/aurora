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
package org.apache.aurora.scheduler.async;

import java.util.concurrent.Executor;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;

/**
 * An executor that supports executing work after a minimum time delay.
 */
public interface DelayExecutor extends Executor {

  /**
   * Executes {@code work} after no less than {@code minDelay}.
   *
   * @param work Work to execute.
   * @param minDelay Minimum amount of time to wait before executing the work.
   */
  void execute(Runnable work, Amount<Long, Time> minDelay);
}
