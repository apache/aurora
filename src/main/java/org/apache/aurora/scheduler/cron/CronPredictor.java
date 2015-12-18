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
package org.apache.aurora.scheduler.cron;

import java.util.Date;

import com.google.common.base.Optional;

/**
 * A utility function that predicts a cron run given a schedule.
 */
public interface CronPredictor {
  /**
   * Predicts the next date at which a cron schedule will trigger.
   * <p>
   * NB: Some cron schedules can predict a run at an invalid date (eg: too far in the future); and
   * it's these predictions that will result in an absent result.
   *
   * @param schedule Cron schedule to predict the next time for.
   * @return A prediction for the next time a cron will run if a valid prediction can be made.
   */
  Optional<Date> predictNextRun(CrontabEntry schedule);
}
