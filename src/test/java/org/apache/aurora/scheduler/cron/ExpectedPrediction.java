/**
 * Copyright 2014 Apache Software Foundation
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
package org.apache.aurora.scheduler.cron;

import java.io.InputStreamReader;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * A schedule and the expected iteratively-applied prediction results.
 */
public final class ExpectedPrediction {
  private String schedule;
  private List<Long> triggerTimes;

  ExpectedPrediction() {
    // GSON constructor.
  }

  public static List<ExpectedPrediction> getAll() {
    return new Gson()
        .fromJson(
            new InputStreamReader(
                ExpectedPrediction.class.getResourceAsStream("expected-predictions.json"),
                Charsets.UTF_8),
            new TypeToken<List<ExpectedPrediction>>() { }.getType());
  }

  public String getSchedule() {
    return schedule;
  }

  public List<Long> getTriggerTimes() {
    return ImmutableList.copyOf(triggerTimes);
  }

  public CrontabEntry parseCrontabEntry() {
    return CrontabEntry.parse(getSchedule());
  }
}
