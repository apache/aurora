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
package com.twitter.aurora.scheduler.cron.noop;

import java.util.Date;

import com.twitter.aurora.scheduler.cron.CronPredictor;

/**
 * A cron predictor that always suggests that the next run is Unix epoch time.
 *
 * This class exists as a short term hack to get around a license compatibility issue - Real
 * Implementation (TM) coming soon.
 */
class NoopCronPredictor implements CronPredictor {
  @Override
  public Date predictNextRun(String schedule) {
    return new Date(0);
  }
}
