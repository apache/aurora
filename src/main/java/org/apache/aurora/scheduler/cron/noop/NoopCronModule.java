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
package org.apache.aurora.scheduler.cron.noop;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;

import org.apache.aurora.scheduler.cron.CronPredictor;
import org.apache.aurora.scheduler.cron.CronScheduler;

/**
 * A Module to wire up a cron scheduler that does not actually schedule cron jobs.
 *
 * This class exists as a short term hack to get around a license compatibility issue - Real
 * Implementation (TM) coming soon.
 */
public class NoopCronModule extends AbstractModule {
  @Override
  protected void configure() {
    bind(CronScheduler.class).to(NoopCronScheduler.class);
    bind(NoopCronScheduler.class).in(Singleton.class);

    bind(CronPredictor.class).to(NoopCronPredictor.class);
    bind(NoopCronPredictor.class).in(Singleton.class);
  }
}
