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
package org.apache.aurora.scheduler.cron.quartz;

import javax.inject.Inject;
import javax.inject.Provider;

import org.quartz.Job;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkState;

/**
* Adapter that allows AuroraCronJobs to be constructed by Guice instead of directly by quartz.
*/
class AuroraCronJobFactory implements JobFactory {
  private final Provider<AuroraCronJob> auroraCronJobProvider;

  @Inject
  AuroraCronJobFactory(Provider<AuroraCronJob> auroraCronJobProvider) {
    this.auroraCronJobProvider = requireNonNull(auroraCronJobProvider);
  }

  @Override
  public Job newJob(TriggerFiredBundle bundle, Scheduler scheduler) throws SchedulerException {
    checkState(AuroraCronJob.class.equals(bundle.getJobDetail().getJobClass()),
        "Quartz tried to run a type of job we don't know about: %s",
        bundle.getJobDetail().getJobClass());

    return auroraCronJobProvider.get();
  }
}
