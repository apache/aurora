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
package org.apache.aurora.scheduler.http;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;

import org.apache.aurora.scheduler.MesosTaskFactory.MesosTaskFactoryImpl;
import org.apache.aurora.scheduler.http.SchedulerzRole.Job;
import org.apache.aurora.scheduler.storage.entities.IJobKey;

/**
 * Utility class to hold common display helper functions.
 */
public final class DisplayUtils {

  @CmdLine(name = "viz_job_url_prefix", help = "URL prefix for job container stats.")
  private static final Arg<String> VIZ_JOB_URL_PREFIX = Arg.create("");

  private DisplayUtils() {
    // Utility class.
  }

  static final Ordering<Job> JOB_ORDERING = Ordering.natural().onResultOf(
      new Function<Job, String>() {
        @Override public String apply(Job job) {
          return job.getName();
        }
      });

  static String getJobDashboardUrl(IJobKey jobKey) {
    return VIZ_JOB_URL_PREFIX.get() + MesosTaskFactoryImpl.getJobSourceName(jobKey);
  }
}
