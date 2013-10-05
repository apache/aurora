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
package com.twitter.aurora.scheduler.storage.entities;

import com.google.common.base.Function;

import com.twitter.aurora.gen.CronCollisionPolicy;
import com.twitter.aurora.gen.JobConfiguration;

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 * <p>
 * Yes, you're right, it shouldn't be checked in.  We'll get there, I promise.
 */
public class IJobConfiguration {
  private final JobConfiguration wrapped;

  public IJobConfiguration(JobConfiguration wrapped) {
    this.wrapped = wrapped.deepCopy();
  }

  public static final Function<IJobConfiguration, JobConfiguration> TO_BUILDER =
      new Function<IJobConfiguration, JobConfiguration>() {
        @Override
        public JobConfiguration apply(IJobConfiguration input) {
          return input.newBuilder();
        }
      };

  public static final Function<JobConfiguration, IJobConfiguration> FROM_BUILDER =
      new Function<JobConfiguration, IJobConfiguration>() {
        @Override
        public IJobConfiguration apply(JobConfiguration input) {
          return new IJobConfiguration(input);
        }
      };

  public JobConfiguration newBuilder() {
    return wrapped.deepCopy();
  }

  public IJobKey getKey() {
    return new IJobKey(wrapped.getKey());
  }

  public IIdentity getOwner() {
    return new IIdentity(wrapped.getOwner());
  }

  public String getCronSchedule() {
    return wrapped.getCronSchedule();
  }

  public CronCollisionPolicy getCronCollisionPolicy() {
    return wrapped.getCronCollisionPolicy();
  }

  public ITaskConfig getTaskConfig() {
    return new ITaskConfig(wrapped.getTaskConfig());
  }

  public int getShardCount() {
    return wrapped.getShardCount();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IJobConfiguration)) {
      return false;
    }
    IJobConfiguration other = (IJobConfiguration) o;
    return wrapped.equals(other.wrapped);
  }

  @Override
  public int hashCode() {
    return wrapped.hashCode();
  }

  @Override
  public String toString() {
    return wrapped.toString();
  }
}
