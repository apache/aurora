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
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.twitter.aurora.gen.CronCollisionPolicy;
import com.twitter.aurora.gen.JobConfiguration;

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 * <p>
 * Yes, you're right, it shouldn't be checked in.  We'll get there, I promise.
 */
public final class IJobConfiguration {
  private final JobConfiguration wrapped;
  private final IJobKey key;
  private final IIdentity owner;
  private final ITaskConfig taskConfig;

  private IJobConfiguration(JobConfiguration wrapped) {
    this.wrapped = Preconditions.checkNotNull(wrapped);
    this.key = !wrapped.isSetKey()
        ? null
        : IJobKey.buildNoCopy(wrapped.getKey());
    this.owner = !wrapped.isSetOwner()
        ? null
        : IIdentity.buildNoCopy(wrapped.getOwner());
    this.taskConfig = !wrapped.isSetTaskConfig()
        ? null
        : ITaskConfig.buildNoCopy(wrapped.getTaskConfig());
  }

  static IJobConfiguration buildNoCopy(JobConfiguration wrapped) {
    return new IJobConfiguration(wrapped);
  }

  public static IJobConfiguration build(JobConfiguration wrapped) {
    return buildNoCopy(wrapped.deepCopy());
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

  public static ImmutableList<JobConfiguration> toBuildersList(Iterable<IJobConfiguration> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toList();
  }

  public static ImmutableList<IJobConfiguration> listFromBuilders(Iterable<JobConfiguration> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toList();
  }

  public static ImmutableSet<JobConfiguration> toBuildersSet(Iterable<IJobConfiguration> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toSet();
  }

  public static ImmutableSet<IJobConfiguration> setFromBuilders(Iterable<JobConfiguration> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toSet();
  }

  public JobConfiguration newBuilder() {
    return wrapped.deepCopy();
  }

  public boolean isSetKey() {
    return wrapped.isSetKey();
  }

  public IJobKey getKey() {
    return key;
  }

  public boolean isSetOwner() {
    return wrapped.isSetOwner();
  }

  public IIdentity getOwner() {
    return owner;
  }

  public boolean isSetCronSchedule() {
    return wrapped.isSetCronSchedule();
  }

  public String getCronSchedule() {
    return wrapped.getCronSchedule();
  }

  public CronCollisionPolicy getCronCollisionPolicy() {
    return wrapped.getCronCollisionPolicy();
  }

  public boolean isSetTaskConfig() {
    return wrapped.isSetTaskConfig();
  }

  public ITaskConfig getTaskConfig() {
    return taskConfig;
  }

  public boolean isSetInstanceCount() {
    return wrapped.isSetInstanceCount();
  }

  public int getInstanceCount() {
    return wrapped.getInstanceCount();
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
