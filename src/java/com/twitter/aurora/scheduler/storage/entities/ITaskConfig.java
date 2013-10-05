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

import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.twitter.aurora.gen.TaskConfig;

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 * <p>
 * Yes, you're right, it shouldn't be checked in.  We'll get there, I promise.
 */
public class ITaskConfig {
  private final TaskConfig wrapped;

  public ITaskConfig(TaskConfig wrapped) {
    this.wrapped = wrapped.deepCopy();
  }

  public static final Function<ITaskConfig, TaskConfig> TO_BUILDER =
      new Function<ITaskConfig, TaskConfig>() {
        @Override
        public TaskConfig apply(ITaskConfig input) {
          return input.newBuilder();
        }
      };

  public static final Function<TaskConfig, ITaskConfig> FROM_BUILDER =
      new Function<TaskConfig, ITaskConfig>() {
        @Override
        public ITaskConfig apply(TaskConfig input) {
          return new ITaskConfig(input);
        }
      };

  public TaskConfig newBuilder() {
    return wrapped.deepCopy();
  }

  public IIdentity getOwner() {
    return new IIdentity(wrapped.getOwner());
  }

  public String getEnvironment() {
    return wrapped.getEnvironment();
  }

  public String getJobName() {
    return wrapped.getJobName();
  }

  public boolean isIsService() {
    return wrapped.isIsService();
  }

  public double getNumCpus() {
    return wrapped.getNumCpus();
  }

  public long getRamMb() {
    return wrapped.getRamMb();
  }

  public long getDiskMb() {
    return wrapped.getDiskMb();
  }

  public int getPriority() {
    return wrapped.getPriority();
  }

  public int getMaxTaskFailures() {
    return wrapped.getMaxTaskFailures();
  }

  public int getShardId() {
    return wrapped.getShardId();
  }

  public boolean isProduction() {
    return wrapped.isProduction();
  }

  public byte[] getThermosConfig() {
    return wrapped.getThermosConfig();
  }

  public Set<IConstraint> getConstraints() {
    return FluentIterable
        .from(wrapped.getConstraints())
        .transform(IConstraint.FROM_BUILDER)
        .toSet();
  }

  public Set<String> getRequestedPorts() {
    return ImmutableSet.copyOf(wrapped.getRequestedPorts());
  }

  public Map<String, String> getTaskLinks() {
    return ImmutableMap.copyOf(wrapped.getTaskLinks());
  }

  public String getContactEmail() {
    return wrapped.getContactEmail();
  }

  public Set<IPackage> getPackages() {
    return FluentIterable
        .from(wrapped.getPackages())
        .transform(IPackage.FROM_BUILDER)
        .toSet();
  }

  public IExecutorConfig getExecutorConfig() {
    return new IExecutorConfig(wrapped.getExecutorConfig());
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ITaskConfig)) {
      return false;
    }
    ITaskConfig other = (ITaskConfig) o;
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
