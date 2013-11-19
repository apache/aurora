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
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
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
public final class ITaskConfig {
  private final TaskConfig wrapped;
  private final IIdentity owner;
  private final ImmutableSet<IConstraint> constraints;
  private final ImmutableSet<String> requestedPorts;
  private final ImmutableMap<String, String> taskLinks;
  private final ImmutableSet<IPackage> packages;
  private final IExecutorConfig executorConfig;

  private ITaskConfig(TaskConfig wrapped) {
    this.wrapped = Preconditions.checkNotNull(wrapped);
    this.owner = !wrapped.isSetOwner()
        ? null
        : IIdentity.buildNoCopy(wrapped.getOwner());
    this.constraints = !wrapped.isSetConstraints()
        ? ImmutableSet.<IConstraint>of()
        : FluentIterable.from(wrapped.getConstraints())
              .transform(IConstraint.FROM_BUILDER)
              .toSet();
    this.requestedPorts = !wrapped.isSetRequestedPorts()
        ? ImmutableSet.<String>of()
        : ImmutableSet.copyOf(wrapped.getRequestedPorts());
    this.taskLinks = !wrapped.isSetTaskLinks()
        ? ImmutableMap.<String, String>of()
        : ImmutableMap.copyOf(wrapped.getTaskLinks());
    this.packages = !wrapped.isSetPackages()
        ? ImmutableSet.<IPackage>of()
        : FluentIterable.from(wrapped.getPackages())
              .transform(IPackage.FROM_BUILDER)
              .toSet();
    this.executorConfig = !wrapped.isSetExecutorConfig()
        ? null
        : IExecutorConfig.buildNoCopy(wrapped.getExecutorConfig());
  }

  static ITaskConfig buildNoCopy(TaskConfig wrapped) {
    return new ITaskConfig(wrapped);
  }

  public static ITaskConfig build(TaskConfig wrapped) {
    return buildNoCopy(wrapped.deepCopy());
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

  public static ImmutableList<TaskConfig> toBuildersList(Iterable<ITaskConfig> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toList();
  }

  public static ImmutableList<ITaskConfig> listFromBuilders(Iterable<TaskConfig> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toList();
  }

  public static ImmutableSet<TaskConfig> toBuildersSet(Iterable<ITaskConfig> w) {
    return FluentIterable.from(w).transform(TO_BUILDER).toSet();
  }

  public static ImmutableSet<ITaskConfig> setFromBuilders(Iterable<TaskConfig> b) {
    return FluentIterable.from(b).transform(FROM_BUILDER).toSet();
  }

  public TaskConfig newBuilder() {
    return wrapped.deepCopy();
  }

  public boolean isSetOwner() {
    return wrapped.isSetOwner();
  }

  public IIdentity getOwner() {
    return owner;
  }

  public boolean isSetEnvironment() {
    return wrapped.isSetEnvironment();
  }

  public String getEnvironment() {
    return wrapped.getEnvironment();
  }

  public boolean isSetJobName() {
    return wrapped.isSetJobName();
  }

  public String getJobName() {
    return wrapped.getJobName();
  }

  public boolean isSetIsService() {
    return wrapped.isSetIsService();
  }

  public boolean isIsService() {
    return wrapped.isIsService();
  }

  public boolean isSetNumCpus() {
    return wrapped.isSetNumCpus();
  }

  public double getNumCpus() {
    return wrapped.getNumCpus();
  }

  public boolean isSetRamMb() {
    return wrapped.isSetRamMb();
  }

  public long getRamMb() {
    return wrapped.getRamMb();
  }

  public boolean isSetDiskMb() {
    return wrapped.isSetDiskMb();
  }

  public long getDiskMb() {
    return wrapped.getDiskMb();
  }

  public boolean isSetPriority() {
    return wrapped.isSetPriority();
  }

  public int getPriority() {
    return wrapped.getPriority();
  }

  public boolean isSetMaxTaskFailures() {
    return wrapped.isSetMaxTaskFailures();
  }

  public int getMaxTaskFailures() {
    return wrapped.getMaxTaskFailures();
  }

  public boolean isSetInstanceIdDEPRECATED() {
    return wrapped.isSetInstanceIdDEPRECATED();
  }

  public int getInstanceIdDEPRECATED() {
    return wrapped.getInstanceIdDEPRECATED();
  }

  public boolean isSetProduction() {
    return wrapped.isSetProduction();
  }

  public boolean isProduction() {
    return wrapped.isProduction();
  }

  public boolean isSetConstraints() {
    return wrapped.isSetConstraints();
  }

  public Set<IConstraint> getConstraints() {
    return constraints;
  }

  public boolean isSetRequestedPorts() {
    return wrapped.isSetRequestedPorts();
  }

  public Set<String> getRequestedPorts() {
    return requestedPorts;
  }

  public boolean isSetTaskLinks() {
    return wrapped.isSetTaskLinks();
  }

  public Map<String, String> getTaskLinks() {
    return taskLinks;
  }

  public boolean isSetContactEmail() {
    return wrapped.isSetContactEmail();
  }

  public String getContactEmail() {
    return wrapped.getContactEmail();
  }

  public boolean isSetPackages() {
    return wrapped.isSetPackages();
  }

  public Set<IPackage> getPackages() {
    return packages;
  }

  public boolean isSetExecutorConfig() {
    return wrapped.isSetExecutorConfig();
  }

  public IExecutorConfig getExecutorConfig() {
    return executorConfig;
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
