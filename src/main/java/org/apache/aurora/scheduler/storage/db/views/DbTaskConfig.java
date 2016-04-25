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
package org.apache.aurora.scheduler.storage.db.views;

import java.util.List;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.common.collections.Pair;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.Metadata;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static org.apache.aurora.GuavaUtils.toImmutableSet;

public final class DbTaskConfig {
  private long rowId;
  private JobKey job;
  private Identity owner;
  private boolean isService;
  private double numCpus;
  private long ramMb;
  private long diskMb;
  private int priority;
  private int maxTaskFailures;
  private boolean production;
  private List<DbConstraint> constraints;
  private List<String> requestedPorts;
  private List<Pair<String, String>> taskLinks;
  private String contactEmail;
  private ExecutorConfig executorConfig;
  private List<Metadata> metadata;
  private DbContainer container;
  private String tier;
  private DbImage image;
  private List<DBResource> resources;

  private DbTaskConfig() {
  }

  public long getRowId() {
    return rowId;
  }

  TaskConfig toThrift() {
    return new TaskConfig()
        .setJob(job)
        .setOwner(owner)
        .setIsService(isService)
        .setNumCpus(numCpus)
        .setRamMb(ramMb)
        .setDiskMb(diskMb)
        .setPriority(priority)
        .setMaxTaskFailures(maxTaskFailures)
        .setProduction(production)
        .setTier(tier)
        .setImage(image == null ? null : image.toThrift())
        .setConstraints(constraints.stream()
            .map(DbConstraint::toThrift)
            .collect(toImmutableSet()))
        .setRequestedPorts(ImmutableSet.copyOf(requestedPorts))
        .setTaskLinks(Pairs.toMap(taskLinks))
        .setContactEmail(contactEmail)
        .setExecutorConfig(executorConfig)
        .setMetadata(ImmutableSet.copyOf(metadata))
        .setContainer(
            container == null ? Container.mesos(new MesosContainer()) : container.toThrift())
        .setResources(resources.stream().map(DBResource::toThrift).collect(toImmutableSet()));
  }

  public ITaskConfig toImmutable() {
    return ITaskConfig.build(toThrift());
  }
}
