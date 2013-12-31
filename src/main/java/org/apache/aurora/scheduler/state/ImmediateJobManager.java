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
package org.apache.aurora.scheduler.state;

import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.configuration.SanitizedConfiguration;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IJobKey;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Job scheduler that accepts any job and executes it immediately.
 */
class ImmediateJobManager extends JobManager {

  private static final Logger LOG = Logger.getLogger(ImmediateJobManager.class.getName());

  private final StateManager stateManager;
  private final Storage storage;

  @Inject
  ImmediateJobManager(StateManager stateManager, Storage storage) {
    this.stateManager = checkNotNull(stateManager);
    this.storage = checkNotNull(storage);
  }

  @Override
  public String getUniqueKey() {
    return "IMMEDIATE";
  }

  @Override
  public boolean receiveJob(SanitizedConfiguration config) {
    LOG.info("Launching " + config.getTaskConfigs().size() + " tasks.");
    stateManager.insertPendingTasks(config.getTaskConfigs());
    return true;
  }

  @Override
  public boolean hasJob(final IJobKey jobKey) {
    return !Storage.Util.consistentFetchTasks(storage, Query.jobScoped(jobKey).active()).isEmpty();
  }
}
