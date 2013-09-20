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
package com.twitter.aurora.scheduler.state;

import java.util.logging.Logger;

import com.google.inject.Inject;

import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.configuration.ParsedConfiguration;
import com.twitter.aurora.scheduler.storage.Storage;

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
  public boolean receiveJob(ParsedConfiguration config) {
    LOG.info("Launching " + config.getTaskConfigs().size() + " tasks.");
    stateManager.insertPendingTasks(config.getTaskConfigs());
    return true;
  }

  @Override
  public boolean hasJob(final JobKey jobKey) {
    return !Storage.Util.consistentFetchTasks(storage, Query.jobScoped(jobKey).active()).isEmpty();
  }
}
