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
