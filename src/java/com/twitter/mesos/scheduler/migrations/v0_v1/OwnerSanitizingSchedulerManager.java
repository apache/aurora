package com.twitter.mesos.scheduler.migrations.v0_v1;

import java.util.logging.Logger;

import com.google.inject.Inject;

import org.apache.thrift.TException;

import com.twitter.mesos.gen.CreateJobResponse;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.KillResponse;
import com.twitter.mesos.gen.MesosSchedulerManager;
import com.twitter.mesos.gen.ScheduleStatusResponse;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.UpdateRequest;
import com.twitter.mesos.gen.UpdateResponse;
import com.twitter.mesos.migrations.v0_v1.OwnerIdentities;
import com.twitter.mesos.scheduler.migrations.ForwardingMesosSchedulerManager;
import com.twitter.mesos.scheduler.migrations.SchemaMigrationDelegate;

/**
 * Sanitizes old style thrift request struct owner strings to {@link com.twitter.mesos.gen.Identity}
 * owners before handing requests off to the real scheduler manager.
 *
 * @author John Sirois
 */
public class OwnerSanitizingSchedulerManager extends ForwardingMesosSchedulerManager {

  private static final Logger LOG = Logger.getLogger(OwnerSanitizingSchedulerManager.class.getName());

  @Inject
  public OwnerSanitizingSchedulerManager(
      @SchemaMigrationDelegate MesosSchedulerManager.Iface delegate) {
    super(delegate);
  }

  @Override
  public CreateJobResponse createJob(JobConfiguration jobConfiguration) throws TException {
    return super.createJob(sanitize(jobConfiguration));
  }

  @Override
  public ScheduleStatusResponse getTasksStatus(TaskQuery query) throws TException {
    return super.getTasksStatus(sanitize(query));
  }

  @Override
  public UpdateResponse updateTasks(UpdateRequest updateRequest) throws TException {
    return super.updateTasks(sanitize(updateRequest));
  }

  @Override
  public KillResponse killTasks(TaskQuery query) throws TException {
    return super.killTasks(sanitize(query));
  }

  private static JobConfiguration sanitize(JobConfiguration jobConfiguration) throws TException {
    try {
      JobConfiguration sanitized = OwnerIdentities.repairOwnership(jobConfiguration);
      if (!sanitized.equals(jobConfiguration)) {
        LOG.info("Sanitized: " + jobConfiguration);
      }
      return sanitized;
    } catch (IllegalArgumentException e) {
      throw new TException("Failed to sanitize: " + jobConfiguration, e);
    }
  }

  private static TaskQuery sanitize(TaskQuery query) throws TException {
    // TaskQueries need not have owner set unless the query is by owner and so we only sanitize
    // the cases where the oldOwner field is set - blindly replacing/filling in the new owner field
    // as the case may be

    if (!query.isSetOldOwner()) {
      return query;
    }

    TaskQuery sanitized = query.deepCopy();
    try {
      sanitized.setOwner(
          OwnerIdentities.repairIdentity(sanitized.getOldOwner(), sanitized.getOwner()));
    } catch (IllegalArgumentException e) {
      throw new TException("Failed to sanitize: " + query, e);
    }
    if (!sanitized.equals(query)) {
      LOG.info("Sanitized: " + query);
    }
    return sanitized;
  }

  private static UpdateRequest sanitize(UpdateRequest updateRequest) throws TException {
    UpdateRequest sanitized = updateRequest.deepCopy();
    try {
      sanitized.setUpdatedJob(OwnerIdentities.repairOwnership(sanitized.getUpdatedJob()));
    } catch (IllegalArgumentException e) {
      throw new TException("Failed to sanitize: " + updateRequest, e);
    }
    if (!sanitized.equals(updateRequest)) {
      LOG.info("Sanitized: " + updateRequest);
    }
    return sanitized;
  }
}
