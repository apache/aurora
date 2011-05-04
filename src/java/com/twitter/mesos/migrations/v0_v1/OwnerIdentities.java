package com.twitter.mesos.migrations.v0_v1;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.TwitterTaskInfo;

/**
 * Utility methods for transforming a v0 style owner string into a v1 {@link Identity}.
 *
 * @author John Sirois
 */
public final class OwnerIdentities {

  private OwnerIdentities() {
    // utility
  }

  /**
   * Repairs all the owner of the given {@code jobConfiguration} and the owners of its tasks.
   *
   * @param jobConfiguration a v0 job configuration struct
   * @return a v1 jobConfiguration struct with migrated owner fields
   * @throws IllegalArgumentException if the job configuration does not have a proper owner
   */
  public static JobConfiguration repairOwnership(JobConfiguration jobConfiguration) {
    JobConfiguration repaired = jobConfiguration.deepCopy();
    final Identity jobOwner = repairIdentity(repaired.getOldOwner(), repaired.getOwner());
    repaired.setOwner(jobOwner);
    if (repaired.isSetTaskConfigs()) {
      // since task info is value equals we need to re-build the set or items will be lost due to
      // new hashing
      repaired.setTaskConfigs(Sets.newHashSet(Iterables.transform(repaired.getTaskConfigs(),
          new Function<TwitterTaskInfo, TwitterTaskInfo>() {
            @Override public TwitterTaskInfo apply(TwitterTaskInfo taskInfo) {
              return taskInfo.setOwner(jobOwner);
            }
          })));
    }
    return repaired;
  }

  /**
   * Gets a valid v1 owner identity for the given {@code task}.
   *
   * @param task a v0 task struct
   * @return a v1 owner identity that represents the task's v0 owner
   * @throws IllegalArgumentException if the task does not have a proper owner
   */
  public static Identity getOwner(TwitterTaskInfo task) {
    return repairIdentity(task.getOldOwner(), task.getOwner());
  }

  /**
   * Given the two possible forms of ownership identification; return a valid v1 owner identity.
   *
   * @param oldOwner a possibly null v0 owner string
   * @param owner a possibly null v1 owner identity
   * @return {@code owner} if non-null; otherwise an identity constructed from the {@code oldOwner}
   * @throws IllegalArgumentException if the task does not have a proper owner
   */
  public static Identity repairIdentity(@Nullable String oldOwner, @Nullable Identity owner) {
    Preconditions.checkArgument(oldOwner != null || owner != null,
        "At least one form of owner must be non-null");
    return owner != null ? owner : new Identity(oldOwner, oldOwner);
  }
}
