package com.twitter.mesos.migrations.v0_v1;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.TwitterTaskInfo;

/**
 * Utility methods for transforming a v0 style owner string into a v1 {@link Identity}.
 *
 * @author John Sirois
 */
public final class OwnerIdentities {

  /**
   * @param task a v0 task struct
   * @return a v1 owner identity that represents the tasks v0 owner
   */
  public static Identity getOwner(TwitterTaskInfo task) {
    return repairIdentity(task.getOldOwner(), task.getOwner());
  }

  /**
   * @param oldOwner a possibly null v0 owner string
   * @param owner a possibly null v1 owner identity
   * @return {@code owner} if non-null; otherwise an identity constructed from the {@code oldOwner}
   */
  public static Identity repairIdentity(@Nullable String oldOwner, @Nullable Identity owner) {
    Preconditions.checkArgument(oldOwner != null || owner != null,
        "At least one form of owner must be non-null");
    return owner != null ? owner : new Identity(oldOwner, oldOwner);
  }
}
