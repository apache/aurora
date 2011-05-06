package com.twitter.mesos.migrations.v0_v1;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.junit.Test;

import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.TwitterTaskInfo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * @author John Sirois
 */
public class OwnerIdentitiesTest {
  @Test
  public void testRepairOwnership() throws Exception {
    JobConfiguration job2 = new JobConfiguration().setOwner(new Identity("bubba", "lou"));
    assertEquals(job2, OwnerIdentities.repairOwnership(job2));

    JobConfiguration job1 = new JobConfiguration().setOldOwner("fred");
    assertEquals(job1.deepCopy().setOwner(new Identity("fred", "fred")),
        OwnerIdentities.repairOwnership(job1));

    JobConfiguration job3 =
        new JobConfiguration().setOldOwner("jake")
            .setTaskConfigs(
                Sets.newHashSet(
                    new TwitterTaskInfo(),
                    new TwitterTaskInfo().setOldOwner("jeff"),
                    new TwitterTaskInfo().setOwner(new Identity("james", "dean"))));

    JobConfiguration expectedJob = job3.deepCopy();
    Identity expectedJobOwner = new Identity("jake", "jake");
    expectedJob.setOwner(expectedJobOwner);
    ImmutableSet.Builder<TwitterTaskInfo> expectedTasks = ImmutableSet.builder();
    for (TwitterTaskInfo taskInfo : expectedJob.getTaskConfigs()) {
      expectedTasks.add(taskInfo.setOwner(expectedJobOwner));
    }
    expectedJob.setTaskConfigs(expectedTasks.build());

    assertEquals(expectedJob, OwnerIdentities.repairOwnership(job3));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRepairOwnershipBad() throws Exception {
    OwnerIdentities.repairOwnership(new JobConfiguration());
  }

  @Test
  public void testGetOwner() throws Exception {
    Identity owner = new Identity("jack", "spratt");
    assertSame("existing owner should be preserved",
        owner, OwnerIdentities.getOwner(new TwitterTaskInfo().setOwner(owner)));

    assertEquals("old owner should be used when no owner present",
        new Identity("fred", "fred"),
        OwnerIdentities.getOwner(new TwitterTaskInfo().setOldOwner("fred")));

    assertSame("owner should be used when set in preference to oldOwner", owner, OwnerIdentities
        .getOwner(new TwitterTaskInfo().setOldOwner("fred").setOwner(owner)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetOwnerBad() throws Exception {
    OwnerIdentities.getOwner(new TwitterTaskInfo());
  }

  @Test
  public void testRepairIdentity() throws Exception {
    Identity owner = new Identity("jim", "bob");
    assertSame(owner, OwnerIdentities.repairIdentity(null, owner));
    assertEquals(new Identity("fred", "fred"), OwnerIdentities.repairIdentity("fred", null));
    assertSame("owner should be favored over oldOwner",
        owner, OwnerIdentities.repairIdentity("fred", owner));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRepairIdentityBad() throws Exception {
    OwnerIdentities.repairIdentity(null, null);
  }
}
