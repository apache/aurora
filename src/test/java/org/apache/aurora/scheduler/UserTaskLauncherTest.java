/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler;

import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.twitter.common.collections.Pair;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.async.OfferQueue;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Text;
import org.apache.mesos.Protos.Value.Type;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.ScheduleStatus.FAILED;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.HOST_CONSTRAINT;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UserTaskLauncherTest extends EasyMockTest {

  private static final String FRAMEWORK_ID = "FrameworkId";

  private static final SlaveID SLAVE_ID = SlaveID.newBuilder().setValue("SlaveId").build();
  private static final String SLAVE_HOST_1 = "SlaveHost1";

  private static final String TASK_ID_A = "task_id_a";

  private static final OfferID OFFER_ID = OfferID.newBuilder().setValue("OfferId").build();
  private static final Offer OFFER = createOffer(SLAVE_ID, SLAVE_HOST_1, 4, 1024, 1024);

  private OfferQueue offerQueue;
  private StateManager stateManager;

  private TaskLauncher launcher;

  @Before
  public void setUp() {
    offerQueue = createMock(OfferQueue.class);
    stateManager = createMock(StateManager.class);
    launcher = new UserTaskLauncher(offerQueue, stateManager);
  }

  @Test
  public void testForwardsOffers() throws Exception {
    offerQueue.addOffer(OFFER);

    control.replay();

    assertFalse(launcher.createTask(OFFER).isPresent());
  }

  @Test
  public void testForwardsStatusUpdates() throws Exception {
    expect(stateManager.changeState(
        TASK_ID_A,
        Optional.<ScheduleStatus>absent(),
        RUNNING,
        Optional.of("fake message")))
        .andReturn(true);

    control.replay();

    TaskStatus status = TaskStatus.newBuilder()
        .setState(TaskState.TASK_RUNNING)
        .setTaskId(TaskID.newBuilder().setValue(TASK_ID_A))
        .setMessage("fake message")
        .build();
    assertTrue(launcher.statusUpdate(status));
  }

  @Test
  public void testForwardsRescindedOffers() throws Exception {
    launcher.cancelOffer(OFFER_ID);

    control.replay();

    launcher.cancelOffer(OFFER_ID);
  }

  @Test(expected = StorageException.class)
  public void testFailedStatusUpdate() throws Exception {
    expect(stateManager.changeState(
        TASK_ID_A,
        Optional.<ScheduleStatus>absent(),
        RUNNING,
        Optional.of("fake message")))
        .andThrow(new StorageException("Injected error"));

    control.replay();

    TaskStatus status = TaskStatus.newBuilder()
        .setState(TaskState.TASK_RUNNING)
        .setTaskId(TaskID.newBuilder().setValue(TASK_ID_A))
        .setMessage("fake message")
        .build();
    launcher.statusUpdate(status);
  }

  @Test
  public void testMemoryLimitTranslationHack() throws Exception {
    expect(stateManager.changeState(
        TASK_ID_A,
        Optional.<ScheduleStatus>absent(),
        FAILED,
        Optional.of(UserTaskLauncher.MEMORY_LIMIT_DISPLAY)))
        .andReturn(false);

    control.replay();

    TaskStatus status = TaskStatus.newBuilder()
        .setState(TaskState.TASK_FAILED)
        .setTaskId(TaskID.newBuilder().setValue(TASK_ID_A))
        .setMessage("Memory limit exceeded: Requested 256MB, Used 256MB.\n\n"
            + "MEMORY STATISTICS: \n"
            + "cache 20422656\n"
            + "rss 248012800\n"
            + "mapped_file 8192\n"
            + "pgpgin 28892\n"
            + "pgpgout 6791\n"
            + "inactive_anon 90112\n"
            + "active_anon 245768192\n"
            + "inactive_file 19685376\n"
            + "active_file 700416\n"
            + "unevictable 0\n"
            + "hierarchical_memory_limit 268435456\n"
            + "total_cache 20422656\n"
            + "total_rss 248012800\n"
            + "total_mapped_file 8192\n"
            + "total_pgpgin 28892\n"
            + "total_pgpgout 6791\n"
            + "total_inactive_anon 90112\n"
            + "total_active_anon 245768192\n"
            + "total_inactive_file 19685376\n"
            + "total_active_file 700416\n"
            + "total_unevictable 0 ")
        .build();
    launcher.statusUpdate(status);
  }

  private static Offer createOffer(SlaveID slave, String slaveHost, double cpu,
      double ramMb, double diskMb) {
    return createOffer(slave, slaveHost, cpu, ramMb, diskMb,
        ImmutableSet.<Pair<Integer, Integer>>of());
  }

  private static Offer createOffer(SlaveID slave, String slaveHost, double cpu,
      double ramMb, double diskMb, Set<Pair<Integer, Integer>> ports) {

    Ranges portRanges = Ranges.newBuilder()
        .addAllRange(Iterables.transform(ports, new Function<Pair<Integer, Integer>, Range>() {
          @Override public Range apply(Pair<Integer, Integer> range) {
            return Range.newBuilder().setBegin(range.getFirst()).setEnd(range.getSecond()).build();
          }
        }))
        .build();

    return Offer.newBuilder()
        .addResources(Resource.newBuilder().setType(Type.SCALAR).setName(Resources.CPUS)
            .setScalar(Scalar.newBuilder().setValue(cpu)))
        .addResources(Resource.newBuilder().setType(Type.SCALAR).setName(Resources.RAM_MB)
            .setScalar(Scalar.newBuilder().setValue(ramMb)))
        .addResources(Resource.newBuilder().setType(Type.SCALAR).setName(Resources.DISK_MB)
            .setScalar(Scalar.newBuilder().setValue(diskMb)))
        .addResources(Resource.newBuilder().setType(Type.RANGES).setName(Resources.PORTS)
            .setRanges(portRanges))
        .addAttributes(Attribute.newBuilder().setType(Type.TEXT)
            .setName(HOST_CONSTRAINT)
            .setText(Text.newBuilder().setValue(slaveHost)))
        .setSlaveId(slave)
        .setHostname(slaveHost)
        .setFrameworkId(FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build())
        .setId(OFFER_ID)
        .build();
  }
}
