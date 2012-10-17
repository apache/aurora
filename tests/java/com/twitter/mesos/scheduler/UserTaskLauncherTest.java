package com.twitter.mesos.scheduler;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;

import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Text;
import org.apache.mesos.Protos.Value.Type;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.collections.Pair;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.storage.Storage.StorageException;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.scheduler.configuration.ConfigurationManager.HOST_CONSTRAINT;

public class UserTaskLauncherTest extends EasyMockTest {

  private static final String FRAMEWORK_ID = "FrameworkId";

  private static final SlaveID SLAVE_ID = SlaveID.newBuilder().setValue("SlaveId").build();
  private static final String SLAVE_HOST_1 = "SlaveHost1";

  private static final String TASK_ID_A = "task_id_a";

  private static final OfferID OFFER_ID = OfferID.newBuilder().setValue("OfferId").build();
  private static final Offer OFFER = createOffer(SLAVE_ID, SLAVE_HOST_1, 4, 1024, 1024);

  private StateManager stateManager;
  private TaskAssigner assigner;

  private TaskLauncher launcher;

  @Before
  public void setUp() {
    stateManager = createMock(StateManager.class);
    assigner = createMock(TaskAssigner.class);
    launcher = new UserTaskLauncher(stateManager, assigner);
    stateManager.saveAttributesFromOffer(SLAVE_HOST_1, OFFER.getAttributesList());
    expectLastCall().anyTimes();
  }

  @Test
  public void testOfferNoTasks() throws Exception {
    pendingTasks();
    control.replay();
    assertThat(launcher.createTask(OFFER), is(Optional.<TaskInfo>absent()));
  }

  @Test
  public void testHonorsScheduleFilter() throws Exception {
    ScheduledTask task = task(config());

    pendingTasks(task);
    expect(assigner.maybeAssign(EasyMock.<Offer>anyObject(), EasyMock.eq(task)))
        .andReturn(Optional.<TaskInfo>absent());

    control.replay();

    assertFalse(launcher.createTask(OFFER).isPresent());
  }

  private void expectAssignTask(ScheduledTask task, Offer offer) throws Exception {
    AssignedTask assigned = task.getAssignedTask().deepCopy()
        .setSlaveHost(offer.getHostname())
        .setAssignedPorts(ImmutableMap.<String, Integer>of());

    TaskInfo taskInfo =
        TaskInfo.newBuilder().setName(Tasks.jobKey(task))
            .setTaskId(TaskID.newBuilder().setValue(Tasks.id(task)))
            .setSlaveId(offer.getSlaveId())
            .addAllResources(Resources.from(task.getAssignedTask().getTask()).toResourceList())
            .setData(ByteString.copyFrom(ThriftBinaryCodec.encode(assigned)))
            .setExecutor(ExecutorInfo.newBuilder()
                .setCommand(CommandInfo.newBuilder().setValue("fake"))
                .setExecutorId(ExecutorID.newBuilder().setValue("fake")))
            .build();

    expect(assigner.maybeAssign(offer, task)).andReturn(Optional.of(taskInfo));
  }

  @Test
  public void testSchedulingOrder() throws Exception {
    ScheduledTask task1a = task(config().setShardId(0).setPriority(10));
    ScheduledTask task1b = task(config().setShardId(0).setPriority(10));
    ScheduledTask task2a = task(config().setShardId(0).setPriority(0).setProduction(true));
    ScheduledTask task2b = task(config().setShardId(0).setPriority(0).setProduction(true));
    ScheduledTask task3a = task(config().setShardId(0).setPriority(11));
    ScheduledTask task3b = task(config().setShardId(0).setPriority(11));

    pendingTasks(task1a, task1b, task2a, task2b, task3a, task3b);
    pendingTasks(task1a, task1b, task2b, task3a, task3b);
    pendingTasks(task1a, task1b, task3a, task3b);
    pendingTasks(task1b, task3a, task3b);
    pendingTasks(task3a, task3b);
    pendingTasks(task3b);

    expectAssignTask(task2a, OFFER);
    expectAssignTask(task2b, OFFER);
    expectAssignTask(task1a, OFFER);
    expectAssignTask(task1b, OFFER);
    expectAssignTask(task3a, OFFER);
    expectAssignTask(task3b, OFFER);

    control.replay();

    sendOffer(task2a);
    sendOffer(task2b);
    sendOffer(task1a);
    sendOffer(task1b);
    sendOffer(task3a);
    sendOffer(task3b);
  }

  @Test(expected = StorageException.class)
  public void testFailedStatusUpdate() throws Exception {
    expect(stateManager.fetchTasks(Query.byId(TASK_ID_A)))
        .andReturn(ImmutableSet.of(task(config())));
    expect(stateManager.changeState(
        Query.byId(TASK_ID_A),
        ScheduleStatus.RUNNING,
        Optional.of("fake message")))
        .andThrow(new StorageException("Injected error"));

    control.replay();

    TaskStatus status = TaskStatus.newBuilder()
        .setState(TaskState.TASK_RUNNING)
        .setTaskId(TaskID.newBuilder().setValue(TASK_ID_A))
        .setData(ByteString.copyFromUtf8("fake message"))
        .build();
    launcher.statusUpdate(status);
  }

  private IExpectationSetters<Set<ScheduledTask>> pendingTasks(ScheduledTask... tasks) {
    Set<ScheduledTask> allTasks = ImmutableSet.<ScheduledTask>builder().add(tasks).build();
    return expect(stateManager.fetchTasks(Query.byStatus(PENDING)))
        .andReturn(allTasks);
  }

  private final AtomicLong taskIdCounter = new AtomicLong();

  private ScheduledTask task(TwitterTaskInfo config) {
    return new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTaskId(String.valueOf(taskIdCounter.incrementAndGet()))
            .setTask(config));
  }

  private TwitterTaskInfo config() {
    return new TwitterTaskInfo()
        .setJobName("job_a")
        .setOwner(new Identity("user", "role"));
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

  private void sendOffer(ScheduledTask task) throws Exception {
    sendOffer(task, ImmutableSet.<String>of(), ImmutableSet.<Integer>of());
  }

  private void sendOffer(
      ScheduledTask task,
      Set<String> portNames,
      Set<Integer> ports) throws Exception {

    TwitterTaskInfo config = task.getAssignedTask().getTask();
    ImmutableList.Builder<Resource> resourceBuilder = ImmutableList.<Resource>builder()
        .add(Resources.makeMesosResource(Resources.CPUS, config.getNumCpus()))
        .add(Resources.makeMesosResource(Resources.DISK_MB, config.getDiskMb()))
        .add(Resources.makeMesosResource(Resources.RAM_MB, config.getRamMb()));
    if (ports.size() > 0) {
      resourceBuilder.add(Resources.makeMesosRangeResource(Resources.PORTS, ports));
    }
    List<Resource> resources = resourceBuilder.build();

    Optional<TaskInfo> launched = launcher.createTask(OFFER);
    AssignedTask assigned = extractTask(launched);

    assertThat(launched.get().getResourcesList(), is(resources));
    assertThat(assigned.getSlaveHost(), is(OFFER.getHostname()));
    Map<String, Integer> assignedPorts = assigned.getAssignedPorts();
    assertThat(assignedPorts.keySet(), is(portNames));
    assertEquals(ports, ImmutableSet.copyOf(assignedPorts.values()));
  }

  private AssignedTask extractTask(Optional<TaskInfo> task) throws CodingException {
    assertTrue(task.isPresent());
    return ThriftBinaryCodec.decode(AssignedTask.class, task.get().getData().toByteArray());
  }
}
