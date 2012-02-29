package com.twitter.mesos.scheduler;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.Constraint;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.LimitConstraint;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskConstraint;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.ValueConstraint;
import com.twitter.mesos.scheduler.SchedulingFilter.Veto;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.storage.AttributeStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work.Quiet;
import com.twitter.mesos.scheduler.storage.TaskStore;

import static com.twitter.mesos.scheduler.ConstraintFilter.unsatisfiedLimitVeto;
import static com.twitter.mesos.scheduler.ConstraintFilter.unsatisfiedValueVeto;
import static com.twitter.mesos.scheduler.SchedulingFilterImpl.CPU_VETO;
import static com.twitter.mesos.scheduler.SchedulingFilterImpl.DEDICATED_HOST_VETO;
import static com.twitter.mesos.scheduler.SchedulingFilterImpl.DISK_VETO;
import static com.twitter.mesos.scheduler.SchedulingFilterImpl.PORTS_VETO;
import static com.twitter.mesos.scheduler.SchedulingFilterImpl.RAM_VETO;
import static com.twitter.mesos.scheduler.SchedulingFilterImpl.RESERVED_HOST_VETO;
import static com.twitter.mesos.scheduler.SchedulingFilterImpl.RESTRICTED_JOB_VETO;
import static com.twitter.mesos.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author William Farner
 */
public class SchedulingFilterImplTest extends EasyMockTest {

  private static final String HOST_A = "hostA";
  private static final String HOST_B = "hostB";
  private static final String HOST_C = "hostC";

  private static final String RACK_A = "rackA";
  private static final String RACK_B = "rackB";

  private static final String RACK_ATTRIBUTE = "rack";
  private static final String HOST_ATTRIBUTE = "host";

  private static final String JOB_A = "myJobA";
  private static final String JOB_B = "myJobB";

  private static final String ROLE_A = "roleA";
  private static final String USER_A = "userA";
  private static final Identity OWNER_A = new Identity(ROLE_A, USER_A);

  private static final String ROLE_B = "roleB";
  private static final String USER_B = "userB";
  private static final Identity OWNER_B = new Identity(ROLE_B, USER_B);

  private static final int DEFAULT_CPUS = 4;
  private static final long DEFAULT_RAM = 1000;
  private static final long DEFAULT_DISK = 2000;
  private static final Resources DEFAULT_OFFER = new Resources(DEFAULT_CPUS,
      Amount.of(DEFAULT_RAM, Data.MB), Amount.of(DEFAULT_DISK, Data.MB), 0);

  private SchedulingFilter defaultFilter;
  private Storage storage;
  private StoreProvider storeProvider;
  private TaskStore taskStore;
  private AttributeStore attributeStore;

  @Before
  public void setUp() throws Exception {
    storage = createMock(Storage.class);
    defaultFilter = new SchedulingFilterImpl(Maps.<String, String>newHashMap(), storage);
    storeProvider = createMock(StoreProvider.class);
    taskStore = createMock(TaskStore.class);
    attributeStore = createMock(AttributeStore.class);

    // Link the store provider to the store mocks.
    expect(storage.doInTransaction(EasyMock.<Quiet<?>>anyObject()))
        .andAnswer(new IAnswer<Object>() {
          @Override
          public Object answer() {
            @SuppressWarnings("unchecked")
            Quiet<?> arg = (Quiet<?>) EasyMock.getCurrentArguments()[0];
            return arg.apply(storeProvider);
          }
        })
        .anyTimes();

    expect(storeProvider.getTaskStore()).andReturn(taskStore).anyTimes();
    expect(storeProvider.getAttributeStore()).andReturn(attributeStore).anyTimes();
  }

  @Test
  public void testMeetsOffer() throws Exception {
    control.replay();

    assertNoVetoes(makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK));
    assertNoVetoes(makeTask(DEFAULT_CPUS - 1, DEFAULT_RAM - 1, DEFAULT_DISK - 1));
  }

  @Test
  public void testSufficientPorts() throws Exception {
    control.replay();

    Resources twoPorts = new Resources(DEFAULT_CPUS, Amount.of(DEFAULT_RAM, Data.MB),
        Amount.of(DEFAULT_DISK, Data.MB), 2);

    TwitterTaskInfo noPortTask = makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .setRequestedPorts(ImmutableSet.<String>of());
    TwitterTaskInfo onePortTask = makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .setRequestedPorts(ImmutableSet.of("one"));
    TwitterTaskInfo twoPortTask = makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .setRequestedPorts(ImmutableSet.of("one", "two"));
    TwitterTaskInfo threePortTask = makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .setRequestedPorts(ImmutableSet.of("one", "two", "three"));

    assertTrue(defaultFilter.filter(twoPorts, Optional.<String>absent(), noPortTask).isEmpty());
    assertTrue(defaultFilter.filter(twoPorts, Optional.<String>absent(), onePortTask).isEmpty());
    assertTrue(defaultFilter.filter(twoPorts, Optional.<String>absent(), twoPortTask).isEmpty());
    assertEquals(ImmutableSet.of(PORTS_VETO),
        defaultFilter.filter(twoPorts, Optional.<String>absent(), threePortTask));
  }

  @Test
  public void testInsufficientResources() throws Exception {
    control.replay();

    assertVetoes(makeTask(DEFAULT_CPUS + 1, DEFAULT_RAM + 1, DEFAULT_DISK + 1),
        CPU_VETO, DISK_VETO, RAM_VETO);
    assertVetoes(makeTask(DEFAULT_CPUS + 1, DEFAULT_RAM, DEFAULT_DISK), CPU_VETO);
    assertVetoes(makeTask(DEFAULT_CPUS, DEFAULT_RAM + 1, DEFAULT_DISK), RAM_VETO);
    assertVetoes(makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK + 1), DISK_VETO);
  }

  @Test
  public void testJobAllowedOnMachine() throws Exception {
    expectGetHostAttributes(HOST_B).anyTimes();
    expectGetHostAttributes(HOST_C).anyTimes();
    control.replay();

    SchedulingFilter filterBuilder = new SchedulingFilterImpl(ImmutableMap.of(
        HOST_B, Tasks.jobKey(OWNER_A, JOB_A),
        HOST_C, Tasks.jobKey(OWNER_A, JOB_A)),
        storage);

    assertThat(filterBuilder.filter(DEFAULT_OFFER, Optional.of(HOST_B),
        makeTask(OWNER_A, JOB_A, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)).isEmpty(),
        is(true));

    assertThat(filterBuilder.filter(DEFAULT_OFFER, Optional.of(HOST_C),
        makeTask(OWNER_A, JOB_A, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)).isEmpty(),
        is(true));
  }

  @Test
  public void testJobNotAllowedOnMachine() throws Exception {
    expectGetHostAttributes(HOST_A).anyTimes();
    control.replay();

    SchedulingFilter filterBuilder = new SchedulingFilterImpl(ImmutableMap.of(
        HOST_B, Tasks.jobKey(OWNER_A, JOB_A),
        HOST_C, Tasks.jobKey(OWNER_A, JOB_A)),
        storage);

    // HOST_B can only run OWNER_A/JOB_A.
    assertThat(
        filterBuilder.filter(DEFAULT_OFFER, Optional.of(HOST_A),
            makeTask(OWNER_A, JOB_B, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)).isEmpty(),
        is(true));
    assertEquals(ImmutableSet.of(RESERVED_HOST_VETO),
        filterBuilder.filter(DEFAULT_OFFER, Optional.of(HOST_B),
            makeTask(OWNER_A, JOB_B, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)));
  }

  @Test
  public void testMachineNotAllowedToRunJob() throws Exception {
    expectGetHostAttributes(HOST_A).anyTimes();
    control.replay();

    SchedulingFilter filterBuilder = new SchedulingFilterImpl(ImmutableMap.of(
        HOST_B, Tasks.jobKey(OWNER_A, JOB_A),
        HOST_C, Tasks.jobKey(OWNER_A, JOB_A)),
        storage);

    // OWNER_A/JOB_A can only run on HOST_B or HOST_C
    assertThat(
        filterBuilder.filter(DEFAULT_OFFER, Optional.of(HOST_A),
        makeTask(OWNER_A, JOB_B, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)).isEmpty(),
        is(true));
    assertEquals(ImmutableSet.of(RESTRICTED_JOB_VETO),
        filterBuilder.filter(DEFAULT_OFFER, Optional.of(HOST_A),
            makeTask(OWNER_A, JOB_A, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)));
  }

  @Test
  public void testDedicatedRole() throws Exception {
    expectGetHostAttributes(HOST_A, dedicated(ROLE_A)).anyTimes();

    control.replay();

    checkConstraint(HOST_A, DEDICATED_ATTRIBUTE, true, ROLE_A);
    assertVetoes(makeTask(OWNER_B, JOB_B), HOST_A, DEDICATED_HOST_VETO);
  }

  @Test
  public void testMultiValuedAttributes() throws Exception {
    expectGetHostAttributes(HOST_A, valueAttribute("jvm", "1.0", "2.0", "3.0")).anyTimes();
    expectGetHostAttributes(HOST_B, valueAttribute("jvm", "1.0")).anyTimes();

    control.replay();

    checkConstraint(HOST_A, "jvm", true, "1.0");
    checkConstraint(HOST_A, "jvm", false, "4.0");

    checkConstraint(HOST_A, "jvm", true, "1.0", "2.0");
    checkConstraint(HOST_B, "jvm", false, "2.0", "3.0");
  }

  @Test
  public void testMultipleTaskConstraints() throws Exception {
    expectGetHostAttributes(HOST_A, dedicated(HOST_A), host(HOST_A));
    expectGetHostAttributes(HOST_B, dedicated("xxx"), host(HOST_A));
    control.replay();

    Constraint constraint1 = makeConstraint("host", HOST_A);
    Constraint constraint2 = makeConstraint(DEDICATED_ATTRIBUTE, "xxx");

    assertVetoes(makeTask(OWNER_A, JOB_A, constraint1, constraint2), HOST_A,
        unsatisfiedValueVeto(DEDICATED_ATTRIBUTE));
    assertNoVetoes(makeTask(OWNER_B, JOB_B, constraint1, constraint2), HOST_B);
  }

  @Test
  public void testUnderLimitNoTasks() throws Exception {
    expectGetHostAttributes(HOST_A);
    expectGetHostAttributes(HOST_A, host(HOST_A));
    expectGetTasks();

    control.replay();

    assertNoVetoes(hostLimitTask(2), HOST_A);
  }

  private Attribute host(String host) {
    return valueAttribute(HOST_ATTRIBUTE, host);
  }

  private Attribute rack(String rack) {
    return valueAttribute(RACK_ATTRIBUTE, rack);
  }

  private Attribute dedicated(String role) {
    return valueAttribute(DEDICATED_ATTRIBUTE, role);
  }

  @Test
  public void testLimitWithinJob() throws Exception {
    expectGetHostAttributes(HOST_A, host(HOST_A), rack(RACK_A)).atLeastOnce();
    expectGetHostAttributes(HOST_B, host(HOST_B), rack(RACK_A)).atLeastOnce();
    expectGetHostAttributes(HOST_C, host(HOST_C), rack(RACK_B)).atLeastOnce();

    expectGetTasks(
        makeScheduledTask(OWNER_A, JOB_A, HOST_A),
        makeScheduledTask(OWNER_B, JOB_A, HOST_A),
        makeScheduledTask(OWNER_B, JOB_A, HOST_A),
        makeScheduledTask(OWNER_A, JOB_A, HOST_B),
        makeScheduledTask(OWNER_A, JOB_A, HOST_B),
        makeScheduledTask(OWNER_B, JOB_A, HOST_B),
        makeScheduledTask(OWNER_A, JOB_A, HOST_C))
        .atLeastOnce();

    control.replay();

    assertNoVetoes(hostLimitTask(OWNER_A, JOB_A, 2), HOST_A);
    assertVetoes(hostLimitTask(OWNER_A, JOB_A, 1), HOST_B, unsatisfiedLimitVeto(HOST_ATTRIBUTE));
    assertVetoes(hostLimitTask(OWNER_A, JOB_A, 2), HOST_B, unsatisfiedLimitVeto(HOST_ATTRIBUTE));
    assertNoVetoes(hostLimitTask(OWNER_A, JOB_A, 3), HOST_B);

    assertVetoes(rackLimitTask(OWNER_B, JOB_A, 2), HOST_B, unsatisfiedLimitVeto(RACK_ATTRIBUTE));
    assertVetoes(rackLimitTask(OWNER_B, JOB_A, 3), HOST_B, unsatisfiedLimitVeto(RACK_ATTRIBUTE));
    assertNoVetoes(rackLimitTask(OWNER_B, JOB_A, 4), HOST_B);

    assertNoVetoes(rackLimitTask(OWNER_B, JOB_A, 1), HOST_C);

    assertVetoes(rackLimitTask(OWNER_A, JOB_A, 1), HOST_C, unsatisfiedLimitVeto(RACK_ATTRIBUTE));
    assertNoVetoes(rackLimitTask(OWNER_B, JOB_A, 2), HOST_C);
  }

  @Test
  public void testAttribute() throws Exception {
    expectGetHostAttributes(HOST_A, valueAttribute("jvm", "1.0")).atLeastOnce();

    control.replay();

    // Matches attribute, matching value.
    checkConstraint(HOST_A, "jvm", true, "1.0");

    // Matches attribute, different value.
    checkConstraint(HOST_A, "jvm", false, "1.4");

    // Does not match attribute.
    checkConstraint(HOST_A, "xxx", false, "1.4");

    // Logical 'OR' matching attribute.
    checkConstraint(HOST_A, "jvm", false, "1.2", "1.4");

    // Logical 'OR' not matching attribute.
    checkConstraint(HOST_A, "xxx", false, "1.0", "1.4");
  }

  @Test
  public void testAttributes() throws Exception {
    expectGetHostAttributes(HOST_A,
        valueAttribute("jvm", "1.4", "1.6", "1.7"),
        valueAttribute("zone", "a", "b", "c")).atLeastOnce();

    control.replay();

    // Matches attribute, matching value.
    checkConstraint(HOST_A, "jvm", true, "1.4");

    // Matches attribute, different value.
    checkConstraint(HOST_A, "jvm", false, "1.0");

    // Does not match attribute.
    checkConstraint(HOST_A, "xxx", false, "1.4");

    // Logical 'OR' with attribute and value match.
    checkConstraint(HOST_A, "jvm", true, "1.2", "1.4");

    // Does not match attribute.
    checkConstraint(HOST_A, "xxx", false, "1.0", "1.4");

    // Check that logical AND works.
    Constraint jvmConstraint = makeConstraint("jvm", "1.6");
    Constraint zoneConstraint = makeConstraint("zone", "c");

    assertThat(defaultFilter.filter(DEFAULT_OFFER, Optional.of(HOST_A),
        makeTask(OWNER_A, JOB_A, jvmConstraint, zoneConstraint)).isEmpty(),
        is(true));

    Constraint jvmNegated = jvmConstraint.deepCopy();
    jvmNegated.getConstraint().getValue().setNegated(true);
    Constraint zoneNegated = jvmConstraint.deepCopy();
    zoneNegated.getConstraint().getValue().setNegated(true);
    assertVetoes(makeTask(OWNER_A, JOB_A, jvmNegated, zoneNegated), HOST_A,
        unsatisfiedValueVeto("jvm"));
  }

  private void checkConstraint(String host, String constraintName,
      boolean expected, String value, String... vs) {
    checkConstraint(OWNER_A, JOB_A, host, constraintName, expected, value, vs);
  }

  private void checkConstraint(Identity owner, String jobName, String host, String constraintName,
      boolean expected, String value, String... vs) {
    checkConstraint(owner, jobName, host, constraintName, expected,
        new ValueConstraint(false,
            ImmutableSet.<String>builder().add(value).addAll(Arrays.asList(vs)).build()));
  }

  private void checkConstraint(Identity owner, String jobName, String host, String constraintName,
      boolean expected, ValueConstraint value) {

    Constraint constraint = new Constraint(constraintName, TaskConstraint.value(value));
    assertThat(defaultFilter.filter(DEFAULT_OFFER, Optional.of(host),
        makeTask(owner, jobName, constraint)).isEmpty(), is(expected));

    Constraint negated = constraint.deepCopy();
    negated.getConstraint().getValue().setNegated(!value.isNegated());
    assertThat(defaultFilter.filter(DEFAULT_OFFER, Optional.of(host),
        makeTask(owner, jobName, negated)).isEmpty(), is(!expected));
  }

  private void assertNoVetoes(TwitterTaskInfo task) {
    assertNoVetoes(task, null);
  }

  private void assertNoVetoes(TwitterTaskInfo task, @Nullable String host) {
    assertVetoes(task, host);
  }

  private void assertVetoes(TwitterTaskInfo task, Veto... vetos) {
    assertVetoes(task, null, vetos);
  }

  private void assertVetoes(TwitterTaskInfo task, @Nullable String host, Veto... vetoes) {
    assertEquals(ImmutableSet.copyOf(vetoes), defaultFilter.filter(
        DEFAULT_OFFER, host == null ? Optional.<String>absent() : Optional.of(host), task));
  }

  private Attribute valueAttribute(String name, String string, String... strings) {
    return new Attribute(name,
        ImmutableSet.<String>builder().add(string).addAll(Arrays.asList(strings)).build());
  }

  private static Constraint makeConstraint(String name, String... values) {
    return new Constraint(name,
        TaskConstraint.value(new ValueConstraint(false, ImmutableSet.copyOf(values))));
  }

  private IExpectationSetters<ImmutableSet<ScheduledTask>> expectGetTasks(ScheduledTask... tasks) {
    return expect(taskStore.fetchTasks((TaskQuery) anyObject()))
        .andReturn(ImmutableSet.copyOf(tasks));
  }

  private IExpectationSetters<Iterable<Attribute>> expectGetHostAttributes(String host,
      Attribute... attributes) {
    return expect(attributeStore.getHostAttributes(host)).andReturn(Arrays.asList(attributes));
  }

  private final AtomicLong taskIdCounter = new AtomicLong();

  private ScheduledTask makeScheduledTask(Identity owner, String jobName, String host) {
    return new ScheduledTask().setAssignedTask(
        new AssignedTask()
            .setSlaveHost(host)
            .setTaskId("Task-" + taskIdCounter.incrementAndGet())
            .setTask(hostLimitTask(owner, jobName, 1 /* Max per host not used here. */)));
  }

  private Constraint limitConstraint(String name, int value) {
    return new Constraint(name, TaskConstraint.limit(new LimitConstraint(value)));
  }

  private TwitterTaskInfo makeTask(Identity owner, String jobName, Constraint... constraint) {
    return makeTask(OWNER_A, JOB_A, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .setOwner(owner)
        .setJobName(jobName)
        .setConstraints(Sets.newHashSet(constraint));
  }

  private TwitterTaskInfo hostLimitTask(Identity owner, String jobName, int maxPerHost) {
    return makeTask(owner, jobName, limitConstraint(HOST_ATTRIBUTE, maxPerHost));
  }

  private TwitterTaskInfo hostLimitTask(int maxPerHost) {
    return hostLimitTask(OWNER_A, JOB_A, maxPerHost);
  }

  private TwitterTaskInfo rackLimitTask(Identity owner, String jobName, int maxPerRack) {
    return makeTask(owner, jobName, limitConstraint(RACK_ATTRIBUTE, maxPerRack));
  }

  private TwitterTaskInfo makeTask(Identity owner, String jobName, int cpus, long ramMb,
      long diskMb) {
    return ConfigurationManager.applyDefaultsIfUnset(new TwitterTaskInfo()
        .setOwner(owner)
        .setJobName(jobName)
        .setNumCpus(cpus)
        .setRamMb(ramMb)
        .setDiskMb(diskMb)
        .setMaxPerHost(1));
  }

  private TwitterTaskInfo makeTask(int cpus, long ramMb, long diskMb) throws Exception {
    return makeTask(OWNER_A, JOB_A, cpus, ramMb, diskMb);
  }
}
