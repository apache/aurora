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
package org.apache.aurora.scheduler.filter;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Optional;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.state.MaintenanceController;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work.Quiet;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.apache.aurora.scheduler.filter.ConstraintFilter.limitVeto;
import static org.apache.aurora.scheduler.filter.ConstraintFilter.mismatchVeto;
import static org.apache.aurora.scheduler.filter.SchedulingFilterImpl.DEDICATED_HOST_VETO;
import static org.apache.aurora.scheduler.filter.SchedulingFilterImpl.ResourceVector.CPU;
import static org.apache.aurora.scheduler.filter.SchedulingFilterImpl.ResourceVector.DISK;
import static org.apache.aurora.scheduler.filter.SchedulingFilterImpl.ResourceVector.PORTS;
import static org.apache.aurora.scheduler.filter.SchedulingFilterImpl.ResourceVector.RAM;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SchedulingFilterImplTest extends EasyMockTest {

  private static final String TASK_ID = "taskId";

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
  private static final ResourceSlot DEFAULT_OFFER = ResourceSlot.from(
      DEFAULT_CPUS,
      Amount.of(DEFAULT_RAM, Data.MB),
      Amount.of(DEFAULT_DISK, Data.MB),
      0);

  private AttributeAggregate emptyJob;

  private final AtomicLong taskIdCounter = new AtomicLong();

  private SchedulingFilter defaultFilter;
  private MaintenanceController maintenance;
  private Storage storage;
  private StoreProvider storeProvider;
  private AttributeStore.Mutable attributeStore;

  @Before
  public void setUp() throws Exception {
    storage = createMock(Storage.class);
    maintenance = createMock(MaintenanceController.class);
    defaultFilter = new SchedulingFilterImpl(storage, maintenance);
    storeProvider = createMock(StoreProvider.class);
    attributeStore = createMock(AttributeStore.Mutable.class);
    emptyJob = new AttributeAggregate(
        Suppliers.ofInstance(ImmutableSet.<IScheduledTask>of()),
        attributeStore);

    // Link the store provider to the store mocks.
    expectReads();

    expect(storeProvider.getAttributeStore()).andReturn(attributeStore).anyTimes();
  }

  private void expectReads() throws Exception {
    expect(storage.weaklyConsistentRead(EasyMock.<Quiet<?>>anyObject()))
        .andAnswer(new IAnswer<Object>() {
          @Override
          public Object answer() throws Exception {
            Quiet<?> arg = (Quiet<?>) EasyMock.getCurrentArguments()[0];
            return arg.apply(storeProvider);
          }
        }).anyTimes();
  }

  @Test
  public void testMeetsOffer() throws Exception {
    expectGetHostAttributes(HOST_A, host(HOST_A), rack(RACK_A)).atLeastOnce();
    expectGetHostMaintenanceStatus(HOST_A).times(2);

    control.replay();

    assertNoVetoes(makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK));
    assertNoVetoes(makeTask(DEFAULT_CPUS - 1, DEFAULT_RAM - 1, DEFAULT_DISK - 1), emptyJob);
  }

  @Test
  public void testSufficientPorts() throws Exception {
    expectGetHostAttributes(HOST_A, host(HOST_A), rack(RACK_A)).atLeastOnce();
    expectGetHostMaintenanceStatus(HOST_A).times(4);

    control.replay();

    ResourceSlot twoPorts = ResourceSlot.from(
        DEFAULT_CPUS,
        Amount.of(DEFAULT_RAM, Data.MB),
        Amount.of(DEFAULT_DISK, Data.MB),
        2);

    ITaskConfig noPortTask = ITaskConfig.build(makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .newBuilder()
        .setRequestedPorts(ImmutableSet.<String>of()));
    ITaskConfig onePortTask = ITaskConfig.build(makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .newBuilder()
        .setRequestedPorts(ImmutableSet.of("one")));
    ITaskConfig twoPortTask = ITaskConfig.build(makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .newBuilder()
        .setRequestedPorts(ImmutableSet.of("one", "two")));
    ITaskConfig threePortTask = ITaskConfig.build(makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .newBuilder()
        .setRequestedPorts(ImmutableSet.of("one", "two", "three")));

    Set<Veto> none = ImmutableSet.of();
    assertEquals(none, defaultFilter.filter(twoPorts, HOST_A, noPortTask, TASK_ID, emptyJob));
    assertEquals(none, defaultFilter.filter(twoPorts, HOST_A, onePortTask, TASK_ID, emptyJob));
    assertEquals(none, defaultFilter.filter(twoPorts, HOST_A, twoPortTask, TASK_ID, emptyJob));
    assertEquals(
        ImmutableSet.of(PORTS.veto(1)),
        defaultFilter.filter(twoPorts, HOST_A, threePortTask, TASK_ID, emptyJob));
  }

  @Test
  public void testInsufficientResources() throws Exception {
    expectGetHostAttributes(HOST_A, host(HOST_A), rack(RACK_A)).atLeastOnce();
    expectGetHostMaintenanceStatus(HOST_A).times(4);

    control.replay();

    assertVetoes(
        makeTask(DEFAULT_CPUS + 1, DEFAULT_RAM + 1, DEFAULT_DISK + 1),
        CPU.veto(1), DISK.veto(1), RAM.veto(1));
    assertVetoes(makeTask(DEFAULT_CPUS + 1, DEFAULT_RAM, DEFAULT_DISK), CPU.veto(1));
    assertVetoes(makeTask(DEFAULT_CPUS, DEFAULT_RAM + 1, DEFAULT_DISK), RAM.veto(1));
    assertVetoes(makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK + 1), DISK.veto(1));
  }

  @Test
  public void testDedicatedRole() throws Exception {
    expectGetHostAttributes(HOST_A, dedicated(ROLE_A)).anyTimes();
    expectGetHostMaintenanceStatus(HOST_A).times(2);

    control.replay();

    checkConstraint(HOST_A, DEDICATED_ATTRIBUTE, true, ROLE_A);
    assertVetoes(makeTask(OWNER_B, JOB_B), HOST_A, DEDICATED_HOST_VETO);
  }

  @Test
  public void testSharedDedicatedHost() throws Exception {
    String dedicated1 = "userA/jobA";
    String dedicated2 = "kestrel/kestrel";

    expectGetHostAttributes(HOST_A, dedicated(dedicated1, dedicated2)).anyTimes();
    expectGetHostMaintenanceStatus(HOST_A).atLeastOnce();

    control.replay();

    assertNoVetoes(checkConstraint(
        new Identity().setRole("userA"),
        "jobA",
        HOST_A,
        DEDICATED_ATTRIBUTE,
        true,
        dedicated1));
    assertNoVetoes(checkConstraint(
        new Identity().setRole("kestrel"),
        "kestrel",
        HOST_A,
        DEDICATED_ATTRIBUTE,
        true,
        dedicated2));
  }

  @Test
  public void testMultiValuedAttributes() throws Exception {
    expectGetHostAttributes(HOST_A, valueAttribute("jvm", "1.0", "2.0", "3.0")).anyTimes();
    expectGetHostAttributes(HOST_B, valueAttribute("jvm", "1.0")).anyTimes();
    expectGetHostMaintenanceStatus(HOST_A).atLeastOnce();
    expectGetHostMaintenanceStatus(HOST_B).atLeastOnce();

    control.replay();

    checkConstraint(HOST_A, "jvm", true, "1.0");
    checkConstraint(HOST_A, "jvm", false, "4.0");

    checkConstraint(HOST_A, "jvm", true, "1.0", "2.0");
    checkConstraint(HOST_B, "jvm", false, "2.0", "3.0");
  }

  @Test
  public void testHostScheduledForMaintenance() throws Exception {
    expectGetHostAttributes(HOST_A, host(HOST_A), rack(RACK_A)).atLeastOnce();
    expectGetHostMaintenanceStatus(HOST_A, MaintenanceMode.SCHEDULED);

    control.replay();

    assertNoVetoes(makeTask(), HOST_A);
  }

  @Test
  public void testHostDrainingForMaintenance() throws Exception {
    expectGetHostAttributes(HOST_A, host(HOST_A), rack(RACK_A)).atLeastOnce();
    expectGetHostMaintenanceStatus(HOST_A, MaintenanceMode.DRAINING);

    control.replay();

    assertVetoes(makeTask(), ConstraintFilter.maintenanceVeto("draining"));
  }

  @Test
  public void testHostDrainedForMaintenance() throws Exception {
    expectGetHostAttributes(HOST_A, host(HOST_A), rack(RACK_A)).atLeastOnce();
    expectGetHostMaintenanceStatus(HOST_A, MaintenanceMode.DRAINED);

    control.replay();

    assertVetoes(makeTask(), ConstraintFilter.maintenanceVeto("drained"));
  }

  @Test
  public void testMultipleTaskConstraints() throws Exception {
    expectGetHostAttributes(HOST_A, dedicated(HOST_A), host(HOST_A));
    expectGetHostAttributes(HOST_B, dedicated("xxx"), host(HOST_A));
    expectGetHostMaintenanceStatus(HOST_A);
    expectGetHostMaintenanceStatus(HOST_B);

    control.replay();

    Constraint constraint1 = makeConstraint("host", HOST_A);
    Constraint constraint2 = makeConstraint(DEDICATED_ATTRIBUTE, "xxx");

    assertVetoes(
        makeTask(OWNER_A, JOB_A, constraint1, constraint2),
        HOST_A,
        mismatchVeto(DEDICATED_ATTRIBUTE));
    assertNoVetoes(makeTask(OWNER_B, JOB_B, constraint1, constraint2), HOST_B);
  }

  @Test
  public void testDedicatedMismatchShortCircuits() throws Exception {
    // Ensures that a dedicated mismatch short-circuits other filter operations, such as
    // evaluation of limit constraints.  Reduction of task queries is the desired outcome.

    expectGetHostAttributes(HOST_A, host(HOST_A));
    expectGetHostAttributes(HOST_B, dedicated(OWNER_B.getRole() + "/" + JOB_B), host(HOST_B));
    expectGetHostMaintenanceStatus(HOST_A);
    expectGetHostMaintenanceStatus(HOST_B);

    control.replay();

    Constraint hostLimit = limitConstraint("host", 1);
    assertVetoes(
        makeTask(OWNER_A, JOB_A, hostLimit, makeConstraint(DEDICATED_ATTRIBUTE, "xxx")),
        HOST_A,
        mismatchVeto(DEDICATED_ATTRIBUTE));
    assertVetoes(
        makeTask(OWNER_B, JOB_A, hostLimit, makeConstraint(DEDICATED_ATTRIBUTE, "xxx")),
        HOST_B,
        mismatchVeto(DEDICATED_ATTRIBUTE));
  }

  @Test
  public void testUnderLimitNoTasks() throws Exception {
    expectGetHostAttributes(HOST_A);
    expectGetHostAttributes(HOST_A, host(HOST_A));
    expectGetHostMaintenanceStatus(HOST_A);

    control.replay();

    assertNoVetoes(hostLimitTask(2), HOST_A);
  }

  private Attribute host(String host) {
    return valueAttribute(HOST_ATTRIBUTE, host);
  }

  private Attribute rack(String rack) {
    return valueAttribute(RACK_ATTRIBUTE, rack);
  }

  private Attribute dedicated(String value, String... values) {
    return valueAttribute(DEDICATED_ATTRIBUTE, value, values);
  }

  @Test
  public void testLimitWithinJob() throws Exception {
    expectGetHostAttributes(HOST_A, host(HOST_A), rack(RACK_A)).atLeastOnce();
    expectGetHostAttributes(HOST_B, host(HOST_B), rack(RACK_A)).atLeastOnce();
    expectGetHostAttributes(HOST_C, host(HOST_C), rack(RACK_B)).atLeastOnce();
    expectGetHostMaintenanceStatus(HOST_A).atLeastOnce();
    expectGetHostMaintenanceStatus(HOST_B).atLeastOnce();
    expectGetHostMaintenanceStatus(HOST_C).atLeastOnce();

    AttributeAggregate stateA = new AttributeAggregate(Suppliers.ofInstance(ImmutableSet.of(
        makeScheduledTask(OWNER_A, JOB_A, HOST_A),
        makeScheduledTask(OWNER_A, JOB_A, HOST_B),
        makeScheduledTask(OWNER_A, JOB_A, HOST_B),
        makeScheduledTask(OWNER_A, JOB_A, HOST_C))),
        attributeStore);
    AttributeAggregate stateB = new AttributeAggregate(Suppliers.ofInstance(ImmutableSet.of(
        makeScheduledTask(OWNER_B, JOB_A, HOST_A),
        makeScheduledTask(OWNER_B, JOB_A, HOST_A),
        makeScheduledTask(OWNER_B, JOB_A, HOST_B))),
        attributeStore);

    control.replay();

    assertNoVetoes(hostLimitTask(OWNER_A, JOB_A, 2), HOST_A, stateA);
    assertVetoes(hostLimitTask(OWNER_A, JOB_A, 1), HOST_B, stateA, limitVeto(HOST_ATTRIBUTE));
    assertVetoes(hostLimitTask(OWNER_A, JOB_A, 2), HOST_B, stateA, limitVeto(HOST_ATTRIBUTE));
    assertNoVetoes(hostLimitTask(OWNER_A, JOB_A, 3), HOST_B, stateA);

    assertVetoes(rackLimitTask(OWNER_B, JOB_A, 2), HOST_B, stateB, limitVeto(RACK_ATTRIBUTE));
    assertVetoes(rackLimitTask(OWNER_B, JOB_A, 3), HOST_B, stateB, limitVeto(RACK_ATTRIBUTE));
    assertNoVetoes(rackLimitTask(OWNER_B, JOB_A, 4), HOST_B, stateB);

    assertNoVetoes(rackLimitTask(OWNER_B, JOB_A, 1), HOST_C, stateB);

    assertVetoes(rackLimitTask(OWNER_A, JOB_A, 1), HOST_C, stateA, limitVeto(RACK_ATTRIBUTE));
    assertNoVetoes(rackLimitTask(OWNER_B, JOB_A, 2), HOST_C, stateB);
  }

  @Test
  public void testAttribute() throws Exception {
    expectGetHostAttributes(HOST_A, valueAttribute("jvm", "1.0")).atLeastOnce();
    expectGetHostMaintenanceStatus(HOST_A).atLeastOnce();

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
    expectGetHostMaintenanceStatus(HOST_A).atLeastOnce();

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

    ITaskConfig task = makeTask(OWNER_A, JOB_A, jvmConstraint, zoneConstraint);
    assertTrue(defaultFilter.filter(DEFAULT_OFFER, HOST_A, task, TASK_ID, emptyJob).isEmpty());

    Constraint jvmNegated = jvmConstraint.deepCopy();
    jvmNegated.getConstraint().getValue().setNegated(true);
    Constraint zoneNegated = jvmConstraint.deepCopy();
    zoneNegated.getConstraint().getValue().setNegated(true);
    assertVetoes(
        makeTask(OWNER_A, JOB_A, jvmNegated, zoneNegated),
        HOST_A,
        mismatchVeto("jvm"));
  }

  @Test
  public void testVetoScaling() {
    control.replay();

    assertEquals((int) (Veto.MAX_SCORE * 1.0 / CPU.getRange()), CPU.veto(1).getScore());
    assertEquals(Veto.MAX_SCORE, CPU.veto(CPU.getRange() * 10).getScore());
    assertEquals((int) (Veto.MAX_SCORE * 2.0 / RAM.getRange()), RAM.veto(2).getScore());
    assertEquals((int) (Veto.MAX_SCORE * 200.0 / DISK.getRange()), DISK.veto(200).getScore());
  }

  @Test
  public void testDuplicatedAttribute() throws Exception {
    expectGetHostAttributes(HOST_A,
        valueAttribute("jvm", "1.4"),
        valueAttribute("jvm", "1.6", "1.7")).atLeastOnce();
    expectGetHostMaintenanceStatus(HOST_A).atLeastOnce();

    control.replay();

    // Matches attribute, matching value.
    checkConstraint(HOST_A, "jvm", true, "1.4");
    checkConstraint(HOST_A, "jvm", true, "1.6");
    checkConstraint(HOST_A, "jvm", true, "1.7");
    checkConstraint(HOST_A, "jvm", true, "1.6", "1.7");
  }

  private ITaskConfig checkConstraint(
      String host,
      String constraintName,
      boolean expected,
      String value,
      String... vs) {

    return checkConstraint(
        OWNER_A,
        JOB_A,
        host,
        constraintName,
        expected,
        value,
        vs);
  }

  private ITaskConfig checkConstraint(
      Identity owner,
      String jobName,
      String host,
      String constraintName,
      boolean expected,
      String value,
      String... vs) {

    return checkConstraint(owner, jobName, emptyJob, host, constraintName, expected,
        new ValueConstraint(false,
            ImmutableSet.<String>builder().add(value).addAll(Arrays.asList(vs)).build()));
  }

  private ITaskConfig checkConstraint(
      Identity owner,
      String jobName,
      AttributeAggregate aggregate,
      String host,
      String constraintName,
      boolean expected,
      ValueConstraint value) {

    Constraint constraint = new Constraint(constraintName, TaskConstraint.value(value));
    ITaskConfig task = makeTask(owner, jobName, constraint);
    assertEquals(
        expected,
        defaultFilter.filter(DEFAULT_OFFER, host, task, TASK_ID, aggregate).isEmpty());

    Constraint negated = constraint.deepCopy();
    negated.getConstraint().getValue().setNegated(!value.isNegated());
    ITaskConfig negatedTask = makeTask(owner, jobName, negated);
    assertEquals(
        !expected,
        defaultFilter.filter(DEFAULT_OFFER, host, negatedTask, TASK_ID, aggregate).isEmpty());
    return task;
  }

  private void assertNoVetoes(ITaskConfig task) {
    assertNoVetoes(task, emptyJob);
  }

  private void assertNoVetoes(ITaskConfig task, AttributeAggregate jobState) {
    assertNoVetoes(task, HOST_A, jobState);
  }

  private void assertNoVetoes(ITaskConfig task, String host) {
    assertVetoes(task, host, emptyJob);
  }

  private void assertNoVetoes(ITaskConfig task, String host, AttributeAggregate jobState) {
    assertVetoes(task, host, jobState);
  }

  private void assertVetoes(ITaskConfig task, Veto... vetos) {
    assertVetoes(task, emptyJob, vetos);
  }

  private void assertVetoes(ITaskConfig task, AttributeAggregate jobState, Veto... vetos) {
    assertVetoes(task, HOST_A, jobState, vetos);
  }

  private void assertVetoes(
      ITaskConfig task,
      String host,
      Veto... vetoes) {

    assertVetoes(task, host, emptyJob, vetoes);
  }

  private void assertVetoes(
      ITaskConfig task,
      String host,
      AttributeAggregate jobState,
      Veto... vetoes) {

    assertEquals(
        ImmutableSet.copyOf(vetoes),
        defaultFilter.filter(DEFAULT_OFFER, host, task, TASK_ID, jobState));
  }

  private Attribute valueAttribute(String name, String string, String... strings) {
    return new Attribute(name,
        ImmutableSet.<String>builder().add(string).addAll(Arrays.asList(strings)).build());
  }

  private static Constraint makeConstraint(String name, String... values) {
    return new Constraint(name,
        TaskConstraint.value(new ValueConstraint(false, ImmutableSet.copyOf(values))));
  }

  private IExpectationSetters<MaintenanceMode> expectGetHostMaintenanceStatus(String host) {
    return expectGetHostMaintenanceStatus(host, MaintenanceMode.NONE);
  }

  private IExpectationSetters<MaintenanceMode> expectGetHostMaintenanceStatus(
      String host, MaintenanceMode mode) {
    return expect(maintenance.getMode(host)).andReturn(mode);
  }

  private IExpectationSetters<Optional<HostAttributes>> expectGetHostAttributes(
      String host,
      Attribute... attributes) {

    HostAttributes hostAttributes = new HostAttributes()
        .setHost(host)
        .setAttributes(ImmutableSet.<Attribute>builder().add(attributes).build());
    return expect(attributeStore.getHostAttributes(host)).andReturn(Optional.of(hostAttributes));
  }

  private IScheduledTask makeScheduledTask(Identity owner, String jobName, String host) {
    return IScheduledTask.build(new ScheduledTask().setAssignedTask(
        new AssignedTask()
            .setSlaveHost(host)
            .setTaskId("Task-" + taskIdCounter.incrementAndGet())
            .setTask(hostLimitTask(owner, jobName, 1 /* Max per host not used here. */)
                .newBuilder())));
  }

  private Constraint limitConstraint(String name, int value) {
    return new Constraint(name, TaskConstraint.limit(new LimitConstraint(value)));
  }

  private ITaskConfig makeTask(Identity owner, String jobName, Constraint... constraint) {
    return ITaskConfig.build(makeTask(OWNER_A, JOB_A, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .newBuilder()
        .setOwner(owner)
        .setJobName(jobName)
        .setConstraints(Sets.newHashSet(constraint)));
  }

  private ITaskConfig hostLimitTask(Identity owner, String jobName, int maxPerHost) {
    return makeTask(owner, jobName, limitConstraint(HOST_ATTRIBUTE, maxPerHost));
  }

  private ITaskConfig hostLimitTask(int maxPerHost) {
    return hostLimitTask(OWNER_A, JOB_A, maxPerHost);
  }

  private ITaskConfig rackLimitTask(Identity owner, String jobName, int maxPerRack) {
    return makeTask(owner, jobName, limitConstraint(RACK_ATTRIBUTE, maxPerRack));
  }

  private ITaskConfig makeTask(
      Identity owner,
      String jobName,
      int cpus,
      long ramMb,
      long diskMb) {

    return ITaskConfig.build(ConfigurationManager.applyDefaultsIfUnset(new TaskConfig()
        .setOwner(owner)
        .setJobName(jobName)
        .setNumCpus(cpus)
        .setRamMb(ramMb)
        .setDiskMb(diskMb)
        .setExecutorConfig(new ExecutorConfig("aurora", "config"))));
  }

  private ITaskConfig makeTask(int cpus, long ramMb, long diskMb) throws Exception {
    return makeTask(OWNER_A, JOB_A, cpus, ramMb, diskMb);
  }

  private ITaskConfig makeTask() throws Exception {
    return makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK);
  }
}
