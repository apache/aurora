/**
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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.twitter.common.collections.Pair;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.filter.SchedulingFilter.UnusedResource;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.filter.SchedulingFilter.VetoGroup;
import org.apache.aurora.scheduler.filter.SchedulingFilter.VetoType;
import org.apache.aurora.scheduler.mesos.Offers;
import org.apache.aurora.scheduler.mesos.TaskExecutors;
import org.apache.aurora.scheduler.storage.entities.IAttribute;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.apache.aurora.scheduler.filter.AttributeAggregate.EMPTY;
import static org.apache.aurora.scheduler.filter.SchedulingFilterImpl.ResourceVector.CPU;
import static org.apache.aurora.scheduler.filter.SchedulingFilterImpl.ResourceVector.DISK;
import static org.apache.aurora.scheduler.filter.SchedulingFilterImpl.ResourceVector.PORTS;
import static org.apache.aurora.scheduler.filter.SchedulingFilterImpl.ResourceVector.RAM;
import static org.junit.Assert.assertEquals;

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
      Offers.createOffer(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK, Pair.of(80, 80)));

  private SchedulingFilter defaultFilter;

  @Before
  public void setUp() {
    defaultFilter = new SchedulingFilterImpl(TaskExecutors.NO_OVERHEAD_EXECUTOR);
  }

  @Test
  public void testMeetsOffer() {
    control.replay();

    IHostAttributes attributes = hostAttributes(HOST_A, host(HOST_A), rack(RACK_A));
    assertNoVetoes(makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK), attributes);
    assertNoVetoes(
        makeTask(DEFAULT_CPUS - 1, DEFAULT_RAM - 1, DEFAULT_DISK - 1),
        attributes);
  }

  @Test
  public void testSufficientPorts() {
    control.replay();

    ResourceSlot twoPorts = ResourceSlot.from(
        Offers.createOffer(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK, Pair.of(80, 81)));

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
    IHostAttributes hostA = hostAttributes(HOST_A, host(HOST_A), rack(RACK_A));
    assertEquals(
        none,
        defaultFilter.filter(
            new UnusedResource(twoPorts, hostA),
            new ResourceRequest(noPortTask, EMPTY)));
    assertEquals(
        none,
        defaultFilter.filter(
            new UnusedResource(twoPorts, hostA),
            new ResourceRequest(onePortTask, EMPTY)));
    assertEquals(
        none,
        defaultFilter.filter(
            new UnusedResource(twoPorts, hostA),
            new ResourceRequest(twoPortTask, EMPTY)));
    assertEquals(
        ImmutableSet.of(PORTS.veto(1)),
        defaultFilter.filter(
            new UnusedResource(twoPorts, hostA),
            new ResourceRequest(threePortTask, EMPTY)));
  }

  @Test
  public void testInsufficientResources() {
    control.replay();

    IHostAttributes hostA = hostAttributes(HOST_A, host(HOST_A), rack(RACK_A));
    assertVetoes(
        makeTask(DEFAULT_CPUS + 1, DEFAULT_RAM + 1, DEFAULT_DISK + 1),
        hostA,
        CPU.veto(1), DISK.veto(1), RAM.veto(1));
    assertVetoes(makeTask(DEFAULT_CPUS + 1, DEFAULT_RAM, DEFAULT_DISK), hostA, CPU.veto(1));
    assertVetoes(makeTask(DEFAULT_CPUS, DEFAULT_RAM + 1, DEFAULT_DISK), hostA, RAM.veto(1));
    assertVetoes(makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK + 1), hostA, DISK.veto(1));
  }

  @Test
  public void testDedicatedRole() {
    control.replay();

    IHostAttributes hostA = hostAttributes(HOST_A, dedicated(ROLE_A));
    checkConstraint(hostA, DEDICATED_ATTRIBUTE, true, ROLE_A);
    assertVetoes(makeTask(OWNER_B, JOB_B), hostA, Veto.dedicatedHostConstraintMismatch());
  }

  @Test
  public void testSharedDedicatedHost() {
    control.replay();

    String dedicated1 = "userA/jobA";
    String dedicated2 = "kestrel/kestrel";
    IHostAttributes hostA = hostAttributes(HOST_A, dedicated(dedicated1, dedicated2));
    assertNoVetoes(
        checkConstraint(
            new Identity().setRole("userA"),
            "jobA",
            hostA,
            DEDICATED_ATTRIBUTE,
            true,
            dedicated1),
        hostA);
    assertNoVetoes(
        checkConstraint(
            new Identity().setRole("kestrel"),
            "kestrel",
            hostA,
            DEDICATED_ATTRIBUTE,
            true,
            dedicated2),
        hostA);
  }

  @Test
  public void testMultiValuedAttributes() {
    control.replay();

    IHostAttributes hostA = hostAttributes(HOST_A, valueAttribute("jvm", "1.0", "2.0", "3.0"));
    checkConstraint(hostA, "jvm", true, "1.0");
    checkConstraint(hostA, "jvm", false, "4.0");

    checkConstraint(hostA, "jvm", true, "1.0", "2.0");
    IHostAttributes hostB = hostAttributes(HOST_A, valueAttribute("jvm", "1.0"));
    checkConstraint(hostB, "jvm", false, "2.0", "3.0");
  }

  @Test
  public void testHostScheduledForMaintenance() {
    control.replay();

    assertNoVetoes(
        makeTask(),
        hostAttributes(HOST_A, MaintenanceMode.SCHEDULED, host(HOST_A), rack(RACK_A)));
  }

  @Test
  public void testHostDrainingForMaintenance() {
    control.replay();

    assertVetoes(
        makeTask(),
        hostAttributes(HOST_A, MaintenanceMode.DRAINING, host(HOST_A), rack(RACK_A)),
        Veto.maintenance("draining"));
  }

  @Test
  public void testHostDrainedForMaintenance() {
    control.replay();

    assertVetoes(
        makeTask(),
        hostAttributes(HOST_A, MaintenanceMode.DRAINED, host(HOST_A), rack(RACK_A)),
        Veto.maintenance("drained"));
  }

  @Test
  public void testMultipleTaskConstraints() {
    control.replay();

    Constraint constraint1 = makeConstraint("host", HOST_A);
    Constraint constraint2 = makeConstraint(DEDICATED_ATTRIBUTE, "xxx");

    assertVetoes(
        makeTask(OWNER_A, JOB_A, constraint1, constraint2),
        hostAttributes(HOST_A, dedicated(HOST_A), host(HOST_A)),
        Veto.constraintMismatch(DEDICATED_ATTRIBUTE));
    assertNoVetoes(
        makeTask(OWNER_B, JOB_B, constraint1, constraint2),
        hostAttributes(HOST_B, dedicated("xxx"), host(HOST_A)));
  }

  @Test
  public void testDedicatedMismatchShortCircuits() {
    // Ensures that a dedicated mismatch short-circuits other filter operations, such as
    // evaluation of limit constraints.  Reduction of task queries is the desired outcome.

    control.replay();

    Constraint hostLimit = limitConstraint("host", 1);
    assertVetoes(
        makeTask(OWNER_A, JOB_A, hostLimit, makeConstraint(DEDICATED_ATTRIBUTE, "xxx")),
        hostAttributes(HOST_A, host(HOST_A)),
        Veto.constraintMismatch(DEDICATED_ATTRIBUTE));
    assertVetoes(
        makeTask(OWNER_B, JOB_A, hostLimit, makeConstraint(DEDICATED_ATTRIBUTE, "xxx")),
        hostAttributes(HOST_B, dedicated(OWNER_B.getRole() + "/" + JOB_B), host(HOST_B)),
        Veto.constraintMismatch(DEDICATED_ATTRIBUTE));
  }

  @Test
  public void testUnderLimitNoTasks() {
    control.replay();

    assertNoVetoes(hostLimitTask(2), hostAttributes(HOST_A, host(HOST_A)));
  }

  private IAttribute host(String host) {
    return valueAttribute(HOST_ATTRIBUTE, host);
  }

  private IAttribute rack(String rack) {
    return valueAttribute(RACK_ATTRIBUTE, rack);
  }

  private IAttribute dedicated(String value, String... values) {
    return valueAttribute(DEDICATED_ATTRIBUTE, value, values);
  }

  @Test
  public void testLimitWithinJob() throws Exception {
    control.replay();

    AttributeAggregate stateA = AttributeAggregate.create(
        Suppliers.<Iterable<IAttribute>>ofInstance(ImmutableList.of(
            host(HOST_A),
            rack(RACK_A),
            host(HOST_B),
            rack(RACK_A),
            host(HOST_B),
            rack(RACK_A),
            host(HOST_C),
            rack(RACK_B))));
    AttributeAggregate stateB = AttributeAggregate.create(
        Suppliers.<Iterable<IAttribute>>ofInstance(ImmutableList.of(
            host(HOST_A),
            rack(RACK_A),
            host(HOST_A),
            rack(RACK_A),
            host(HOST_B),
            rack(RACK_A))));

    IHostAttributes hostA = hostAttributes(HOST_A, host(HOST_A), rack(RACK_A));
    IHostAttributes hostB = hostAttributes(HOST_B, host(HOST_B), rack(RACK_A));
    IHostAttributes hostC = hostAttributes(HOST_C, host(HOST_C), rack(RACK_B));
    assertNoVetoes(hostLimitTask(OWNER_A, JOB_A, 2), hostA, stateA);
    assertVetoes(
        hostLimitTask(OWNER_A, JOB_A, 1),
        hostB,
        stateA,
        Veto.unsatisfiedLimit(HOST_ATTRIBUTE));
    assertVetoes(
        hostLimitTask(OWNER_A, JOB_A, 2),
        hostB,
        stateA,
        Veto.unsatisfiedLimit(HOST_ATTRIBUTE));
    assertNoVetoes(hostLimitTask(OWNER_A, JOB_A, 3), hostB, stateA);

    assertVetoes(
        rackLimitTask(OWNER_B, JOB_A, 2),
        hostB,
        stateB,
        Veto.unsatisfiedLimit(RACK_ATTRIBUTE));
    assertVetoes(
        rackLimitTask(OWNER_B, JOB_A, 3),
        hostB,
        stateB,
        Veto.unsatisfiedLimit(RACK_ATTRIBUTE));
    assertNoVetoes(rackLimitTask(OWNER_B, JOB_A, 4), hostB, stateB);

    assertNoVetoes(rackLimitTask(OWNER_B, JOB_A, 1), hostC, stateB);

    assertVetoes(
        rackLimitTask(OWNER_A, JOB_A, 1),
        hostC,
        stateA,
        Veto.unsatisfiedLimit(RACK_ATTRIBUTE));
    assertNoVetoes(rackLimitTask(OWNER_B, JOB_A, 2), hostC, stateB);
  }

  @Test
  public void testAttribute() {
    control.replay();

    IHostAttributes hostA = hostAttributes(HOST_A, valueAttribute("jvm", "1.0"));

    // Matches attribute, matching value.
    checkConstraint(hostA, "jvm", true, "1.0");

    // Matches attribute, different value.
    checkConstraint(hostA, "jvm", false, "1.4");

    // Does not match attribute.
    checkConstraint(hostA, "xxx", false, "1.4");

    // Logical 'OR' matching attribute.
    checkConstraint(hostA, "jvm", false, "1.2", "1.4");

    // Logical 'OR' not matching attribute.
    checkConstraint(hostA, "xxx", false, "1.0", "1.4");
  }

  @Test
  public void testAttributes() {
    control.replay();

    IHostAttributes hostA = hostAttributes(
        HOST_A,
        valueAttribute("jvm", "1.4", "1.6", "1.7"),
        valueAttribute("zone", "a", "b", "c"));

    // Matches attribute, matching value.
    checkConstraint(hostA, "jvm", true, "1.4");

    // Matches attribute, different value.
    checkConstraint(hostA, "jvm", false, "1.0");

    // Does not match attribute.
    checkConstraint(hostA, "xxx", false, "1.4");

    // Logical 'OR' with attribute and value match.
    checkConstraint(hostA, "jvm", true, "1.2", "1.4");

    // Does not match attribute.
    checkConstraint(hostA, "xxx", false, "1.0", "1.4");

    // Check that logical AND works.
    Constraint jvmConstraint = makeConstraint("jvm", "1.6");
    Constraint zoneConstraint = makeConstraint("zone", "c");

    ITaskConfig task = makeTask(OWNER_A, JOB_A, jvmConstraint, zoneConstraint);
    assertEquals(
        ImmutableSet.<Veto>of(),
        defaultFilter.filter(
            new UnusedResource(DEFAULT_OFFER, hostA),
            new ResourceRequest(task, EMPTY)));

    Constraint jvmNegated = jvmConstraint.deepCopy();
    jvmNegated.getConstraint().getValue().setNegated(true);
    Constraint zoneNegated = jvmConstraint.deepCopy();
    zoneNegated.getConstraint().getValue().setNegated(true);
    assertVetoes(
        makeTask(OWNER_A, JOB_A, jvmNegated, zoneNegated),
        hostA,
        Veto.constraintMismatch("jvm"));
  }

  @Test
  public void testVetoScaling() {
    control.replay();

    int maxScore = VetoType.INSUFFICIENT_RESOURCES.getScore();
    assertEquals((int) (maxScore * 1.0 / CPU.getRange()), CPU.veto(1).getScore());
    assertEquals(maxScore, CPU.veto(CPU.getRange() * 10).getScore());
    assertEquals((int) (maxScore * 2.0 / RAM.getRange()), RAM.veto(2).getScore());
    assertEquals((int) (maxScore * 200.0 / DISK.getRange()), DISK.veto(200).getScore());
  }

  @Test
  public void testDuplicatedAttribute() {
    control.replay();

    IHostAttributes hostA = hostAttributes(HOST_A,
        valueAttribute("jvm", "1.4"),
        valueAttribute("jvm", "1.6", "1.7"));

    // Matches attribute, matching value.
    checkConstraint(hostA, "jvm", true, "1.4");
    checkConstraint(hostA, "jvm", true, "1.6");
    checkConstraint(hostA, "jvm", true, "1.7");
    checkConstraint(hostA, "jvm", true, "1.6", "1.7");
  }

  @Test
  public void testVetoGroups() {
    control.replay();

    assertEquals(VetoGroup.EMPTY, Veto.identifyGroup(ImmutableSet.<Veto>of()));

    assertEquals(
        VetoGroup.STATIC,
        Veto.identifyGroup(ImmutableSet.of(
            Veto.constraintMismatch("denied"),
            Veto.insufficientResources("ram", 100),
            Veto.maintenance("draining"))));

    assertEquals(
        VetoGroup.DYNAMIC,
        Veto.identifyGroup(ImmutableSet.of(Veto.unsatisfiedLimit("denied"))));

    assertEquals(
        VetoGroup.MIXED,
        Veto.identifyGroup(ImmutableSet.of(
            Veto.insufficientResources("ram", 100),
            Veto.unsatisfiedLimit("denied"))));
  }

  private ITaskConfig checkConstraint(
      IHostAttributes hostAttributes,
      String constraintName,
      boolean expected,
      String value,
      String... vs) {

    return checkConstraint(
        OWNER_A,
        JOB_A,
        hostAttributes,
        constraintName,
        expected,
        value,
        vs);
  }

  private ITaskConfig checkConstraint(
      Identity owner,
      String jobName,
      IHostAttributes hostAttributes,
      String constraintName,
      boolean expected,
      String value,
      String... vs) {

    return checkConstraint(
        owner,
        jobName,
        EMPTY,
        hostAttributes,
        constraintName,
        expected,
        new ValueConstraint(false,
            ImmutableSet.<String>builder().add(value).addAll(Arrays.asList(vs)).build()));
  }

  private ITaskConfig checkConstraint(
      Identity owner,
      String jobName,
      AttributeAggregate aggregate,
      IHostAttributes hostAttributes,
      String constraintName,
      boolean expected,
      ValueConstraint value) {

    Constraint constraint = new Constraint(constraintName, TaskConstraint.value(value));
    ITaskConfig task = makeTask(owner, jobName, constraint);
    assertEquals(
        expected,
        defaultFilter.filter(
            new UnusedResource(DEFAULT_OFFER, hostAttributes),
            new ResourceRequest(task, aggregate))
            .isEmpty());

    Constraint negated = constraint.deepCopy();
    negated.getConstraint().getValue().setNegated(!value.isNegated());
    ITaskConfig negatedTask = makeTask(owner, jobName, negated);
    assertEquals(
        !expected,
        defaultFilter.filter(
            new UnusedResource(DEFAULT_OFFER, hostAttributes),
            new ResourceRequest(negatedTask, aggregate))
            .isEmpty());
    return task;
  }

  private void assertNoVetoes(ITaskConfig task, IHostAttributes hostAttributes) {
    assertVetoes(task, hostAttributes, EMPTY);
  }

  private void assertNoVetoes(
      ITaskConfig task,
      IHostAttributes attributes,
      AttributeAggregate jobState) {

    assertVetoes(task, attributes, jobState);
  }

  private void assertVetoes(ITaskConfig task, IHostAttributes hostAttributes, Veto... vetoes) {
    assertVetoes(task, hostAttributes, EMPTY, vetoes);
  }

  private void assertVetoes(
      ITaskConfig task,
      IHostAttributes hostAttributes,
      AttributeAggregate jobState,
      Veto... vetoes) {

    assertEquals(
        ImmutableSet.copyOf(vetoes),
        defaultFilter.filter(
            new UnusedResource(DEFAULT_OFFER, hostAttributes),
            new ResourceRequest(task, jobState)));
  }

  private static IHostAttributes hostAttributes(
      String host,
      MaintenanceMode mode,
      IAttribute... attributes) {

    return IHostAttributes.build(
        new HostAttributes()
            .setHost(host)
            .setMode(mode)
            .setAttributes(IAttribute.toBuildersSet(ImmutableSet.copyOf(attributes))));
  }

  private static IHostAttributes hostAttributes(
      String host,
      IAttribute... attributes) {

    return hostAttributes(host, MaintenanceMode.NONE, attributes);
  }

  private IAttribute valueAttribute(String name, String string, String... strings) {
    return IAttribute.build(new Attribute(name,
        ImmutableSet.<String>builder().add(string).addAll(Arrays.asList(strings)).build()));
  }

  private static Constraint makeConstraint(String name, String... values) {
    return new Constraint(name,
        TaskConstraint.value(new ValueConstraint(false, ImmutableSet.copyOf(values))));
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

  private ITaskConfig makeTask(int cpus, long ramMb, long diskMb) {
    return makeTask(OWNER_A, JOB_A, cpus, ramMb, diskMb);
  }

  private ITaskConfig makeTask() {
    return makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK);
  }
}
