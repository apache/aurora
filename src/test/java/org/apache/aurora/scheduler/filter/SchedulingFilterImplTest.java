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

import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.gen.apiConstants;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.filter.SchedulingFilter.ResourceRequest;
import org.apache.aurora.scheduler.filter.SchedulingFilter.UnusedResource;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.filter.SchedulingFilter.VetoGroup;
import org.apache.aurora.scheduler.filter.SchedulingFilter.VetoType;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.entities.IAttribute;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.Resource.diskMb;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.gen.Resource.ramMb;
import static org.apache.aurora.scheduler.base.TaskTestUtil.TIER_MANAGER;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.apache.aurora.scheduler.filter.AttributeAggregate.empty;
import static org.apache.aurora.scheduler.mesos.TaskExecutors.NO_OVERHEAD_EXECUTOR;
import static org.apache.aurora.scheduler.resources.ResourceManager.bagFromMesosResources;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosRange;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.resetPorts;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;
import static org.junit.Assert.assertEquals;

public class SchedulingFilterImplTest extends EasyMockTest {
  private static final String HOST_A = "hostA";
  private static final String HOST_B = "hostB";
  private static final String HOST_C = "hostC";

  private static final String RACK_A = "rackA";
  private static final String RACK_B = "rackB";

  private static final String RACK_ATTRIBUTE = "rack";
  private static final String HOST_ATTRIBUTE = "host";

  private static final IJobKey JOB_A = JobKeys.from("roleA", "env", "jobA");
  private static final IJobKey JOB_B = JobKeys.from("roleB", "env", "jobB");

  private static final int DEFAULT_CPUS = 4;
  private static final long DEFAULT_RAM = 1000;
  private static final long DEFAULT_DISK = 2000;
  private static final ResourceBag DEFAULT_OFFER = bagFromMesosResources(ImmutableSet.of(
      mesosScalar(CPUS, DEFAULT_CPUS),
      mesosScalar(RAM_MB, DEFAULT_RAM),
      mesosScalar(DISK_MB, DEFAULT_DISK),
      mesosRange(PORTS, 80, 81)));

  private static final Amount<Long, Time> UNAVAILABILITY_THRESHOLD = Amount.of(2L, Time.MINUTES);
  private final FakeClock clock = new FakeClock();
  private SchedulingFilter defaultFilter;

  @Before
  public void setUp() {
    defaultFilter = new SchedulingFilterImpl(UNAVAILABILITY_THRESHOLD, clock);
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

    ITaskConfig noPortTask = resetPorts(
        makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK),
        ImmutableSet.of());
    ITaskConfig onePortTask = resetPorts(
        makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK),
        ImmutableSet.of("one"));
    ITaskConfig twoPortTask = resetPorts(
        makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK),
        ImmutableSet.of("one", "two"));
    ITaskConfig threePortTask = resetPorts(
        makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK),
        ImmutableSet.of("one", "two", "three"));

    Set<Veto> none = ImmutableSet.of();
    IHostAttributes hostA = hostAttributes(HOST_A, host(HOST_A), rack(RACK_A));
    assertEquals(
        none,
        defaultFilter.filter(
            new UnusedResource(DEFAULT_OFFER, hostA),
            TaskTestUtil.toResourceRequest(noPortTask)));
    assertEquals(
        none,
        defaultFilter.filter(
            new UnusedResource(DEFAULT_OFFER, hostA),
            TaskTestUtil.toResourceRequest(onePortTask)));
    assertEquals(
        none,
        defaultFilter.filter(
            new UnusedResource(DEFAULT_OFFER, hostA),
            TaskTestUtil.toResourceRequest(twoPortTask)));
    assertEquals(
        ImmutableSet.of(veto(PORTS, 1)),
        defaultFilter.filter(
            new UnusedResource(DEFAULT_OFFER, hostA),
            TaskTestUtil.toResourceRequest(threePortTask)));
  }

  @Test
  public void testInsufficientResources() {
    control.replay();

    IHostAttributes hostA = hostAttributes(HOST_A, host(HOST_A), rack(RACK_A));
    assertVetoes(
        makeTask(DEFAULT_CPUS + 1, DEFAULT_RAM + 1, DEFAULT_DISK + 1),
        hostA,
        veto(CPUS, 1), veto(DISK_MB, 1), veto(RAM_MB, 1));
    assertVetoes(makeTask(DEFAULT_CPUS + 1, DEFAULT_RAM, DEFAULT_DISK), hostA, veto(CPUS, 1));
    assertVetoes(makeTask(DEFAULT_CPUS, DEFAULT_RAM + 1, DEFAULT_DISK), hostA, veto(RAM_MB, 1));
    assertVetoes(makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK + 1), hostA, veto(DISK_MB, 1));
  }

  @Test
  public void testDedicatedRole() {
    control.replay();

    IHostAttributes hostA = hostAttributes(HOST_A, dedicated(JOB_A.getRole()));
    checkConstraint(hostA, DEDICATED_ATTRIBUTE, true, JOB_A.getRole());
    assertVetoes(makeTask(JOB_B), hostA, Veto.dedicatedHostConstraintMismatch());
  }

  @Test
  public void testSharedDedicatedHost() {
    control.replay();

    String dedicated1 = dedicatedFor(JOB_A);
    String dedicated2 = dedicatedFor(JOB_B);
    IHostAttributes hostA = hostAttributes(HOST_A, dedicated(dedicated1, dedicated2));
    assertNoVetoes(
        checkConstraint(
            JOB_A,
            hostA,
            DEDICATED_ATTRIBUTE,
            true,
            dedicated1),
        hostA);
    assertNoVetoes(
        checkConstraint(
            JOB_B,
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
  public void testDrainingMesosMaintenance() {
    // Start the test at minute 8
    clock.advance(Amount.of(8L, Time.MINUTES));

    // The agent will go down at minute 9
    // this is less than the threshold of two minutes

    Instant start = Instant.ofEpochMilli(Amount.of(9L, Time.MINUTES).as(Time.MILLISECONDS));

    ITaskConfig task = makeTask();
    UnusedResource unusedResource = new UnusedResource(
        DEFAULT_OFFER,
        hostAttributes(HOST_A),
        Optional.of(start));

    control.replay();

    assertEquals(
        ImmutableSet.of(Veto.maintenance("draining")),
        defaultFilter.filter(unusedResource, TaskTestUtil.toResourceRequest(task)));
  }

  @Test
  public void testNotVetoingWithMesosMaintenace() {
    // Start the test at minute 8
    clock.advance(Amount.of(8L, Time.MINUTES));

    // The agent will go down at minute 100
    // this is greater than the threshold of two minutes

    Instant start = Instant.ofEpochMilli(Amount.of(100L, Time.MINUTES).as(Time.MILLISECONDS));

    ITaskConfig task = makeTask();
    UnusedResource unusedResource = new UnusedResource(
        DEFAULT_OFFER,
        hostAttributes(HOST_A),
        Optional.of(start));

    control.replay();

    assertEquals(
        ImmutableSet.of(),
        defaultFilter.filter(unusedResource, TaskTestUtil.toResourceRequest(task)));
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
        makeTask(JOB_A, constraint1, constraint2),
        hostAttributes(HOST_A, dedicated(HOST_A), host(HOST_A)),
        Veto.constraintMismatch(DEDICATED_ATTRIBUTE));
    assertNoVetoes(
        makeTask(JOB_B, constraint1, constraint2),
        hostAttributes(HOST_B, dedicated("xxx"), host(HOST_A)));
  }

  @Test
  public void testDedicatedMismatchShortCircuits() {
    // Ensures that a dedicated mismatch short-circuits other filter operations, such as
    // evaluation of limit constraints.  Reduction of task queries is the desired outcome.

    control.replay();

    Constraint hostLimit = limitConstraint("host", 1);
    assertVetoes(
        makeTask(JOB_A, hostLimit, makeConstraint(DEDICATED_ATTRIBUTE, "xxx")),
        hostAttributes(HOST_A, host(HOST_A)),
        Veto.constraintMismatch(DEDICATED_ATTRIBUTE));
    assertVetoes(
        makeTask(JOB_A, hostLimit, makeConstraint(DEDICATED_ATTRIBUTE, "xxx")),
        hostAttributes(HOST_B, dedicated(dedicatedFor(JOB_B)), host(HOST_B)),
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

  private String dedicatedFor(IJobKey job) {
    return job.getRole() + "/" + job.getName();
  }

  @Test
  public void testLimitWithinJob() {
    control.replay();

    AttributeAggregate stateA = AttributeAggregate.create(
        Suppliers.ofInstance(ImmutableList.of(
            host(HOST_A),
            rack(RACK_A),
            host(HOST_B),
            rack(RACK_A),
            host(HOST_B),
            rack(RACK_A),
            host(HOST_C),
            rack(RACK_B))));
    AttributeAggregate stateB = AttributeAggregate.create(
        Suppliers.ofInstance(ImmutableList.of(
            host(HOST_A),
            rack(RACK_A),
            host(HOST_A),
            rack(RACK_A),
            host(HOST_B),
            rack(RACK_A))));

    IHostAttributes hostA = hostAttributes(HOST_A, host(HOST_A), rack(RACK_A));
    IHostAttributes hostB = hostAttributes(HOST_B, host(HOST_B), rack(RACK_A));
    IHostAttributes hostC = hostAttributes(HOST_C, host(HOST_C), rack(RACK_B));
    assertNoVetoes(hostLimitTask(JOB_A, 2), hostA, stateA);
    assertVetoes(
        hostLimitTask(JOB_A, 1),
        hostB,
        stateA,
        Veto.unsatisfiedLimit(HOST_ATTRIBUTE));
    assertVetoes(
        hostLimitTask(JOB_A, 2),
        hostB,
        stateA,
        Veto.unsatisfiedLimit(HOST_ATTRIBUTE));
    assertNoVetoes(hostLimitTask(JOB_A, 3), hostB, stateA);

    assertVetoes(
        rackLimitTask(JOB_A, 2),
        hostB,
        stateB,
        Veto.unsatisfiedLimit(RACK_ATTRIBUTE));
    assertVetoes(
        rackLimitTask(JOB_A, 3),
        hostB,
        stateB,
        Veto.unsatisfiedLimit(RACK_ATTRIBUTE));
    assertNoVetoes(rackLimitTask(JOB_A, 4), hostB, stateB);

    assertNoVetoes(rackLimitTask(JOB_A, 1), hostC, stateB);

    assertVetoes(
        rackLimitTask(JOB_A, 1),
        hostC,
        stateA,
        Veto.unsatisfiedLimit(RACK_ATTRIBUTE));
    assertNoVetoes(rackLimitTask(JOB_A, 2), hostC, stateB);
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

    ITaskConfig task = makeTask(JOB_A, jvmConstraint, zoneConstraint);
    assertEquals(
        ImmutableSet.of(),
        defaultFilter.filter(
            new UnusedResource(DEFAULT_OFFER, hostA),
            TaskTestUtil.toResourceRequest(task)));

    Constraint jvmNegated = jvmConstraint.deepCopy();
    jvmNegated.getConstraint().getValue().setNegated(true);
    Constraint zoneNegated = jvmConstraint.deepCopy();
    zoneNegated.getConstraint().getValue().setNegated(true);
    assertVetoes(
        makeTask(JOB_A, jvmNegated, zoneNegated),
        hostA,
        Veto.constraintMismatch("jvm"));
  }

  @Test
  public void testVetoScaling() {
    control.replay();

    int maxScore = VetoType.INSUFFICIENT_RESOURCES.getScore();
    assertEquals((int) (maxScore * 1.0 / CPUS.getScalingRange()), veto(CPUS, 1).getScore());
    assertEquals(maxScore, veto(CPUS, CPUS.getScalingRange() * 10).getScore());
    assertEquals((int) (maxScore * 2.0 / RAM_MB.getScalingRange()), veto(RAM_MB, 2).getScore());
    assertEquals(
        (int) (maxScore * 200.0 / DISK_MB.getScalingRange()),
        veto(DISK_MB, 200).getScore());
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

    assertEquals(VetoGroup.EMPTY, Veto.identifyGroup(ImmutableSet.of()));

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

  private static Veto veto(ResourceType resourceType, int excess) {
    return Veto.insufficientResources(
        resourceType.getAuroraName(),
        SchedulingFilterImpl.scale(excess, resourceType.getScalingRange()));
  }

  private ITaskConfig checkConstraint(
      IHostAttributes hostAttributes,
      String constraintName,
      boolean expected,
      String value,
      String... vs) {

    return checkConstraint(JOB_A, hostAttributes, constraintName, expected, value, vs);
  }

  private ITaskConfig checkConstraint(
      IJobKey job,
      IHostAttributes hostAttributes,
      String constraintName,
      boolean expected,
      String value,
      String... vs) {

    return checkConstraint(
        job,
        empty(),
        hostAttributes,
        constraintName,
        expected,
        new ValueConstraint(false,
            ImmutableSet.<String>builder().add(value).addAll(Arrays.asList(vs)).build()));
  }

  private ITaskConfig checkConstraint(
      IJobKey job,
      AttributeAggregate aggregate,
      IHostAttributes hostAttributes,
      String constraintName,
      boolean expected,
      ValueConstraint value) {

    Constraint constraint = new Constraint(constraintName, TaskConstraint.value(value));
    ITaskConfig task = makeTask(job, constraint);
    assertEquals(
        expected,
        defaultFilter.filter(
            new UnusedResource(DEFAULT_OFFER, hostAttributes),
            TaskTestUtil.toResourceRequest(task))
            .isEmpty());

    Constraint negated = constraint.deepCopy();
    negated.getConstraint().getValue().setNegated(!value.isNegated());
    ITaskConfig negatedTask = makeTask(job, negated);
    assertEquals(
        !expected,
        defaultFilter.filter(
            new UnusedResource(DEFAULT_OFFER, hostAttributes),
            ResourceRequest.fromTask(negatedTask, NO_OVERHEAD_EXECUTOR, aggregate, TIER_MANAGER))
            .isEmpty());
    return task;
  }

  private void assertNoVetoes(ITaskConfig task, IHostAttributes hostAttributes) {
    assertVetoes(task, hostAttributes, empty());
  }

  private void assertNoVetoes(
      ITaskConfig task,
      IHostAttributes attributes,
      AttributeAggregate jobState) {

    assertVetoes(task, attributes, jobState);
  }

  private void assertVetoes(ITaskConfig task, IHostAttributes hostAttributes, Veto... vetoes) {
    assertVetoes(task, hostAttributes, empty(), vetoes);
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
            ResourceRequest.fromTask(task, NO_OVERHEAD_EXECUTOR, jobState, TIER_MANAGER)));
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

  private ITaskConfig makeTask(IJobKey job, Constraint... constraint) {
    return ITaskConfig.build(makeTask(job, DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK)
        .newBuilder()
        .setConstraints(Sets.newHashSet(constraint)));
  }

  private ITaskConfig hostLimitTask(IJobKey job, int maxPerHost) {
    return makeTask(job, limitConstraint(HOST_ATTRIBUTE, maxPerHost));
  }

  private ITaskConfig hostLimitTask(int maxPerHost) {
    return hostLimitTask(JOB_A, maxPerHost);
  }

  private ITaskConfig rackLimitTask(IJobKey job, int maxPerRack) {
    return makeTask(job, limitConstraint(RACK_ATTRIBUTE, maxPerRack));
  }

  private ITaskConfig makeTask(IJobKey job, int cpus, long ramMb, long diskMb) {
    return ITaskConfig.build(new TaskConfig()
        .setJob(job.newBuilder())
        .setResources(ImmutableSet.of(numCpus(cpus), ramMb(ramMb), diskMb(diskMb)))
        .setExecutorConfig(new ExecutorConfig(apiConstants.AURORA_EXECUTOR_NAME, "config")));
  }

  private ITaskConfig makeTask(int cpus, long ramMb, long diskMb) {
    return makeTask(JOB_A, cpus, ramMb, diskMb);
  }

  private ITaskConfig makeTask() {
    return makeTask(DEFAULT_CPUS, DEFAULT_RAM, DEFAULT_DISK);
  }
}
