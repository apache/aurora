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

package org.apache.aurora.scheduler.config;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import javax.security.auth.kerberos.KerberosPrincipal;

import com.beust.jcommander.Parameter;
import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.DockerParameter;
import org.apache.aurora.gen.Mode;
import org.apache.aurora.gen.Volume;
import org.apache.aurora.scheduler.app.MoreModules;
import org.apache.aurora.scheduler.app.SchedulerMain.Options.DriverKind;
import org.apache.aurora.scheduler.config.types.DataAmount;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.http.api.security.HttpSecurityModule.Options.HttpAuthenticationMechanism;
import org.apache.aurora.scheduler.http.api.security.ShiroIniConverterTest;
import org.apache.aurora.scheduler.offers.OfferOrder;
import org.apache.aurora.scheduler.sla.MetricCalculator.MetricCategory;
import org.apache.shiro.authc.credential.AllowAllCredentialsMatcher;
import org.apache.shiro.config.Ini;
import org.apache.shiro.config.Ini.Section;
import org.apache.shiro.web.filter.authc.AnonymousFilter;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.http.api.security.ShiroIniConverterTest.EXAMPLE_RESOURCE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CommandLineTest {
  @Before
  public void setUp() {
    CommandLine.initializeForTest();
  }

  @Test
  public void testCustomOptions() {
    CommandLine.clearForTest();

    // Ensures that a custom module can register command line arguments.

    CliOptions options = CommandLine.parseOptions(
        "-task_assigner_modules=org.apache.aurora.scheduler.config.CustomModule",
        "-cluster_name=test",
        "-mesos_master_address=localhost:8080",
        "-backup_dir=/dev/null",
        "-serverset_path=/tmp",
        "-zk_endpoints=localhost:2181",
        "-custom_flag=customValue");

    Injector injector = Guice.createInjector(
        MoreModules.instantiateAll(options.state.taskAssignerModules, options));

    assertEquals("customValue", injector.getInstance(CustomModule.BINDING_KEY));
  }

  public static class NoopModule extends AbstractModule {
    @Override
    protected void configure() {
      // No-op.
    }
  }

  private static final TimeAmount TEST_TIME = new TimeAmount(42, Time.DAYS);
  private static final DataAmount TEST_DATA = new DataAmount(42, Data.GB);

  @Test
  public void testParseAllOptions() {
    File tempFile;
    try {
      tempFile = File.createTempFile(getClass().getCanonicalName(), null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    tempFile.deleteOnExit();

    CliOptions expected = new CliOptions();
    expected.reconciliation.transientTaskStateTimeout = TEST_TIME;
    expected.reconciliation.initialTaskKillRetryInterval = TEST_TIME;
    expected.reconciliation.reconciliationInitialDelay = TEST_TIME;
    expected.reconciliation.reconciliationExplicitInterval = TEST_TIME;
    expected.reconciliation.reconciliationImplicitInterval = TEST_TIME;
    expected.reconciliation.reconciliationScheduleSpread = TEST_TIME;
    expected.reconciliation.reconciliationBatchSize = 42;
    expected.reconciliation.reconciliationBatchInterval = TEST_TIME;
    expected.offer.holdOffersForever = true;
    expected.offer.minOfferHoldTime = TEST_TIME;
    expected.offer.offerHoldJitterWindow = TEST_TIME;
    expected.offer.offerStaticBanCacheMaxSize = 42L;
    expected.offer.offerFilterDuration = TEST_TIME;
    expected.offer.unavailabilityThreshold = TEST_TIME;
    expected.offer.offerOrder = ImmutableList.of(OfferOrder.CPU, OfferOrder.DISK);
    expected.offer.offerOrderModules = ImmutableList.of(NoopModule.class);
    expected.executor.customExecutorConfig = tempFile;
    expected.executor.thermosExecutorPath = "testing";
    expected.executor.thermosExecutorResources = ImmutableList.of("testing");
    expected.executor.thermosExecutorFlags = "testing";
    expected.executor.thermosHomeInSandbox = true;
    expected.executor.executorOverheadCpus = 42;
    expected.executor.executorOverheadRam = new DataAmount(42, Data.GB);
    expected.executor.globalContainerMounts =
        ImmutableList.of(new Volume("/container", "/host", Mode.RO));
    expected.executor.populateDiscoveryInfo = true;
    expected.app.maxTasksPerJob = 42;
    expected.app.maxUpdateInstanceFailures = 42;
    expected.app.allowedContainerTypes = ImmutableList.of(Container._Fields.DOCKER);
    expected.app.enableDockerParameters = true;
    expected.app.defaultDockerParameters = ImmutableList.of(new DockerParameter("a", "testing"));
    expected.app.requireDockerUseExecutor = false;
    expected.app.enableMesosFetcher = true;
    expected.app.allowContainerVolumes = true;
    expected.app.allowedJobEnvironments = "^(foo|bar|zaa)$";
    expected.main.clusterName = "testing";
    expected.main.serversetPath = "testing";
    expected.main.serversetEndpointName = "testing";
    expected.main.statsUrlPrefix = "testing";
    expected.main.allowGpuResource = true;
    expected.main.driverImpl = DriverKind.V0_DRIVER;
    expected.scheduling.maxScheduleAttemptsPerSec = 42;
    expected.scheduling.flappingThreshold = TEST_TIME;
    expected.scheduling.initialFlappingDelay = TEST_TIME;
    expected.scheduling.maxFlappingDelay = TEST_TIME;
    expected.scheduling.maxReschedulingDelay = new TimeAmount(42, Time.DAYS);
    expected.scheduling.firstScheduleDelay = TEST_TIME;
    expected.scheduling.initialSchedulePenalty = TEST_TIME;
    expected.scheduling.maxSchedulePenalty = TEST_TIME;
    expected.scheduling.reservationDuration = TEST_TIME;
    expected.scheduling.schedulingMaxBatchSize = 42;
    expected.scheduling.maxTasksPerScheduleAttempt = 42;
    expected.async.asyncWorkerThreads = 42;
    expected.zk.inProcess = true;
    expected.zk.zkEndpoints = ImmutableList.of(InetSocketAddress.createUnresolved("testing", 42));
    expected.zk.chrootPath = "testing";
    expected.zk.sessionTimeout = TEST_TIME;
    expected.zk.connectionTimeout = TEST_TIME;
    expected.zk.digestCredentials = "testing";
    expected.updater.enableAffinity = true;
    expected.updater.affinityExpiration = TEST_TIME;
    expected.state.taskAssignerModules = ImmutableList.of(NoopModule.class);
    expected.logStorage.shutdownGracePeriod = TEST_TIME;
    expected.logStorage.snapshotInterval = TEST_TIME;
    expected.logStorage.maxLogEntrySize = TEST_DATA;
    expected.backup.backupInterval = TEST_TIME;
    expected.backup.maxSavedBackups = 42;
    expected.backup.backupDir = new File("testing");
    expected.aop.methodInterceptorModules = ImmutableList.of(NoopModule.class);
    expected.pruning.historyPruneThreshold = TEST_TIME;
    expected.pruning.historyMaxPerJobThreshold = 42;
    expected.pruning.historyMinRetentionThreshold = TEST_TIME;
    expected.pruning.jobUpdateHistoryPerJobThreshold = 42;
    expected.pruning.jobUpdateHistoryPruningInterval = TEST_TIME;
    expected.pruning.jobUpdateHistoryPruningThreshold = TEST_TIME;
    expected.driver.mesosMasterAddress = "testing";
    expected.driver.frameworkAuthenticationFile = new File("testing");
    expected.driver.frameworkFailoverTimeout = TEST_TIME;
    expected.driver.frameworkAnnouncePrincipal = true;
    expected.driver.frameworkName = "testing";
    expected.driver.executorUser = "testing";
    expected.driver.receiveRevocableResources = true;
    expected.driver.mesosRole = "testing";
    expected.jetty.hostnameOverride = "testing";
    expected.jetty.httpPort = 42;
    expected.jetty.listenIp = "testing";
    expected.httpSecurity.shiroRealmModule = ImmutableList.of(NoopModule.class);
    expected.httpSecurity.shiroAfterAuthFilter = AnonymousFilter.class;
    expected.httpSecurity.httpAuthenticationMechanism = HttpAuthenticationMechanism.NEGOTIATE;
    expected.kerberos.serverKeytab = new File("testing");
    expected.kerberos.serverPrincipal =
        new KerberosPrincipal("HTTP/aurora.example.com@EXAMPLE.COM");
    expected.kerberos.kerberosDebug = true;

    Ini testIni = new Ini();
    Section users = testIni.addSection("users");
    users.putAll(ImmutableMap.of(
        "root", "secret, admin",
        "wfarner", "password, eng",
        "ksweeney", "12345"));
    users.put("root", "secret, admin");
    Section roles = testIni.addSection("roles");
    roles.putAll(ImmutableMap.of(
        "admin", "*",
        "eng", "thrift.AuroraSchedulerManager:*"));

    expected.iniShiroRealm.shiroIniPath = testIni;
    expected.iniShiroRealm.shiroCredentialsMatcher = AllowAllCredentialsMatcher.class;
    expected.api.enableCorsFor = "testing";
    expected.preemptor.enablePreemptor = false;
    expected.preemptor.preemptionDelay = TEST_TIME;
    expected.preemptor.preemptionSlotHoldTime = TEST_TIME;
    expected.preemptor.preemptionSlotSearchInterval = TEST_TIME;
    expected.preemptor.reservationMaxBatchSize = 42;
    expected.preemptor.slotFinderModules = ImmutableList.of(NoopModule.class);
    expected.mesosLog.quorumSize = 42;
    expected.mesosLog.logPath = new File("testing");
    expected.mesosLog.zkLogGroupPath = "testing";
    expected.mesosLog.coordinatorElectionTimeout = TEST_TIME;
    expected.mesosLog.coordinatorElectionRetries = 42;
    expected.mesosLog.readTimeout = TEST_TIME;
    expected.mesosLog.writeTimeout = TEST_TIME;
    expected.sla.slaRefreshInterval = TEST_TIME;
    expected.sla.slaProdMetrics = ImmutableList.of(MetricCategory.JOB_UPTIMES);
    expected.sla.slaNonProdMetrics = ImmutableList.of(MetricCategory.JOB_UPTIMES);
    expected.webhook.webhookConfigFile = tempFile;
    expected.scheduler.maxRegistrationDelay = TEST_TIME;
    expected.scheduler.maxLeadingDuration = TEST_TIME;
    expected.scheduler.maxStatusUpdateBatchSize = 42;
    expected.scheduler.maxTaskEventBatchSize = 42;
    expected.tiers.tierConfigFile = tempFile;
    expected.asyncStats.taskStatInterval = TEST_TIME;
    expected.asyncStats.slotStatInterval = TEST_TIME;
    expected.stats.samplingInterval = TEST_TIME;
    expected.stats.retentionPeriod = TEST_TIME;
    expected.cron.cronSchedulerNumThreads = 42;
    expected.cron.cronTimezone = "testing";
    expected.cron.cronStartInitialBackoff = TEST_TIME;
    expected.cron.cronStartMaxBackoff = TEST_TIME;
    expected.cron.cronMaxBatchSize = 42;
    expected.resourceSettings.enableRevocableCpus = false;
    expected.resourceSettings.enableRevocableRam = true;

    assertAllNonDefaultParameters(expected);

    CliOptions parsed = CommandLine.parseOptions(
        "-transient_task_state_timeout=42days",
        "-initial_task_kill_retry_interval=42days",
        "-reconciliation_initial_delay=42days",
        "-reconciliation_explicit_interval=42days",
        "-reconciliation_implicit_interval=42days",
        "-reconciliation_schedule_spread=42days",
        "-reconciliation_explicit_batch_size=42",
        "-reconciliation_explicit_batch_interval=42days",
        "-hold_offers_forever=true",
        "-min_offer_hold_time=42days",
        "-offer_hold_jitter_window=42days",
        "-offer_static_ban_cache_max_size=42",
        "-offer_filter_duration=42days",
        "-unavailability_threshold=42days",
        "-offer_order=CPU,DISK",
        "-offer_order_modules=org.apache.aurora.scheduler.config.CommandLineTest$NoopModule",
        "-custom_executor_config=" + tempFile.getAbsolutePath(),
        "-thermos_executor_path=testing",
        "-thermos_executor_resources=testing",
        "-thermos_executor_flags=testing",
        "-thermos_home_in_sandbox=true",
        "-thermos_executor_cpu=42",
        "-thermos_executor_ram=42GB",
        "-global_container_mounts=/host:/container:ro",
        "-populate_discovery_info=true",
        "-max_tasks_per_job=42",
        "-max_update_instance_failures=42",
        "-allowed_container_types=DOCKER",
        "-allow_docker_parameters=true",
        "-default_docker_parameters=a=testing",
        "-require_docker_use_executor=false",
        "-enable_mesos_fetcher=true",
        "-allow_container_volumes=true",
        "-allowed_job_environments=^(foo|bar|zaa)$",
        "-cluster_name=testing",
        "-serverset_path=testing",
        "-serverset_endpoint_name=testing",
        "-viz_job_url_prefix=testing",
        "-allow_gpu_resource=true",
        "-mesos_driver=V0_DRIVER",
        "-max_schedule_attempts_per_sec=42",
        "-flapping_task_threshold=42days",
        "-initial_flapping_task_delay=42days",
        "-max_flapping_task_delay=42days",
        "-max_reschedule_task_delay_on_startup=42days",
        "-first_schedule_delay=42days",
        "-initial_schedule_penalty=42days",
        "-max_schedule_penalty=42days",
        "-offer_reservation_duration=42days",
        "-scheduling_max_batch_size=42",
        "-max_tasks_per_schedule_attempt=42",
        "-async_worker_threads=42",
        "-zk_in_proc=true",
        "-zk_endpoints=testing:42",
        "-zk_chroot_path=testing",
        "-zk_session_timeout=42days",
        "-zk_connection_timeout=42days",
        "-zk_digest_credentials=testing",
        "-enable_update_affinity=true",
        "-update_affinity_reservation_hold_time=42days",
        "-task_assigner_modules=org.apache.aurora.scheduler.config.CommandLineTest$NoopModule",
        "-dlog_shutdown_grace_period=42days",
        "-dlog_snapshot_interval=42days",
        "-dlog_max_entry_size=42GB",
        "-backup_interval=42days",
        "-max_saved_backups=42",
        "-backup_dir=testing",
        "-thrift_method_interceptor_modules="
            + "org.apache.aurora.scheduler.config.CommandLineTest$NoopModule",
        "-history_prune_threshold=42days",
        "-history_max_per_job_threshold=42",
        "-history_min_retention_threshold=42days",
        "-job_update_history_per_job_threshold=42",
        "-job_update_history_pruning_interval=42days",
        "-job_update_history_pruning_threshold=42days",
        "-mesos_master_address=testing",
        "-framework_authentication_file=testing",
        "-framework_failover_timeout=42days",
        "-framework_announce_principal=true",
        "-framework_name=testing",
        "-executor_user=testing",
        "-receive_revocable_resources=true",
        "-mesos_role=testing",
        "-hostname=testing",
        "-http_port=42",
        "-ip=testing",
        "-shiro_realm_modules=org.apache.aurora.scheduler.config.CommandLineTest$NoopModule",
        "-shiro_after_auth_filter=org.apache.shiro.web.filter.authc.AnonymousFilter",
        "-http_authentication_mechanism=NEGOTIATE",
        "-kerberos_server_keytab=testing",
        "-kerberos_server_principal=HTTP/aurora.example.com@EXAMPLE.COM",
        "-kerberos_debug=true",
        "-shiro_ini_path=" + ShiroIniConverterTest.class.getResource(EXAMPLE_RESOURCE).toString(),
        "-shiro_credentials_matcher="
            + "org.apache.shiro.authc.credential.AllowAllCredentialsMatcher",
        "-enable_cors_for=testing",
        "-enable_preemptor=false",
        "-preemption_delay=42days",
        "-preemption_slot_hold_time=42days",
        "-preemption_slot_search_interval=42days",
        "-preemption_reservation_max_batch_size=42",
        "-preemption_slot_finder_modules="
            + "org.apache.aurora.scheduler.config.CommandLineTest$NoopModule",
        "-native_log_quorum_size=42",
        "-native_log_file_path=testing",
        "-native_log_zk_group_path=testing",
        "-native_log_election_timeout=42days",
        "-native_log_election_retries=42",
        "-native_log_read_timeout=42days",
        "-native_log_write_timeout=42days",
        "-sla_stat_refresh_interval=42days",
        "-sla_prod_metrics=JOB_UPTIMES",
        "-sla_non_prod_metrics=JOB_UPTIMES",
        "-webhook_config=" + tempFile.getAbsolutePath(),
        "-max_registration_delay=42days",
        "-max_leading_duration=42days",
        "-max_status_update_batch_size=42",
        "-max_task_event_batch_size=42",
        "-tier_config=" + tempFile.getAbsolutePath(),
        "-async_task_stat_update_interval=42days",
        "-async_slot_stat_update_interval=42days",
        "-stat_sampling_interval=42days",
        "-stat_retention_period=42days",
        "-cron_scheduler_num_threads=42",
        "-cron_timezone=testing",
        "-cron_start_initial_backoff=42days",
        "-cron_start_max_backoff=42days",
        "-cron_scheduling_max_batch_size=42",
        "-enable_revocable_cpus=false",
        "-enable_revocable_ram=true"
    );
    assertEqualOptions(expected, parsed);
  }

  private static void assertEqualOptions(CliOptions expected, CliOptions actual) {
    List<Object> actualObjects = CommandLine.getOptionsObjects(actual);
    for (Object expectedOptionContainer : CommandLine.getOptionsObjects(expected)) {
      Iterable<Field> paramFields = FluentIterable
          .from(expectedOptionContainer.getClass().getDeclaredFields())
          .filter(f -> f.getAnnotation(Parameter.class) != null);
      Object actualOptionContainer = FluentIterable.from(actualObjects)
          .firstMatch(Predicates.instanceOf(expectedOptionContainer.getClass()))
          .get();
      for (Field field : paramFields) {
        Parameter declaration = field.getAnnotation(Parameter.class);
        try {
          assertEquals(String.format("Value for %s does not match", declaration.names()[0]),
              field.get(expectedOptionContainer),
              field.get(actualOptionContainer));
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private static void assertAllNonDefaultParameters(CliOptions testCase) {
    // This is a safety check for the test setup itself.  We want to ensure that all options are
    // set up to parse correctly (e.g. have types that the parsing system is set up to handle).
    // To do this, check that the expected parse result sets all parameter fields to non-default
    // values.
    List<Object> defaultContainers = CommandLine.getOptionsObjects(new CliOptions());

    List<String> errors = Lists.newArrayList();

    for (Object testCaseContainer : CommandLine.getOptionsObjects(testCase)) {
      Iterable<Field> paramFields = FluentIterable
          .from(testCaseContainer.getClass().getDeclaredFields())
          .filter(f -> f.getAnnotation(Parameter.class) != null);
      Object defaultObject = FluentIterable.from(defaultContainers)
          .firstMatch(Predicates.instanceOf(testCaseContainer.getClass()))
          .get();
      for (Field field : paramFields) {
        Parameter declaration = field.getAnnotation(Parameter.class);
        try {
          String desc =
              String.format("for field %s (option %s)", field.getName(), declaration.names()[0]);
          Object testCaseOptionValue = field.get(testCaseContainer);
          if (testCaseOptionValue == null) {
            errors.add(String.format("Test case value may not be null %s", desc));
            continue;
          }
          if (testCaseOptionValue instanceof Iterable) {
            Iterable<?> value = (Iterable<?>) testCaseOptionValue;
            if (Iterables.isEmpty(value)) {
              errors.add(String.format("Test case value may not be empty %s", desc));
              continue;
            }
          }
          if (testCaseOptionValue instanceof Map) {
            Map<?, ?> value = (Map<?, ?>) testCaseOptionValue;
            if (value.isEmpty()) {
              errors.add(String.format("Test case value may not be empty %s", desc));
              continue;
            }
          }
          if (testCaseOptionValue.equals(field.get(defaultObject))) {
            errors.add(String.format("Test case may not use default option value %s", desc));
          }
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }

    if (!errors.isEmpty()) {
      fail("Test case is incomplete:\n  " + Joiner.on("\n  ").join(errors));
    }
  }
}
