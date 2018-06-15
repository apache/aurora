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
package org.apache.aurora.scheduler.sla;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;
import javax.inject.Qualifier;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Striped;
import com.google.gson.Gson;
import com.google.inject.Inject;

import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.entities.ICoordinatorSlaPolicy;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.storage.entities.ISlaPolicy;
import org.apache.aurora.scheduler.storage.entities.ITaskEvent;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Param;
import org.asynchttpclient.Response;
import org.asynchttpclient.util.HttpConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

/**
 * Provides methods for performing SLA-safe work. It is used for maintenance and job update
 * operations to guarantee that a job's SLA requirements are always satisfied.
 */
public class SlaManager extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(SlaManager.class);

  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface SlaManagerExecutor { }

  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface MaxParallelCoordinators { }

  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface HttpClient { }

  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface MinRequiredInstances { }

  @VisibleForTesting
  static final String TASK_PARAM = "task";

  private static final String ATTEMPTS_STAT_NAME = "sla_coordinator_attempts";
  private static final String SUCCESS_STAT_NAME = "sla_coordinator_success";
  private static final String ERRORS_STAT_NAME = "sla_coordinator_errors";
  private static final String USER_ERRORS_STAT_NAME = "sla_coordinator_user_errors";
  private static final String LOCK_STARVATION_STAT_NAME = "sla_coordinator_lock_starvation";

  private final ScheduledExecutorService executor;
  private final Storage storage;
  private final IServerInfo serverInfo;
  private final AsyncHttpClient httpClient;
  private final Striped<Lock> lock;
  private final int minRequiredInstances;
  private final TierManager tierManager;

  private final AtomicLong attemptsCounter;
  private final AtomicLong successCounter;
  private final AtomicLong errorsCounter;
  private final AtomicLong userErrorsCounter;
  private final AtomicLong lockStarvationCounter;
  private final LoadingCache<String, AtomicLong> errorsByTaskCounter;
  private final LoadingCache<String, AtomicLong> userErrorsByTaskCounter;
  private final LoadingCache<String, AtomicLong> lockStarvationByTaskCounter;

  @Inject
  SlaManager(@SlaManagerExecutor ScheduledExecutorService executor,
             @MaxParallelCoordinators Integer maxCoordinatorLocks,
             @MinRequiredInstances Integer minRequiredInstances,
             Storage storage,
             IServerInfo serverInfo,
             @HttpClient AsyncHttpClient httpClient,
             TierManager tierManager,
             StatsProvider statsProvider) {

    this.executor = requireNonNull(executor);
    this.storage = requireNonNull(storage);
    this.serverInfo = requireNonNull(serverInfo);
    this.httpClient = requireNonNull(httpClient);
    this.tierManager = requireNonNull(tierManager);
    this.minRequiredInstances = requireNonNull(minRequiredInstances);
    this.attemptsCounter = statsProvider.makeCounter(ATTEMPTS_STAT_NAME);
    this.successCounter = statsProvider.makeCounter(SUCCESS_STAT_NAME);
    this.errorsCounter = statsProvider.makeCounter(ERRORS_STAT_NAME);
    this.userErrorsCounter = statsProvider.makeCounter(USER_ERRORS_STAT_NAME);
    this.lockStarvationCounter = statsProvider.makeCounter(LOCK_STARVATION_STAT_NAME);
    this.lock = Striped.lazyWeakLock(requireNonNull(maxCoordinatorLocks));
    this.errorsByTaskCounter = CacheBuilder.newBuilder().build(
        new CacheLoader<String, AtomicLong>() {
          @Override
          public AtomicLong load(String key) {
            return statsProvider.makeCounter(key);
          }
        }
    );
    this.userErrorsByTaskCounter = CacheBuilder.newBuilder().build(
        new CacheLoader<String, AtomicLong>() {
          @Override
          public AtomicLong load(String key) {
            return statsProvider.makeCounter(key);
          }
        }
    );
    this.lockStarvationByTaskCounter = CacheBuilder.newBuilder().build(
        new CacheLoader<String, AtomicLong>() {
          @Override
          public AtomicLong load(String key) {
            return statsProvider.makeCounter(key);
          }
        }
    );
  }

  private long getSlaDuration(ISlaPolicy slaPolicy) {
    if (slaPolicy.isSetPercentageSlaPolicy()) {
      return slaPolicy.getPercentageSlaPolicy().getDurationSecs();
    } else if (slaPolicy.isSetCountSlaPolicy()) {
      return slaPolicy.getCountSlaPolicy().getDurationSecs();
    }

    throw new IllegalArgumentException("Expected a percentage/count sla policy.");
  }

  private boolean meetsSLAInstances(ISlaPolicy slaPolicy, long running, long total) {
    if (slaPolicy.isSetPercentageSlaPolicy()) {
      double percentageRunning = ((double) running / total) * 100.0;
      double percentageRequired = slaPolicy.getPercentageSlaPolicy().getPercentage();
      long numMaintenance = (long) ((1 - percentageRequired / 100.0) * total);
      if (numMaintenance < 1) {
        double updated = ((total - 1) / (double) total) * 100;
        LOG.warn("Invalid PercentageSlaPolicy(percentage={}) for task with {} instances "
                + "that allows {} instances to be in maintenance."
                + "Using percentage={} which allows 1 instance to be under maintenance.",
            percentageRequired, total, numMaintenance, updated);
        percentageRequired = updated;
      }
      return percentageRunning >= percentageRequired;
    } else if (slaPolicy.isSetCountSlaPolicy()) {
      long numRequired = slaPolicy.getCountSlaPolicy().getCount();
      long numMaintenance = total - numRequired;
      if (numMaintenance < 1) {
        long updated = total - 1;
        LOG.warn("Invalid CountSlaPolicy(count={}) for task with {} instances "
                + "that allows {} instances to be in maintenance."
                + "Using count={} which allows 1 instance to be under maintenance.",
            numRequired, total, numMaintenance, updated);
        numRequired = updated;
      }
      return running >= numRequired;
    }

    throw new IllegalArgumentException("Expected a percentage/count sla policy.");
  }

  private boolean meetsSLADuration(IScheduledTask task, Amount<Long, Time> slaDuration) {
    return task.getTaskEvents()
        .stream()
        .filter(te -> te.getStatus() == ScheduleStatus.RUNNING)
        .map(ITaskEvent::getTimestamp)
        .max(Long::compare)
        .map(t -> System.currentTimeMillis() - t > slaDuration.as(Time.MILLISECONDS))
        .orElse(false);
  }

  private boolean checkSla(IScheduledTask task, ISlaPolicy slaPolicy, StoreProvider store) {
    // Find the number of active tasks for the job, it will be used as the set of tasks
    // against which the percentage/count based SLA will be calculated against.
    final long numActive = store.getTaskStore().fetchTasks(
        Query.jobScoped(task.getAssignedTask().getTask().getJob()).active()
    ).size();

    if (skipSla(task, numActive)) {
      LOG.info("Skip SLA for {} because it is not production or does not have enough instances.",
          Tasks.id(task));
      return true;
    }

    // Find tasks which have been RUNNING for the required SLA duration.
    Amount<Long, Time> slaDuration = new TimeAmount(getSlaDuration(slaPolicy), Time.SECONDS);
    final Set<IScheduledTask> running = store.getTaskStore().fetchTasks(
        Query.jobScoped(task.getAssignedTask().getTask().getJob())
            .byStatus(ScheduleStatus.RUNNING))
        .stream()
        .filter(t -> !Tasks.id(t).equals(Tasks.id(task))) // exclude the task to be removed
        .filter(t -> meetsSLADuration(t, slaDuration)) // task is running for sla duration
        .collect(Collectors.toSet());

    // Check it we satisfy the number of RUNNING tasks per duration time.
    boolean meetsSla = meetsSLAInstances(slaPolicy, running.size(), numActive);

    LOG.info("SlaCheck: {}, {} tasks unaffected after updating state for {}.",
        meetsSla,
        running.size(),
        Tasks.id(task));

    return meetsSla;
  }

  /**
   * Performs the supplied {@link Storage.MutateWork} after checking with the configured
   * coordinator endpoint to make sure it is safe to perform the work.
   *
   * NOTE: Both the SLA check and the {@link Storage.MutateWork} will be performed within a
   * {@link Lock} that is indexed by the coordinator url. We do this to make sure that mutations
   * to the SLA are performed atomically, so the Coordinator does not have to track
   * concurrent requests and simulate SLA changes.
   *
   * @param task Task whose SLA is to checked.
   * @param slaPolicy {@link ICoordinatorSlaPolicy} to use for checking SLA.
   * @param work {@link Storage.MutateWork} to perform, if SLA is satisfied.
   * @param <T> The type of result the {@link Storage.MutateWork} produces.
   * @param <E> The type of exception the {@link Storage.MutateWork} throw.
   */
  private <T, E extends Exception> void askCoordinatorThenAct(
      IScheduledTask task,
      ICoordinatorSlaPolicy slaPolicy,
      Storage.MutateWork<T, E> work) {

    String taskKey = getTaskKey(task);

    LOG.debug("Awaiting lock on coordinator: {} for task: {}",
        slaPolicy.getCoordinatorUrl(),
        taskKey);
    Lock l = lock.get(slaPolicy.getCoordinatorUrl());
    if (l.tryLock()) {
      try {
        LOG.info("Acquired lock on coordinator: {} for task: {}",
            slaPolicy.getCoordinatorUrl(),
            taskKey);
        attemptsCounter.incrementAndGet();

        if (coordinatorAllows(task, taskKey, slaPolicy)) {
          LOG.info("Performing work after coordinator: {} approval for task: {}",
              slaPolicy.getCoordinatorUrl(),
              taskKey);
          storage.write(work);
        }
      } catch (RuntimeException e) {
        LOG.error("Unexpected failure during coordinator sla check against: {} for task: {}",
            slaPolicy.getCoordinatorUrl(),
            taskKey,
            e);
        throw e;
      } catch (Exception e) {
        incrementErrorCount(ERRORS_STAT_NAME, taskKey);
        LOG.error("Failed to talk to coordinator: {} for task: {}",
            slaPolicy.getCoordinatorUrl(),
            taskKey,
            e);
      } finally {
        LOG.info("Releasing lock for coordinator: {} and task: {}",
            slaPolicy.getCoordinatorUrl(),
            taskKey);
        l.unlock();
      }
    } else {
      incrementErrorCount(LOCK_STARVATION_STAT_NAME, taskKey);
      LOG.info("Failed to acquire lock on coordinator: {} for task: {}",
          slaPolicy.getCoordinatorUrl(),
          taskKey);
    }
  }

  private boolean coordinatorAllows(
      IScheduledTask task,
      String taskKey,
      ICoordinatorSlaPolicy slaPolicy)
      throws InterruptedException, ExecutionException, TException {

    LOG.info("Checking coordinator: {} for task: {}", slaPolicy.getCoordinatorUrl(), taskKey);

    Response response = httpClient.preparePost(slaPolicy.getCoordinatorUrl())
        .setQueryParams(ImmutableList.of(new Param(TASK_PARAM, taskKey)))
        .setBody(new TSerializer(new TSimpleJSONProtocol.Factory()).toString(task.newBuilder()))
        .execute()
        .get();

    if (response.getStatusCode() != HttpConstants.ResponseStatusCodes.OK_200) {
      LOG.error("Request failed to coordinator: {} for task: {}. Response: {}",
          slaPolicy.getCoordinatorUrl(),
          taskKey,
          response.getStatusCode());
      incrementErrorCount(USER_ERRORS_STAT_NAME, taskKey);
      return false;
    }

    successCounter.incrementAndGet();
    String json = response.getResponseBody();
    LOG.info("Got response: {} from {} for task: {}",
        json,
        slaPolicy.getCoordinatorUrl(),
        taskKey);

    Map<String, Boolean> result = new Gson().fromJson(
        json,
        new TypeReference<Map<String, Boolean>>() { }.getType());

    return result.get(slaPolicy.isSetStatusKey() ? slaPolicy.getStatusKey() : "drain");
  }

  @VisibleForTesting
  String getTaskKey(IScheduledTask task) {
    IJobKey jobKey = Tasks.getJob(task);
    int instanceId = Tasks.getInstanceId(task);
    return String.format("%s/%s/%s/%s/%s", serverInfo.getClusterName(),
        jobKey.getRole(), jobKey.getEnvironment(), jobKey.getName(), instanceId);
  }

  private void incrementErrorCount(String prefix, String taskKey) {
    try {
      if (prefix.equals(ERRORS_STAT_NAME)) {
        errorsCounter.incrementAndGet();
        errorsByTaskCounter.get(prefix + "_" + taskKey).incrementAndGet();
      }

      if (prefix.equals(USER_ERRORS_STAT_NAME)) {
        userErrorsCounter.incrementAndGet();
        userErrorsByTaskCounter.get(prefix + "_" + taskKey).incrementAndGet();
      }

      if (prefix.equals(LOCK_STARVATION_STAT_NAME)) {
        lockStarvationCounter.incrementAndGet();
        lockStarvationByTaskCounter.get(prefix + "_" + taskKey).incrementAndGet();
      }
    } catch (ExecutionException e) {
      LOG.error("Failed increment failure metrics for task: {}", taskKey, e);
    }
  }

  /**
   * Checks the SLA for the given task using the {@link ISlaPolicy} supplied.
   *
   * @param task Task whose SLA is to be checked.
   * @param slaPolicy {@link ISlaPolicy} to use.
   * @param work {@link Storage.MutateWork} to perform, if SLA is satisfied.
   * @param force boolean to indicate if work should be performed without checking SLA.
   * @param <T> The type of result the {@link Storage.MutateWork} produces.
   * @param <E> The type of exception the {@link Storage.MutateWork} throw.
   * @throws E raises exception of type E if the {@link Storage.MutateWork} throws.
   */
  @Timed
  public <T, E extends Exception> void checkSlaThenAct(
      IScheduledTask task,
      ISlaPolicy slaPolicy,
      Storage.MutateWork<T, E> work,
      boolean force) throws E {

    if (force) {
      LOG.info("Forcing work without applying SlaPolicy: {} for {}", slaPolicy, Tasks.id(task));
      storage.write(work);
      return;
    }

    LOG.info("Using SlaPolicy: {} for {}", slaPolicy, Tasks.id(task));

    // has custom coordinator sla policy
    if (slaPolicy.isSetCoordinatorSlaPolicy()) {
      // schedule work to perform coordinated transition
      executor.execute(() -> askCoordinatorThenAct(
          task,
          slaPolicy.getCoordinatorSlaPolicy(),
          work));
    } else {
      // verify sla and perform work if satisfied
      storage.write(store -> {
        if (checkSla(task, slaPolicy, store)) {
          work.apply(store);
        }
        return null; // TODO(sshanmugham) we need to satisfy the interface, refactor later
      });
    }
  }

  private boolean skipSla(IScheduledTask task, long numActive) {
    if (!tierManager.getTier(task.getAssignedTask().getTask()).isPreemptible()
        && !tierManager.getTier(task.getAssignedTask().getTask()).isRevocable()) {
      return numActive < minRequiredInstances;
    }
    return true;
  }

  @Override
  protected void startUp() {
    //no-op
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down SlaManager async http client.");
    httpClient.close();
  }
}
