package com.twitter.mesos.scheduler.testing;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.twitter.common.application.AbstractApplication;
import com.twitter.common.application.AppLauncher;
import com.twitter.common.args.Arg;
import com.twitter.common.args.ArgFilters;
import com.twitter.common.args.CmdLine;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.mem.MemStorage;

/**
 * Benchmark to evaluate storage performance.
 */
public class StorageBenchmark extends AbstractApplication {

  private static final String QUERIED_ROLE_NAME = "QueriedRole";
  private static final String QUERIED_JOB_NAME = "QueriedJob";
  private static final String DORMANT_ROLE_NAME = "DormantRole";
  private static final String DORMANT_JOB_NAME = "DormantJob";

  @CmdLine(name = "queried_job_active_stages",
      help = "Test stages for active task count of the job queried.")
  private static final Arg<List<Integer>> ACTIVE_STAGES =
      Arg.create((List<Integer>) ImmutableList.<Integer>of(100, 1000, 10000));

  @CmdLine(name = "queried_job_inactive_stages",
      help = "Test stages for inactive task count of the job queried.")
  private static final Arg<List<Integer>> INACTIVE_STAGES =
      Arg.create((List<Integer>) ImmutableList.<Integer>of(100, 1000, 10000));

  @CmdLine(name = "dormant_task_stages",
      help = "Test stages dormant task count (tasks that are not queried).")
  private static final Arg<List<Integer>> DORMANT_STAGES =
      Arg.create((List<Integer>) ImmutableList.<Integer>of(1000, 10000, 100000));

  @CmdLine(name = "fetches", help = "Number of fetch operations to perform in each test stage.")
  private static final Arg<Integer> FETCHES = Arg.create(10000);

  enum QueryType {
    BY_ID,
    BY_JOB,
    BY_ROLE,
    ALL
  }

  @Override
  public void run() {
    for (QueryType type : QueryType.values()) {
      for (int dormantTasks : DORMANT_STAGES.get()) {
        run(type, dormantTasks);
      }
    }
  }

  // Using a separate function here for now to satisfy checkstyle - current rules
  // don't allow nesting depth > 3.
  private void run(QueryType type, int dormantTasks) {
    for (int activeTasks : ACTIVE_STAGES.get()) {
      for (int inactiveTasks : INACTIVE_STAGES.get()) {
        run(new Stage(activeTasks, inactiveTasks, dormantTasks, type));
      }
    }
  }

  private void run(final Stage stage) {
    System.out.println("Executing stage " + stage);
    Storage storage = MemStorage.newEmptyStorage();
    storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getTaskStore().saveTasks(
            replicate(
                makeTask(QUERIED_ROLE_NAME, QUERIED_JOB_NAME).setStatus(ScheduleStatus.RUNNING),
                stage.active));
        storeProvider.getTaskStore().saveTasks(
            replicate(
                makeTask(QUERIED_ROLE_NAME, QUERIED_JOB_NAME).setStatus(ScheduleStatus.FINISHED),
                stage.inactive));
        storeProvider.getTaskStore().saveTasks(
            replicate(
                makeTask(DORMANT_ROLE_NAME, DORMANT_JOB_NAME).setStatus(ScheduleStatus.RUNNING),
                stage.dormant));
      }
    });

    final TaskQuery query = new TaskQuery();
    switch (stage.type) {
      case BY_ID:
        query.setTaskIds(storage.doInTransaction(new Work.Quiet<Set<String>>() {
          @Override
          public Set<String> apply(StoreProvider storeProvider) {
            return storeProvider.getTaskStore().fetchTaskIds(
                new TaskQuery()
                    .setOwner(new Identity().setRole(QUERIED_ROLE_NAME))
                    .setJobName(QUERIED_JOB_NAME)
                    .setShardIds(ImmutableSet.of(0)));
          }
        }));
        break;

      case BY_ROLE:
        query.setOwner(new Identity().setRole(QUERIED_ROLE_NAME));
        break;

      case BY_JOB:
        query.setOwner(new Identity().setRole(QUERIED_ROLE_NAME))
            .setJobName(QUERIED_JOB_NAME);
        break;

      case ALL:
        break;

      default:
        throw new IllegalStateException("Unrecognized type " + stage.type);
    }

    long start = System.nanoTime();
    for (int i = 0; i < FETCHES.get(); i++) {
      storage.doInTransaction(new Work.Quiet<Void>() {
        @Override public Void apply(StoreProvider storeProvider) {
          storeProvider.getTaskStore().fetchTasks(query);
          return null;
        }
      });
    }

    int queries = FETCHES.get();
    long timeMs = Amount.of(System.nanoTime() - start, Time.NANOSECONDS).as(Time.MILLISECONDS);
    System.out.println("Completed " + queries + " queries in "
        + timeMs + "ms (" + (((double) queries * 1000) / timeMs) + " qps)");
  }

  private ScheduledTask makeTask(String owner, String jobName) {
    return new ScheduledTask()
        .setAssignedTask(new AssignedTask()
            .setTaskId(UUID.randomUUID().toString())
            .setTask(new TwitterTaskInfo()
                .setShardId(0)
                .setOwner(new Identity().setRole(owner))
                .setJobName(jobName)));
  }

  private Set<ScheduledTask> replicate(ScheduledTask task, int copies) {
    ImmutableSet.Builder<ScheduledTask> tasks = ImmutableSet.<ScheduledTask>builder()
        .add(task);
    for (int i = 1; i < copies; i++) {
      ScheduledTask copy = task.deepCopy();
      copy.getAssignedTask().getTask().setShardId(i);
      copy.getAssignedTask().setTaskId(UUID.randomUUID().toString());
      tasks.add(copy);
    }
    return tasks.build();
  }

  private static class Stage {
    final int active;
    final int inactive;
    final int dormant;
    final QueryType type;

    Stage(int active, int inactive, int dormant, QueryType type) {
      this.active = active;
      this.inactive = inactive;
      this.dormant = dormant;
      this.type = type;
    }

    @Override public String toString() {
      return Objects.toStringHelper(this)
          .add("active", active)
          .add("inactive", inactive)
          .add("dormant", dormant)
          .add("type", type)
          .toString();
    }
  }

  /**
   * Runs the app.
   *
   * @param args Command line args.
   */
  public static void main(String[] args) {
    AppLauncher.launch(
        StorageBenchmark.class,
        ArgFilters.selectClass(StorageBenchmark.class),
        args);
  }
}
