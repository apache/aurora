package com.twitter.mesos.scheduler.testing;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;

import com.twitter.common.application.AbstractApplication;
import com.twitter.common.application.AppLauncher;
import com.twitter.common.args.Arg;
import com.twitter.common.args.ArgFilters;
import com.twitter.common.args.CmdLine;
import com.twitter.common.io.FileUtils;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.mem.MemStorage;

/**
 * Benchmark to evaluate storage performance.
 *
 * This can be run with:
 * $ ./pants goal run src/java/com/twitter/mesos/scheduler:storage_benchmark_app \
 *     --jvm-run-jvmargs='-Xms4g -Xmx4g -XX:+UseConcMarkSweepGC -XX:+UseParNewGC'
 */
public class StorageBenchmark extends AbstractApplication {

  private static final String QUERIED_ROLE_NAME = "QueriedRole";
  private static final String QUERIED_JOB_NAME = "QueriedJob";
  private static final String DORMANT_ROLE_NAME = "DormantRole";
  private static final String DORMANT_JOB_NAME = "DormantJob";

  @CmdLine(name = "queried_job_active_stages",
      help = "Test stages for active task count of the job queried.")
  private static final Arg<List<Integer>> ACTIVE_STAGES =
      Arg.create((List<Integer>) ImmutableList.<Integer>of(10, 100, 1000));

  @CmdLine(name = "queried_job_inactive_stages",
      help = "Test stages for inactive task count of the job queried.")
  private static final Arg<List<Integer>> INACTIVE_STAGES =
      Arg.create((List<Integer>) ImmutableList.<Integer>of(100, 1000));

  @CmdLine(name = "dormant_task_stages",
      help = "Test stages dormant task count (tasks that are not queried).")
  private static final Arg<List<Integer>> DORMANT_STAGES =
      Arg.create((List<Integer>) ImmutableList.<Integer>of(
          1000,
          5000,
          10000,
          50000));

  @CmdLine(name = "fetches", help = "Number of fetch operations to perform in each test stage.")
  private static final Arg<Integer> FETCHES = Arg.create(10000);

  enum QueryType {
    BY_ID,
    BY_JOB,
    BY_ROLE
  }

  @Override
  public void run() {
    final File outputDir = FileUtils.createTempDir();
    try {
      System.out.println("Writing test output to " + outputDir);
      String readme = "Test configuration"
          + "\nqueried_job_active_stages: " + ACTIVE_STAGES.get()
          + "\nqueried_job_inactive_stages: " + INACTIVE_STAGES.get()
          + "\ndormant_task_stages: " + DORMANT_STAGES.get()
          + "\nfetches: " + FETCHES.get()
          + "\nquery types: " + ImmutableList.copyOf(QueryType.values()) + "\n";
      Files.write(readme, new File(outputDir, "README"), Charsets.UTF_8);

      for (QueryType type : QueryType.values()) {
        // Each data file represents a scatter plot on a graph.
        List<Plot> plots = Lists.newArrayList();
        for (int activeTasks : ACTIVE_STAGES.get()) {
          plots.addAll(run(type, activeTasks, outputDir));
        }

        Iterable<String> plotCommands = FluentIterable.from(plots).transform(
            new Function<Plot, String>() {
              @Override public String apply(Plot plot) {
                return "'" + plot.file + "' title '" + plot.title + "' with linespoints";
              }
            });

        List<String> commands = ImmutableList.of(
            "set title '" + type.name().toLowerCase() + "'",
            "set xlabel 'Task Store Size'",
            "set ylabel 'QPS'",
            "set term png",
            "set output '" + type.name().toLowerCase() + ".png'",
            "plot " + Joiner.on(", ").join(plotCommands)
        );
        String[] command = new String[] {
            "gnuplot",
            "-e",
            Joiner.on("; ").join(commands) + ""
        };
        try {
          Process process = Runtime.getRuntime().exec(command);
          int result = process.waitFor();
          if (result != 0) {
            System.err.println("gnuplot exited with code " + result);
            System.err.println("stdout: "
                + CharStreams.toString(new InputStreamReader(process.getInputStream())));
            System.err.println("stderr: "
                + CharStreams.toString(new InputStreamReader(process.getErrorStream())));
          }
        } catch (InterruptedException e) {
          System.err.println("Interrupted while waiting for gnuplot command:" + e);
          Thread.currentThread().interrupt();
        }
      }
    } catch (IOException e) {
      System.err.println("Failed to write test output: " + e.getMessage());
    } finally {
      try {
        org.apache.commons.io.FileUtils.deleteDirectory(outputDir);
      } catch (IOException e) {
        System.err.println("Failed to delete temp dir:" + e);
      }
    }
  }

  private static class Plot {
    final String file;
    final String title;

    Plot(String file, String title) {
      this.file = file;
      this.title = title;
    }
  }

  // Using a separate function here for now to satisfy checkstyle - current rules
  // don't allow nesting depth > 3.
  private List<Plot> run(QueryType type, int activeTasks, File outputDir) throws IOException {
    List<Plot> plots = Lists.newArrayList();
    for (int inactiveTasks : INACTIVE_STAGES.get()) {
      List<String> points = Lists.newArrayList();
      for (int dormantTasks : DORMANT_STAGES.get()) {
        TestResult result = run(new Stage(activeTasks, inactiveTasks, dormantTasks, type));
        points.add(result.totalTasks + " " + result.qps);
      }

      File dataFile = new File(outputDir, type.name().toLowerCase()
          + "_" + activeTasks + "_active" + "_" + inactiveTasks + "_inactive");
      Files.write(
          Joiner.on("\n").join(points) + "\n",
          dataFile,
          Charsets.UTF_8);
      plots.add(new Plot(
          dataFile.getAbsolutePath(),
          activeTasks + "_active" + "_" + inactiveTasks + "_inactive"));
    }
    return plots;
  }

  private static class TestResult {
    double qps;
    int totalTasks;

    TestResult(double qps, int totalTasks) {
      this.qps = qps;
      this.totalTasks = totalTasks;
    }
  }

  private TestResult run(final Stage stage) throws IOException {
    System.out.println("Executing stage " + stage);
    Storage storage = MemStorage.newEmptyStorage();
    storage.writeOp(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(
            replicate(
                makeTask(QUERIED_ROLE_NAME, QUERIED_JOB_NAME).setStatus(ScheduleStatus.RUNNING),
                stage.active));
        storeProvider.getUnsafeTaskStore().saveTasks(
            replicate(
                makeTask(QUERIED_ROLE_NAME, QUERIED_JOB_NAME).setStatus(ScheduleStatus.FINISHED),
                stage.inactive));
        storeProvider.getUnsafeTaskStore().saveTasks(
            replicate(
                makeTask(DORMANT_ROLE_NAME, DORMANT_JOB_NAME).setStatus(ScheduleStatus.RUNNING),
                stage.dormant));
      }
    });

    final TaskQuery query;
    switch (stage.type) {
      case BY_ID:
        query = Query.byId(storage.readOp(new Work.Quiet<Set<String>>() {
          @Override public Set<String> apply(StoreProvider storeProvider) {
            return storeProvider.getTaskStore().fetchTaskIds(
                Query.shardScoped(QUERIED_ROLE_NAME, QUERIED_JOB_NAME, 0).active().get());
          }
        }));
        break;

      case BY_ROLE:
        query = Query.byRole(QUERIED_ROLE_NAME);
        break;

      case BY_JOB:
        query = Query.byJob(QUERIED_ROLE_NAME, QUERIED_JOB_NAME);
        break;

      default:
        throw new IllegalStateException("Unrecognized type " + stage.type);
    }

    long start = System.nanoTime();
    for (int i = 0; i < FETCHES.get(); i++) {
      storage.readOp(new Work.Quiet<Void>() {
        @Override public Void apply(StoreProvider storeProvider) {
          storeProvider.getTaskStore().fetchTasks(query);
          return null;
        }
      });
    }

    long timeMs = Amount.of(System.nanoTime() - start, Time.NANOSECONDS).as(Time.MILLISECONDS);
    int totalTasks = storage.readOp(new Work.Quiet<Integer>() {
      @Override public Integer apply(StoreProvider storeProvider) {
        return storeProvider.getTaskStore().fetchTasks(Query.GET_ALL).size();
      }
    });

    return new TestResult(((double) FETCHES.get() * 1000) / timeMs, totalTasks);
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
