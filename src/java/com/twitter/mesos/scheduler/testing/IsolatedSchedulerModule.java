package com.twitter.mesos.scheduler.testing;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;

import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Text;
import org.apache.mesos.Protos.Value.Type;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;
import com.twitter.mesos.gen.CreateJobResponse;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.Package;
import com.twitter.mesos.gen.SessionKey;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.scheduler.DriverFactory;
import com.twitter.mesos.scheduler.Resources;
import com.twitter.mesos.scheduler.SchedulerThriftInterface;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.events.PubsubEvent.DriverRegistered;
import com.twitter.mesos.scheduler.events.PubsubEvent.EventSubscriber;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.events.TaskEventModule;
import com.twitter.mesos.scheduler.storage.DistributedSnapshotStore;
import com.twitter.mesos.scheduler.storage.mem.MemStorageModule;
import com.twitter.mesos.scheduler.testing.FakeDriverFactory.FakeSchedulerDriver;

/**
 * A module that binds a fake mesos driver factory and a volatile storage system.
 */
public class IsolatedSchedulerModule extends AbstractModule {

  private static final Logger LOG = Logger.getLogger(IsolatedSchedulerModule.class.getName());

  @Override
  protected void configure() {
    MemStorageModule.bind(binder());

    bind(DriverFactory.class).to(FakeDriverFactory.class);
    bind(FakeDriverFactory.class).in(Singleton.class);
    bind(DistributedSnapshotStore.class).toInstance(new DistributedSnapshotStore() {
      @Override public void persist(Snapshot snapshot) {
        LOG.warning("Pretending to write snapshot.");
      }
    });
    LifecycleModule.bindStartupAction(binder(), FakeClusterRunner.class);
    TaskEventModule.bindSubscriber(binder(), FakeClusterRunner.class);
  }

  static class FakeClusterRunner implements Command, EventSubscriber {
    private final FrameworkID frameworkId =
        FrameworkID.newBuilder().setValue("framework-id").build();
    private final List<FakeSlave> cluster = ImmutableList.of(
        new FakeSlave(frameworkId, "fake-host1", "rack1", "slave-id1"),
        new FakeSlave(frameworkId, "fake-host2", "rack2", "slave-id2")
    );

    private final AtomicLong offerId = new AtomicLong();
    private final Function<FakeSlave, Offer> slaveToOffer = new Function<FakeSlave, Offer>() {
      @Override public Offer apply(FakeSlave slave) {
        return slave.makeOffer(offerId.incrementAndGet());
      }
    };

    private final Provider<Scheduler> scheduler;
    private final SchedulerThriftInterface thrift;
    private final ScheduledExecutorService executor;
    private final SchedulerDriver driver;

    @Inject
    FakeClusterRunner(
        Provider<Scheduler> scheduler,
        SchedulerThriftInterface thrift,
        ShutdownRegistry shutdownRegistry) {

      this.scheduler = scheduler;
      this.thrift = thrift;
      this.executor = createThreadPool(shutdownRegistry);
      this.driver = new FakeSchedulerDriver();
    }

    private static ScheduledExecutorService createThreadPool(ShutdownRegistry shutdownRegistry) {
      final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
          1,
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("TaskScheduler-%d").build()) {

        @Override protected void afterExecute(Runnable runnable, @Nullable Throwable throwable) {
          if (throwable != null) {
            LOG.log(Level.WARNING, "Error: " + throwable, throwable);
          } else if (runnable instanceof Future) {
            Future<?> future = (Future<?>) runnable;
            try {
              future.get();
            } catch (InterruptedException e) {
              e.printStackTrace();
            } catch (ExecutionException e) {
              e.printStackTrace();
            }
          }
        }
      };
      Stats.exportSize("schedule_queue_size", executor.getQueue());
      shutdownRegistry.addAction(new Command() {
        @Override public void execute() {
          new ExecutorServiceShutdown(executor, Amount.of(1L, Time.SECONDS)).execute();
        }
      });
      return executor;
    }

    @Override
    public void execute() {
      executor.submit(new Runnable() {
        @Override public void run() {
          scheduler.get().registered(
              driver,
              frameworkId,
              MasterInfo.newBuilder().setId("master-id").setIp(100).setPort(200).build());
        }
      });
    }

    private void offerClusterResources() {
      executor.submit(new Runnable() {
        @Override public void run() {
          scheduler.get().resourceOffers(
              driver,
              FluentIterable.from(cluster).transform(slaveToOffer).toImmutableList());
        }
      });
    }

    @Subscribe
    public void registered(DriverRegistered event) {
      executor.submit(new Runnable() {
        @Override public void run() {
          Identity mesosUser = new Identity("mesos", "mesos");
          for (int i = 0; i < 20; i++) {
            JobConfiguration service = createJob("serviceJob" + i, mesosUser);
            service.getTaskConfig().setIsService(true);
            submitJob(service);
          }

          for (int i = 0; i < 20; i++) {
            JobConfiguration cron = createJob("cronJob" + i, mesosUser);
            cron.setCronSchedule("* * * * *");
            submitJob(cron);
          }
        }
      });

      offerClusterResources();
      // Send the offers again, since the first batch of offers will be consumed by GC executors.
      offerClusterResources();
    }

    private void moveTaskToState(final String taskId, final TaskState state, long delaySeconds) {
      Runnable changeState = new Runnable() {
        @Override public void run() {
          scheduler.get().statusUpdate(
              driver,
              TaskStatus.newBuilder()
                  .setTaskId(TaskID.newBuilder().setValue(taskId))
                  .setState(state)
                  .build());
        }
      };
      executor.schedule(changeState, delaySeconds, TimeUnit.SECONDS);
    }

    @Subscribe
    public void stateChanged(TaskStateChange stateChange) {
      String taskId = stateChange.getTaskId();
      switch (stateChange.getNewState()) {
        case ASSIGNED:
          moveTaskToState(taskId, TaskState.TASK_STARTING, 1);
          break;

        case STARTING:
          moveTaskToState(taskId, TaskState.TASK_RUNNING, 1);
          break;

        case RUNNING:
          // Let the task finish some time randomly in the next 5 minutes.
          moveTaskToState(taskId, TaskState.TASK_FINISHED, (long) (Math.random() * 300));
          break;

        case FINISHED:
          offerClusterResources();
          break;

        default:
          break;
      }
    }

    private JobConfiguration createJob(String jobName, Identity owner) {
      return new JobConfiguration()
          .setName(jobName)
          .setOwner(owner)
          .setShardCount(5)
          .setTaskConfig(new TwitterTaskInfo()
              .setOwner(owner)
              .setJobName(jobName)
              .setNumCpus(1.0)
              .setDiskMb(1024)
              .setRamMb(1024)
              .setPackages(ImmutableSet.of(new Package(owner.getRole(), "package", 15)))
              .setThermosConfig("opaque".getBytes()));
    }

    private void submitJob(JobConfiguration job) {
      SessionKey sessionKey = new SessionKey(
          job.getOwner().getUser(),
          System.currentTimeMillis(),
          ByteBuffer.wrap("fake".getBytes()));
      CreateJobResponse response = thrift.createJob(
          job,
          sessionKey);
      LOG.info("Create job response: " + response);
    }
  }

  private static class FakeSlave {
    private final FrameworkID framework;
    private final String host;
    private final String rack;
    private final String slaveId;

    FakeSlave(FrameworkID framework, String host, String rack, String slaveId) {
      this.framework = framework;
      this.host = host;
      this.rack = rack;
      this.slaveId = slaveId;
    }

    private static Resource.Builder scalar(String name, double value) {
      return Resource.newBuilder()
          .setName(name)
          .setType(Type.SCALAR)
          .setScalar(Scalar.newBuilder().setValue(value));
    }

    private static Attribute.Builder attribute(String name, String value) {
      return Attribute.newBuilder()
          .setName(name)
          .setType(Type.TEXT)
          .setText(Text.newBuilder().setValue(value));
    }

    Offer makeOffer(long offerId) {
      return Offer.newBuilder()
          .setId(OfferID.newBuilder().setValue("offer" + offerId))
          .setFrameworkId(framework)
          .setSlaveId(SlaveID.newBuilder().setValue(slaveId))
          .setHostname(host)
          .addResources(scalar(Resources.CPUS, 16))
          .addResources(scalar(Resources.RAM_MB, 24576))
          .addResources(scalar(Resources.DISK_MB, 102400))
          .addAttributes(attribute(ConfigurationManager.RACK_CONSTRAINT, rack))
          .addAttributes(attribute(ConfigurationManager.HOST_CONSTRAINT, host))
          .build();
    }
  }
}
