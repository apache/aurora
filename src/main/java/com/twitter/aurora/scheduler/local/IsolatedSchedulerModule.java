/*
 * Copyright 2013 Twitter, Inc.
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
package com.twitter.aurora.scheduler.local;

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
import com.google.common.base.Throwables;
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
import org.apache.thrift.TException;

import com.twitter.aurora.gen.AuroraAdmin;
import com.twitter.aurora.gen.ExecutorConfig;
import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.Package;
import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.gen.Response;
import com.twitter.aurora.gen.SessionKey;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.scheduler.DriverFactory;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.configuration.ConfigurationManager;
import com.twitter.aurora.scheduler.configuration.Resources;
import com.twitter.aurora.scheduler.events.PubsubEvent.DriverRegistered;
import com.twitter.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import com.twitter.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.aurora.scheduler.events.PubsubEventModule;
import com.twitter.aurora.scheduler.local.FakeDriverFactory.FakeSchedulerDriver;
import com.twitter.aurora.scheduler.log.testing.FileLogStreamModule;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;

/**
 * A module that binds a fake mesos driver factory and a local (non-replicated) storage system.
 * <p>
 * The easiest way to run the scheduler in local/isolated mode is by executing:
 * <pre>
 * $ ./pants goal bundle aurora:scheduler-local && ./aurora/scripts/scheduler.sh -c local
 * </pre>
 */
public class IsolatedSchedulerModule extends AbstractModule {

  private static final Logger LOG = Logger.getLogger(IsolatedSchedulerModule.class.getName());

  @Override
  protected void configure() {
    bind(DriverFactory.class).to(FakeDriverFactory.class);
    bind(FakeDriverFactory.class).in(Singleton.class);
    PubsubEventModule.bindSubscriber(binder(), FakeClusterRunner.class);
    install(new FileLogStreamModule());
  }

  static class FakeClusterRunner implements EventSubscriber {
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
    private final AuroraAdmin.Iface thrift;
    private final ScheduledExecutorService executor;
    private final SchedulerDriver driver;

    @Inject
    FakeClusterRunner(
        Provider<Scheduler> scheduler,
        AuroraAdmin.Iface thrift,
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
            } catch (InterruptedException | ExecutionException e) {
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

    private void offerClusterResources() {
      executor.submit(new Runnable() {
        @Override public void run() {
          scheduler.get().resourceOffers(
              driver,
              FluentIterable.from(cluster).transform(slaveToOffer).toList());
        }
      });
    }

    private void setQuotas() {
      executor.submit(new Runnable() {
        @Override public void run() {
          try {
            thrift.setQuota(
                "mesos",
                new Quota(2.0 * 1000000, 100000000, 100000000),
                new SessionKey());
          } catch (TException e) {
            throw Throwables.propagate(e);
          }
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
            service.getTaskConfig().setProduction((i % 2) == 0);
            service.getTaskConfig().setIsService(true);
            submitJob(service);
          }

          for (int i = 0; i < 20; i++) {
            JobConfiguration adhocJob = createJob("adhocJob" + i, mesosUser);
            adhocJob.getTaskConfig().setProduction((i % 2) == 0);
            adhocJob.getTaskConfig();
            submitJob(adhocJob);
          }

          for (int i = 0; i < 20; i++) {
            JobConfiguration cron = createJob("cronJob" + i, mesosUser);
            cron.getTaskConfig().setProduction((i % 2) == 0);
            cron.setCronSchedule("* * * * *");
            submitJob(cron);
          }
        }
      });

      setQuotas();
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
          .setKey(JobKeys.from(owner.getRole(), "test", jobName).newBuilder())
          .setOwner(owner)
          .setInstanceCount(5)
          .setTaskConfig(new TaskConfig()
              .setOwner(owner)
              .setJobName(jobName)
              .setEnvironment("test")
              .setNumCpus(1.0)
              .setDiskMb(1024)
              .setRamMb(1024)
              .setPackages(ImmutableSet.of(new Package(owner.getRole(), "package", 15)))
              // TODO(maximk): Dump thermosConfig during the MESOS-2635 cleanup stage
              .setThermosConfig("opaque".getBytes())
              .setExecutorConfig(new ExecutorConfig("aurora", "opaque")));
    }

    private void submitJob(JobConfiguration job) {
      Response response;
      try {
        response = thrift.createJob(job, null, new SessionKey());
      } catch (TException e) {
        throw Throwables.propagate(e);
      }
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
