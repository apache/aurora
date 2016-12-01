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
package org.apache.aurora.scheduler;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.Atomics;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.aurora.GuavaUtils.ServiceManagerIface;
import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.common.base.Consumers;
import org.apache.aurora.common.base.ExceptionalCommand;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.StateMachine;
import org.apache.aurora.common.util.StateMachine.Transition;
import org.apache.aurora.common.zookeeper.SingletonService;
import org.apache.aurora.common.zookeeper.SingletonService.LeaderControl;
import org.apache.aurora.scheduler.events.PubsubEvent.DriverRegistered;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.common.zookeeper.SingletonService.LeadershipListener;

/**
 * The central driver of the scheduler runtime lifecycle.  Handles the transitions from startup and
 * initialization through acting as a standby scheduler / log replica and finally to becoming the
 * scheduler leader.
 * <p>
 * The (enforced) call order to be used with this class is:
 * <ol>
 *   <li>{@link #prepare()}, to initialize the storage system.</li>
 *   <li>{@link LeadershipListener#onLeading(LeaderControl) onLeading()} on the
 *       {@link LeadershipListener LeadershipListener}
 *       returned from {@link #prepare()}, signaling that this process has exclusive control of the
 *       cluster.</li>
 *   <li>{@link #registered(DriverRegistered) registered()},
 *       indicating that registration with the mesos master has succeeded.
 *       At this point, the scheduler's presence will be announced via
 *       {@link LeaderControl#advertise() advertise()}.</li>
 * </ol>
 * If this call order is broken, calls will fail by throwing
 * {@link java.lang.IllegalStateException}.
 * <p>
 * At any point in the lifecycle, the scheduler will respond to
 * {@link LeadershipListener#onDefeated()
 * onDefeated()} by initiating a clean shutdown using {@link Lifecycle#shutdown() shutdown()}.
 * A clean shutdown will also be initiated if control actions fail during normal state transitions.
 */
public class SchedulerLifecycle implements EventSubscriber {

  private static final Logger LOG = LoggerFactory.getLogger(SchedulerLifecycle.class);

  @VisibleForTesting
  enum State {
    IDLE,
    PREPARING_STORAGE,
    STORAGE_PREPARED,
    LEADER_AWAITING_REGISTRATION,
    ACTIVE,
    DEAD
  }

  private static final Predicate<Transition<State>> IS_DEAD = state -> state.getTo() == State.DEAD;

  private static final Predicate<Transition<State>> NOT_DEAD = Predicates.not(IS_DEAD);

  private final LeadershipListener leadershipListener;
  private final AtomicBoolean registrationAcked = new AtomicBoolean(false);
  private final AtomicReference<LeaderControl> leaderControl = Atomics.newReference();
  private final StateMachine<State> stateMachine;

  @Inject
  SchedulerLifecycle(
      NonVolatileStorage storage,
      Lifecycle lifecycle,
      Driver driver,
      LeadingOptions leadingOptions,
      ScheduledExecutorService executorService,
      ShutdownRegistry shutdownRegistry,
      StatsProvider statsProvider,
      @SchedulerActive ServiceManagerIface schedulerActiveServiceManager) {

    this(
        storage,
        lifecycle,
        driver,
        new DefaultDelayedActions(leadingOptions, executorService),
        shutdownRegistry,
        statsProvider,
        schedulerActiveServiceManager);
  }

  private static final class DefaultDelayedActions implements DelayedActions {
    private final LeadingOptions leadingOptions;
    private final ScheduledExecutorService executorService;

    DefaultDelayedActions(LeadingOptions leadingOptions, ScheduledExecutorService executorService) {
      this.leadingOptions = requireNonNull(leadingOptions);
      this.executorService = requireNonNull(executorService);
    }

    @Override
    public void blockingDriverJoin(Runnable runnable) {
      // We intentionally use an independent thread for this operation, since it blocks
      // indefinitely. Using a separate thread allows us to inject an executor service with a safe
      // expectation of operations that use minimal blocking.
      new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("BlockingDriverJoin")
          .build()
          .newThread(runnable)
          .start();
    }

    @Override
    public void onAutoFailover(Runnable runnable) {
      executorService.schedule(
          runnable,
          leadingOptions.leadingTimeLimit.getValue(),
          leadingOptions.leadingTimeLimit.getUnit().getTimeUnit());
    }

    @Override
    public void onRegistrationTimeout(Runnable runnable) {
      LOG.info(
          "Giving up on registration in " + leadingOptions.registrationDelayLimit);
      executorService.schedule(
          runnable,
          leadingOptions.registrationDelayLimit.getValue(),
          leadingOptions.registrationDelayLimit.getUnit().getTimeUnit());
    }
  }

  @VisibleForTesting
  static final String REGISTERED_GAUGE = "framework_registered";

  @VisibleForTesting
  static String stateGaugeName(State state) {
    return "scheduler_lifecycle_" + state;
  }

  @VisibleForTesting
  SchedulerLifecycle(
      final NonVolatileStorage storage,
      final Lifecycle lifecycle,
      final Driver driver,
      final DelayedActions delayedActions,
      final ShutdownRegistry shutdownRegistry,
      StatsProvider statsProvider,
      final ServiceManagerIface schedulerActiveServiceManager) {

    requireNonNull(storage);
    requireNonNull(lifecycle);
    requireNonNull(driver);
    requireNonNull(delayedActions);
    requireNonNull(shutdownRegistry);

    statsProvider.makeGauge(
        REGISTERED_GAUGE,
        new Supplier<Integer>() {
          @Override
          public Integer get() {
            return registrationAcked.get() ? 1 : 0;
          }
        });
    for (final State state : State.values()) {
      statsProvider.makeGauge(
          stateGaugeName(state),
          new Supplier<Integer>() {
            @Override
            public Integer get() {
              return (state == stateMachine.getState()) ? 1 : 0;
            }
          });
    }

    shutdownRegistry.addAction(new ExceptionalCommand<TimeoutException>() {
      @Override
      public void execute() throws TimeoutException {
        stateMachine.transition(State.DEAD);
        schedulerActiveServiceManager.stopAsync();
        schedulerActiveServiceManager.awaitStopped(5L, TimeUnit.SECONDS);
      }
    });

    final Consumer<Transition<State>> prepareStorage = new Consumer<Transition<State>>() {
      @Override
      public void accept(Transition<State> transition) {
        storage.prepare();
        stateMachine.transition(State.STORAGE_PREPARED);
      }
    };

    final Consumer<Transition<State>> handleLeading = new Consumer<Transition<State>>() {
      @Override
      public void accept(Transition<State> transition) {
        LOG.info("Elected as leading scheduler!");

        storage.start(stores -> {
          // If storage backfill operations are necessary, they can be done here.
        });

        driver.startAsync().awaitRunning();

        delayedActions.onRegistrationTimeout(
            () -> {
              if (!registrationAcked.get()) {
                LOG.error(
                    "Framework has not been registered within the tolerated delay.");
                stateMachine.transition(State.DEAD);
              }
            });

        delayedActions.onAutoFailover(
            () -> {
              LOG.info("Triggering automatic failover.");
              stateMachine.transition(State.DEAD);
            });
      }
    };

    final Consumer<Transition<State>> handleRegistered = new Consumer<Transition<State>>() {
      @Override
      public void accept(Transition<State> transition) {
        registrationAcked.set(true);
        delayedActions.blockingDriverJoin(() -> {
          driver.blockUntilStopped();
          LOG.info("Driver exited, terminating lifecycle.");
          stateMachine.transition(State.DEAD);
        });

        // TODO(ksweeney): Extract leader advertisement to its own service.
        schedulerActiveServiceManager.startAsync().awaitHealthy();
        try {
          leaderControl.get().advertise();
        } catch (SingletonService.AdvertiseException | InterruptedException e) {
          LOG.error("Failed to advertise leader, shutting down.");
          throw new RuntimeException(e);
        }
      }
    };

    final Consumer<Transition<State>> shutDown = new Consumer<Transition<State>>() {
      private final AtomicBoolean invoked = new AtomicBoolean(false);
      @Override
      public void accept(Transition<State> transition) {
        if (!invoked.compareAndSet(false, true)) {
          LOG.info("Shutdown already invoked, ignoring extra call.");
          return;
        }

        // TODO(wfarner): Consider using something like guava's Closer to abstractly tear down
        // resources here.
        try {
          LeaderControl control = leaderControl.get();
          if (control != null) {
            try {
              control.leave();
            } catch (SingletonService.LeaveException e) {
              LOG.warn("Failed to leave leadership: " + e, e);
            }
          }

          // TODO(wfarner): Re-evaluate tear-down ordering here.  Should the top-level shutdown
          // be invoked first, or the underlying critical components?
          driver.stopAsync().awaitTerminated();
          storage.stop();
        } finally {
          lifecycle.shutdown();
        }
      }
    };

    stateMachine = StateMachine.<State>builder("SchedulerLifecycle")
        .initialState(State.IDLE)
        .logTransitions()
        .addState(
            dieOnError(Consumers.filter(NOT_DEAD, prepareStorage)),
            State.IDLE,
            State.PREPARING_STORAGE, State.DEAD)
        .addState(
            State.PREPARING_STORAGE,
            State.STORAGE_PREPARED, State.DEAD)
        .addState(
            dieOnError(Consumers.filter(NOT_DEAD, handleLeading)),
            State.STORAGE_PREPARED,
            State.LEADER_AWAITING_REGISTRATION, State.DEAD)
        .addState(
            dieOnError(Consumers.filter(NOT_DEAD, handleRegistered)),
            State.LEADER_AWAITING_REGISTRATION,
            State.ACTIVE, State.DEAD)
        .addState(
            State.ACTIVE,
            State.DEAD)
        .addState(
            State.DEAD,
            // Allow cycles in DEAD to prevent throwing and avoid the need for call-site checking.
            State.DEAD
        )
        .onAnyTransition(
            Consumers.filter(IS_DEAD, shutDown))
        .build();

    this.leadershipListener = new SchedulerCandidateImpl(stateMachine, leaderControl);
  }

  private Consumer<Transition<State>> dieOnError(final Consumer<Transition<State>> closure) {
    return transition -> {
      try {
        closure.accept(transition);
      } catch (RuntimeException e) {
        LOG.error("Caught unchecked exception: " + e, e);
        stateMachine.transition(State.DEAD);
        throw e;
      }
    };
  }

  /**
   * Prepares a scheduler to offer itself as a leader candidate.  After this call the scheduler will
   * host a live log replica and start syncing data from the leader via the log until it gets called
   * upon to lead.
   *
   * @return A listener that can be offered for leadership of a distributed election.
   */
  public LeadershipListener prepare() {
    stateMachine.transition(State.PREPARING_STORAGE);
    return leadershipListener;
  }

  @Subscribe
  public void registered(DriverRegistered event) {
    stateMachine.transition(State.ACTIVE);
  }

  private static class SchedulerCandidateImpl implements LeadershipListener {
    private final StateMachine<State> stateMachine;
    private final AtomicReference<LeaderControl> leaderControl;

    SchedulerCandidateImpl(
        StateMachine<State> stateMachine,
        AtomicReference<LeaderControl> leaderControl) {

      this.stateMachine = stateMachine;
      this.leaderControl = leaderControl;
    }

    @Override
    public void onLeading(LeaderControl control) {
      leaderControl.set(control);
      stateMachine.transition(State.LEADER_AWAITING_REGISTRATION);
    }

    @Override
    public void onDefeated() {
      LOG.error("Lost leadership, committing suicide.");
      stateMachine.transition(State.DEAD);
    }
  }

  public static class LeadingOptions {
    private final Amount<Long, Time> registrationDelayLimit;
    private final Amount<Long, Time> leadingTimeLimit;

    /**
     * Creates a new collection of options for tuning leadership behavior.
     *
     * @param registrationDelayLimit Maximum amount of time to wait for framework registration to
     *                               complete.
     * @param leadingTimeLimit Maximum amount of time to serve as leader before abdicating.
     */
    public LeadingOptions(
        Amount<Long, Time> registrationDelayLimit,
        Amount<Long, Time> leadingTimeLimit) {

      Preconditions.checkArgument(
          registrationDelayLimit.getValue() >= 0,
          "Registration delay limit must be positive.");
      Preconditions.checkArgument(
          leadingTimeLimit.getValue() >= 0,
          "Leading time limit must be positive.");

      this.registrationDelayLimit = requireNonNull(registrationDelayLimit);
      this.leadingTimeLimit = requireNonNull(leadingTimeLimit);
    }
  }

  @VisibleForTesting
  interface DelayedActions {
    void blockingDriverJoin(Runnable runnable);

    void onAutoFailover(Runnable runnable);

    void onRegistrationTimeout(Runnable runnable);
  }

  /**
   * Qualifier for services that will be run after the scheduler storage is available
   * but before leadership is announced in ZooKeeper.
   */
  @Qualifier
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER})
  public static @interface SchedulerActive { }
}
