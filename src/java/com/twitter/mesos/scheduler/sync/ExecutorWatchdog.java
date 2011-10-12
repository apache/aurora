package com.twitter.mesos.scheduler.sync;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;
import com.twitter.mesos.ExecutorKey;
import com.twitter.mesos.gen.comm.StateUpdateRequest;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Maintains state information about executors to facilitate incremental state syncing.
 *
 * @author William Farner
 */
public interface ExecutorWatchdog {

  /**
   * Notifies the watchdog that a state update was received from an executor.
   *
   * @param executor Executor being updated.
   * @param executorUUID Unique ID for the executor run.
   * @param position Position that state was synced up to.
   */
  void stateUpdated(ExecutorKey executor, String executorUUID, int position);

  /**
   * Initiates a request loop that will poll executors for state updates.
   *
   * @param pollInterval Interval on which to poll executors.
   * @param requestHandler Callback to send update requests.
   */
  void startRequestLoop(Amount<Long, Time> pollInterval, Closure<UpdateRequest> requestHandler);

  public static class UpdateRequest {
    public final ExecutorKey executor;
    public final StateUpdateRequest request;

    private UpdateRequest(@Nullable ExecutorKey executor, @Nullable StateUpdateRequest request) {
      this.executor = executor;
      this.request = request;
    }

    public boolean isNone() {
      return executor == null;
    }

    static UpdateRequest none() {
      return new UpdateRequest(null, null);
    }
  }

  /**
   * Watchdog that polls new executors first, and otherwise polls round-robin.
   *
   * TODO(wfarner): Integrate with {@link com.twitter.mesos.scheduler.PulseMonitor}.
   */
  static class ExecutorWatchdogImpl implements ExecutorWatchdog {

    private static final Amount<Long, Time> SHUTDOWN_GRACE_PERIOD = Amount.of(1L, Time.SECONDS);

    private static final int MAP_INITIAL_SIZE = 100;
    private static final float MAP_LOAD_FACTOR = 0.75F;

    @VisibleForTesting
    static final StateUpdateRequest NO_POSITION = new StateUpdateRequest(null, 0);

    private final Map<ExecutorKey, StateUpdateRequest> knownPositions =
        new LinkedHashMap<ExecutorKey, StateUpdateRequest>(MAP_INITIAL_SIZE, MAP_LOAD_FACTOR,
            true /* access order */);
    private final Supplier<Set<ExecutorKey>> knownExecutorSupplier;
    private final ShutdownRegistry shutdownRegistry;

    @Inject
    ExecutorWatchdogImpl(Supplier<Set<ExecutorKey>> knownExecutorSupplier,
        ShutdownRegistry shutdownRegistry) {
      this.shutdownRegistry = checkNotNull(shutdownRegistry);
      this.knownExecutorSupplier = checkNotNull(knownExecutorSupplier);
    }

    @Override
    public void startRequestLoop(Amount<Long, Time> pollInterval,
        final Closure<UpdateRequest> requestHandler) {
      final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1,
          new ThreadFactoryBuilder()
              .setNameFormat("ExecutorStatePoller-%d")
              .setDaemon(true)
              .build());

      Runnable poller = new Runnable() {
        @Override public void run() {
          UpdateRequest request = getNextUpdateRequest(knownExecutorSupplier.get());
          if (!request.isNone()) {
            requestHandler.execute(request);
          }
        }
      };

      executor.scheduleAtFixedRate(poller, pollInterval.getValue(), pollInterval.getValue(),
          pollInterval.getUnit().getTimeUnit());

      shutdownRegistry.addAction(new Command() {
        @Override public void execute() {
          new ExecutorServiceShutdown(executor, SHUTDOWN_GRACE_PERIOD);
        }
      });
    }

    @Override
    public synchronized void stateUpdated(ExecutorKey executor, String executorUUID,
        int position) {
      knownPositions.put(executor, new StateUpdateRequest(executorUUID, position));
    }

    @VisibleForTesting
    synchronized UpdateRequest getNextUpdateRequest(Set<ExecutorKey> executors) {
      // Give priority to any executors that we have no record of (new executors).
      Set<ExecutorKey> newExecutors =
          Sets.difference(executors, ImmutableSet.copyOf(knownPositions.keySet()));
      ExecutorKey request;
      if (!newExecutors.isEmpty()) {
        request = newExecutors.iterator().next();
        knownPositions.put(request, NO_POSITION);
      } else if (!knownPositions.isEmpty()) {
        request = knownPositions.keySet().iterator().next();
      } else {
        return UpdateRequest.none();
      }

      return new UpdateRequest(request, knownPositions.get(request));
    }
  }
}
