package com.twitter.mesos.updater;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.util.StateMachine;
import com.twitter.mesos.gen.UpdateConfig;

import java.util.Collection;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;
import static com.twitter.mesos.updater.UpdateCommand.Type.ROLLBACK_TASK;
import static com.twitter.mesos.updater.UpdateCommand.Type.UPDATE_TASK;
import static com.twitter.mesos.updater.UpdateLogic.State.CANARY;
import static com.twitter.mesos.updater.UpdateLogic.State.COMPLETE_FAILED;
import static com.twitter.mesos.updater.UpdateLogic.State.COMPLETE_SUCCESS;
import static com.twitter.mesos.updater.UpdateLogic.State.INIT;
import static com.twitter.mesos.updater.UpdateLogic.State.PREPARE_ROLLBACK;
import static com.twitter.mesos.updater.UpdateLogic.State.ROLLBACK_BATCH;
import static com.twitter.mesos.updater.UpdateLogic.State.UPDATE_BATCH;

/**
 * Abstract logic for the mesos updater.
 *
 * @author William Farner
 */
public class UpdateLogic {

  private static final Logger LOG = Logger.getLogger(UpdateLogic.class.getName());

  private final ExceptionalFunction<UpdateCommand, Integer, UpdateException> commandRunner;
  private final Set<Integer> newShards;
  private final UpdateConfig config;
  private final Set<Integer> oldShards;

  /**
   * Creates a new update logic.
   *
   * TODO(William Farner): During a rollback, need to get the updated set of shard IDs since the update
   *    may have added shard IDs.
   *
   * TODO(William Farner): Need to make sure to issue a kill command to shards that are being removed.
   *
   */
  public UpdateLogic(Set<Integer> oldShards, Set<Integer> newShards, UpdateConfig config,
      ExceptionalFunction<UpdateCommand, Integer, UpdateException> commandRunner) {
    this.oldShards = checkNotBlank(oldShards);
    this.newShards = checkNotBlank(newShards);
    this.config = checkNotNull(config);
    this.commandRunner = checkNotNull(commandRunner);
  }

  public static class UpdateException extends Exception {
    public UpdateException(String msg) {
      super(msg);
    }
    public UpdateException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  /**
   * Runs the update.
   *
   * @return {@code true} if the update completed successfully or {@code false} if it failed.
   * @throws InterruptedException If interrupted while updating.
   */
  public boolean run() throws InterruptedException {
    StateMachine<State> stateMachine = createStateMachine();

    while (!TERMINAL_STATES.contains(stateMachine.getState())) {
      step(stateMachine);
    }

    return stateMachine.getState() == COMPLETE_SUCCESS;
  }


  private static class ConsumableSortedIds {
    private final Collection<Integer> sortedIds;
    private final Iterable<Integer> consumingIds;

    private ConsumableSortedIds(final Set<Integer> ids) {
      sortedIds = new PriorityQueue<Integer>(ids);
      consumingIds = Iterables.consumingIterable(sortedIds);
    }

    /**
     * Returns an iterable over sorted ids that will consume ids as they are iterated over.  When
     * all ids have been iterated over once, {@link #isEmpty()} will return {@code true}.
     *
     * @return an consuming {@code Iterable} over the sorted shard ids
     */
    public Iterable<Integer> get() {
      return consumingIds;
    }

    /**
     * @return {@code true} iff the Iterable returned by {@link #get()} has been fully iterated.
     */
    public boolean isEmpty() {
      return sortedIds.isEmpty();
    }
  }


  // An iterable that will consume from a queue of shards being operated on in the current stage.
  // Be careful when reading from this, since the objects are removed.
  private ConsumableSortedIds consumingIds;

  int totalFailures = 0;

  private void step(StateMachine<State> stateMachine) throws InterruptedException {
    try {
      switch (stateMachine.getState()) {
        case INIT:
          LOG.info("Initiating update.");
          consumingIds = new ConsumableSortedIds(newShards);
          stateMachine.transition(CANARY);
          break;

        case CANARY:
          int failures = restartShards(UPDATE_TASK, config.getCanaryTaskCount(),
              config.getToleratedCanaryFailures() - totalFailures,
                config.getCanaryWatchDurationSecs());
          if (failures > config.getToleratedCanaryFailures()) {
            stateMachine.transition(PREPARE_ROLLBACK);
          } else {
            stateMachine.transition(UPDATE_BATCH);
          }
          break;

        case UPDATE_BATCH:
          if (consumingIds.isEmpty()) {
            stateMachine.transition(COMPLETE_SUCCESS);
          } else {
            restartShards(UPDATE_TASK, config.getUpdateBatchSize(),
                config.getToleratedUpdateFailures() - totalFailures,
                config.getUpdateWatchDurationSecs());
            if (totalFailures > config.getToleratedUpdateFailures()) {
              stateMachine.transition(PREPARE_ROLLBACK);
            }
          }
          break;

        case PREPARE_ROLLBACK:
          LOG.info("Preparing for rollback.");
          consumingIds = new ConsumableSortedIds(Sets.difference(oldShards,
              ImmutableSet.copyOf(consumingIds.get())));
          stateMachine.transition(ROLLBACK_BATCH);
          break;

        case ROLLBACK_BATCH:
          if (consumingIds.isEmpty()) {
            stateMachine.transition(COMPLETE_FAILED);
          } else {
            // TODO(William Farner): Can/should we do anything if this fails?
            restartShards(ROLLBACK_TASK, config.getUpdateBatchSize(), Integer.MAX_VALUE,
                config.getUpdateWatchDurationSecs());
          }
          break;

        default:
          throw new IllegalStateException("Unhandled state " + stateMachine.getState());
      }
    } catch (UpdateException e) {
      LOG.log(Level.SEVERE, "Update failed.", e);
    }
  }

  /**
   * Performs a restart on a batch of shards, and updates the total failure count.
   *
   * @param type The type of update to issue.
   * @param numShards Number of shards to pull from the queue.
   * @param toleratedFailures The number of tolerated failures in this batch.
   * @param updateWatchDurationSecs The number of seconds that new tasks must survive to be
   *    considered successfully updated.
   * @return The number of shards that did not survive the watch period.
   * @throws UpdateException If the restart attempt failed.
   */
  private int restartShards(UpdateCommand.Type type, int numShards, int toleratedFailures,
      int updateWatchDurationSecs) throws UpdateException {
    Preconditions.checkArgument(numShards > 0, "Restart of zero shards does not make sense.");

    Set<Integer> ids = ImmutableSet.copyOf(Iterables.limit(consumingIds.get(), numShards));
    LOG.info(String.format("Issuing %s on shards %s", type, ids));

    int newFailures = commandRunner.apply(new UpdateCommand(type, ids,
        config.getRestartTimeoutSecs(), updateWatchDurationSecs, toleratedFailures));

    totalFailures += newFailures;
    return newFailures;
  }

  static enum State {
    INIT,
    CANARY,
    UPDATE_BATCH,
    PREPARE_ROLLBACK,
    ROLLBACK_BATCH,
    COMPLETE_SUCCESS,
    COMPLETE_FAILED
  }

  private static StateMachine<State> createStateMachine() {
    return StateMachine.<State>builder("UpdateLogic")
      .addState(INIT, CANARY, COMPLETE_FAILED)
      .addState(CANARY, UPDATE_BATCH, PREPARE_ROLLBACK, COMPLETE_FAILED)
      .addState(UPDATE_BATCH, UPDATE_BATCH, PREPARE_ROLLBACK, COMPLETE_SUCCESS, COMPLETE_FAILED)
      .addState(PREPARE_ROLLBACK, ROLLBACK_BATCH)
      .addState(ROLLBACK_BATCH, ROLLBACK_BATCH, COMPLETE_FAILED)
      .initialState(INIT)
      .build();
  }

  private static final Set<State> TERMINAL_STATES =
      ImmutableSet.of(COMPLETE_SUCCESS, COMPLETE_FAILED);
}
