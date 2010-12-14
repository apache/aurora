package com.twitter.mesos.updater;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.twitter.common.util.StateMachine;
import com.twitter.mesos.gen.UpdateConfig;
import com.twitter.mesos.updater.ConfigParser.UpdateConfigException;

import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;
import static com.twitter.mesos.updater.UpdateLogic.State.*;
import static com.twitter.mesos.updater.UpdateLogic.UpdateCommand.Type.ROLLBACK_TASK;
import static com.twitter.mesos.updater.UpdateLogic.UpdateCommand.Type.UPDATE_TASK;

/**
 * Abstract logic for the mesos updater.
 *
 * @author wfarner
 */
public class UpdateLogic {

  private static final Logger LOG = Logger.getLogger(UpdateLogic.class.getName());

  private final Function<UpdateCommand, Map<Integer, Boolean>> commandRunner;
  private final Set<Integer> updateTasks;
  private final UpdateConfig config;

  static class UpdateCommand {
    static enum Type {
      UPDATE_TASK,
      ROLLBACK_TASK
    }

    final Type type;
    final Set<Integer> ids;
    final int updateWatchSecs;

    UpdateCommand(Type type, Set<Integer> ids, int updateWatchSecs) {
      this.type = type;
      this.ids = ids;
      this.updateWatchSecs = updateWatchSecs;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof UpdateCommand)) return false;
      UpdateCommand that = (UpdateCommand) o;
      return this.type == that.type && this.ids.equals(that.ids)
             && this.updateWatchSecs == that.updateWatchSecs;
    }

    @Override
    public String toString() {
      return "UpdateCommand{" + "type=" + type + ", ids=" + ids + ", updateWatchSecs="
             + updateWatchSecs + '}';
    }
  }

  /**
   * Creates a new update logic.
   *
   * TODO(wfarner): During a rollback, need to get the updated set of shard IDs since the update
   *    may have added shard IDs.
   *
   * TODO(wfarner): Need to make sure to issue a kill command to shards that are being removed.
   *
   */
  public UpdateLogic(Set<Integer> updateTasks, UpdateConfig config,
      Function<UpdateCommand, Map<Integer, Boolean>> commandRunner) throws UpdateConfigException {
    ConfigParser.parseAndVerify(config);

    this.updateTasks = checkNotBlank(updateTasks);
    this.config = checkNotNull(config);
    this.commandRunner = checkNotNull(commandRunner);
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

  // An iterable that will consume from a queue of tasks being operated on in the current stage.
  // Be careful when reading from this, since the objects are removed.
  Iterable<Integer> consumingIds;

  int totalFailures = 0;

  private void step(StateMachine<State> stateMachine) throws InterruptedException {
    switch (stateMachine.getState()) {
      case INIT:
        LOG.info("Initiating update.");
        consumingIds = Iterables.consumingIterable(new PriorityQueue<Integer>(updateTasks));
        stateMachine.transition(CANARY);
        break;

      case CANARY:
        int failures = restartTasks(UPDATE_TASK, config.getCanaryTaskCount(),
            config.getCanaryWatchDurationSecs());
        if (failures > config.getToleratedCanaryFailures()) {
          stateMachine.transition(PREPARE_ROLLBACK);
        } else {
          stateMachine.transition(UPDATE_BATCH);
        }
        break;

      case UPDATE_BATCH:
        if (Iterables.isEmpty(consumingIds)) {
          stateMachine.transition(COMPLETE_SUCCESS);
        } else {
          restartTasks(UPDATE_TASK, config.getUpdateBatchSize(),
              config.getUpdateWatchDurationSecs());
          if (totalFailures > config.getToleratedUpdateFailures()) {
            stateMachine.transition(PREPARE_ROLLBACK);
          }
        }
        break;

      case PREPARE_ROLLBACK:
        LOG.info("Preparing for rollback.");
        consumingIds = Iterables.consumingIterable(new PriorityQueue<Integer>(
            Sets.difference(updateTasks, ImmutableSet.copyOf(consumingIds))));
        stateMachine.transition(ROLLBACK_BATCH);
        break;

      case ROLLBACK_BATCH:
        if (Iterables.isEmpty(consumingIds)) {
          stateMachine.transition(COMPLETE_FAILED);
        } else {
          // TODO(wfarner): Can/should we do anything if this fails?
          restartTasks(ROLLBACK_TASK, config.getUpdateBatchSize(),
              config.getUpdateWatchDurationSecs());
        }
        break;

      default:
        throw new RuntimeException("Unhandled state " + stateMachine.getState());
    }
  }

  /**
   * Performs a restart on a batch of tasks, and updates the total failure count.
   *
   * @param type The type of update to issue.
   * @param numTasks Number of tasks to pull from the queue.
   * @param updateWatchSecs Duration that the task must survive to be considered a success.
   * @return The number of tasks that did not survive the watch period.
   */
  private int restartTasks(UpdateCommand.Type type, int numTasks, int updateWatchSecs) {
    Set<Integer> ids = ImmutableSet.copyOf(Iterables.limit(consumingIds, numTasks));
    LOG.info(String.format("Issuing %s on tasks %s", type, ids));

    // TODO(wfarner): Also supply the number of remaining tolerated failures so that a batch of
    // 10 can fail quickly if only one tolerated failure remains.
    Map<Integer, Boolean> results = commandRunner.apply(
        new UpdateCommand(type, ids, updateWatchSecs));

    int failures = Iterables.size(Iterables.filter(results.values(), Predicates.equalTo(false)));
    totalFailures += failures;
    return failures;
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
