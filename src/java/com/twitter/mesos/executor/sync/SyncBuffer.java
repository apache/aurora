package com.twitter.mesos.executor.sync;

import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import com.twitter.common.collections.Pair;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.comm.StateUpdateResponse;
import com.twitter.mesos.gen.comm.TaskStateUpdate;

/**
 * A buffer that collects task state transitions to enable incremental state sync information.
 * The buffer is told the last position that was received by the receiver, which it uses
 * to compute a diff.
 * State transitions are keyed by task ID.  When a diff contains multiple state updates for a task,
 * the latest state update will be returned.
 * As a fallback, the buffer may return a full state update.
 *
 * @author William Farner
 */
public interface SyncBuffer {

  /**
   * Adds a state update to the buffer.
   *
   * @param taskId ID of the task to record state for.
   * @param update Update to apply to the task.
   */
  void add(String taskId, TaskStateUpdate update);

  /**
   * Computes the state transitions that have occurred since a last-known position in the log.
   *
   * @param bufferId Buffer ID that {@code lastKnownPosition} was received from, or {@code null}
   *     if this is the first request.
   * @param lastKnownPosition The last position that was received from this buffer.
   *     If {@code bufferId} is {@code null}, this field is irrelevant.
   * @return A compacted update diff since the last position, or a full update if the
   *     {@code (bufferId, lastKnownPosition)} is not recognized.
   */
  StateUpdateResponse stateSince(@Nullable String bufferId, int lastKnownPosition);

  /**
   * A sync buffer that has a limited size.
   */
  static class SyncBufferImpl implements SyncBuffer {
    private static final Logger LOG = Logger.getLogger(SyncBufferImpl.class.getName());

    private final AtomicLong entriesDropped = Stats.exportLong("sync_buffer_entries_dropped");

    private final String id = UUID.randomUUID().toString();
    private final Queue<Pair<String, TaskStateUpdate>> buffer = Lists.newLinkedList();

    private final int maxSize;
    private final Supplier<Map<String, ScheduleStatus>> fullStatusAccessor;

    private boolean initialized = false;
    private int highPosition = 0;

    /**
     * Creates a new sync buffer that with a fallback accessor for gathering full state.
     *
     * @param maxSize The maximum number of state changes to store in the buffer before beginning
     *     to automatically truncate.
     * @param fullStatusAccessor Fallback accessor for retrieving a full collection of state
     *     information when an unrecognized position is requested, or a position is requested from
     *     a different buffer.
     */
    @Inject
    SyncBufferImpl(@SyncBufferSize int maxSize,
        Supplier<Map<String, ScheduleStatus>> fullStatusAccessor) {
      Preconditions.checkArgument(maxSize > 0, "maxSize must be positive.");
      this.maxSize = maxSize;
      this.fullStatusAccessor = Preconditions.checkNotNull(fullStatusAccessor);

      Stats.exportSize("sync_buffer_size", buffer);
    }

    @Override
    public synchronized void add(String taskId, TaskStateUpdate update) {
      initialized = true;
      while (buffer.size() >= maxSize) {
        buffer.remove();
        LOG.log(Level.WARNING, "Buffer size limit reached, expunging oldest records.");
        entriesDropped.incrementAndGet();
      }

      buffer.add(Pair.of(taskId, update));
      highPosition += 1;
    }

    @Override
    public synchronized StateUpdateResponse stateSince(String bufferId, int position) {
      LOG.info(String.format("Computing state log for %s since %d", bufferId, position));

      // We always notify the reciever of our current ID, and the latest position for the
      // information sent.
      StateUpdateResponse response = new StateUpdateResponse()
          .setExecutorUUID(id)
          .setPosition(highPosition);

      Map<String, TaskStateUpdate> state = Maps.newHashMap();
      if (!id.equals(bufferId) || !hasEntriesAfter(position)) {

        // The position is unrecognized.  Send the full state.
        response.setIncrementalUpdate(false);

        for (Map.Entry<String, ScheduleStatus> entry : fullStatusAccessor.get().entrySet()) {
          state.put(entry.getKey(), new TaskStateUpdate().setStatus(entry.getValue()));
        }
      } else {

        // The receiver successfully acknowledged an update.  Send a diff and truncate entries
        // up to the acknowledged position.
        response.setIncrementalUpdate(true);
        truncateEntries(position - (highPosition - buffer.size()));

        for (Pair<String, TaskStateUpdate> update : buffer) {
          state.put(update.getFirst(), update.getSecond());
        }
      }

      initialized = true;
      return response.setState(state);
    }

    private boolean hasEntriesAfter(int position) {
      return initialized &&
          (position >= (highPosition - buffer.size())) && (position <= highPosition);
    }

    private void truncateEntries(int count) {
      for (int i = 0; i < count; i++) {
        buffer.remove();
      }
    }
  }
}
