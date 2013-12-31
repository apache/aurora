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
package org.apache.aurora.scheduler.async;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import com.twitter.common.base.Function;
import com.twitter.common.util.BackoffStrategy;

import org.apache.aurora.scheduler.async.TaskGroups.GroupKey;

/**
 * A group of task IDs that are eligible for scheduling, but may be waiting for a backoff to expire.
 */
class TaskGroup {
  private final GroupKey key;
  private final BackoffStrategy backoffStrategy;

  private static final Function<Task, Long> TO_TIMESTAMP = new Function<Task, Long>() {
    @Override public Long apply(Task item) {
      return item.readyTimestampMs;
    }
  };

  // Order the tasks by the time they are ready to be scheduled
  private static final Ordering<Task> TASK_ORDERING = Ordering.natural().onResultOf(TO_TIMESTAMP);
  // 11 is the magic number used by PriorityBlockingQueue as the initial size.
  private final Queue<Task> tasks = new PriorityBlockingQueue<>(11, TASK_ORDERING);
  // Penalty for the task group for failing to schedule.
  private final AtomicLong penaltyMs;

  TaskGroup(GroupKey key, BackoffStrategy backoffStrategy) {
    this.key = key;
    this.backoffStrategy = backoffStrategy;
    penaltyMs = new AtomicLong();
    resetPenaltyAndGet();
  }

  GroupKey getKey() {
    return key;
  }

  private static final Function<Task, String> TO_TASK_ID =
      new Function<Task, String>() {
        @Override public String apply(Task item) {
          return item.taskId;
        }
      };

  /**
   * Removes the task at the head of the queue.
   *
   * @return String the id of the head task.
   * @throws IllegalStateException if the queue is empty.
   */
  String pop() throws IllegalStateException {
    Task head = tasks.poll();
    Preconditions.checkState(head != null);
    return head.taskId;
  }

  void remove(String taskId) {
    Iterables.removeIf(tasks, Predicates.compose(Predicates.equalTo(taskId), TO_TASK_ID));
  }

  void push(final String taskId, long readyTimestamp) {
    tasks.offer(new Task(taskId, readyTimestamp));
  }

  synchronized long resetPenaltyAndGet() {
    penaltyMs.set(backoffStrategy.calculateBackoffMs(0));
    return getPenaltyMs();
  }

  synchronized long penalizeAndGet() {
    penaltyMs.set(backoffStrategy.calculateBackoffMs(getPenaltyMs()));
    return getPenaltyMs();
  }

  GroupState isReady(long nowMs) {
    Task task = tasks.peek();
    if (task == null) {
      return GroupState.EMPTY;
    }

    if (task.readyTimestampMs > nowMs) {
      return GroupState.NOT_READY;
    }
    return GroupState.READY;
  }
  // Begin methods used for debug interfaces.

  public String getName() {
    return key.toString();
  }

  // TODO(zmanji): Return Task instances here. Can use them to display flapping penalty on web UI.
  public Set<String> getTaskIds() {
    return ImmutableSet.copyOf(Iterables.transform(tasks, TO_TASK_ID));
  }

  public long getPenaltyMs() {
    return penaltyMs.get();
  }

  private static class Task {
    private final String taskId;
    private final long readyTimestampMs;

    Task(String taskId, long readyTimestampMs) {
      this.taskId = Preconditions.checkNotNull(taskId);
      this.readyTimestampMs = readyTimestampMs;
    }
  }

  enum GroupState {
    EMPTY,      // The group is empty.
    NOT_READY,  // Every task in the group is not ready yet.
    READY       // The task at the head of the queue is ready.
  }
}
