package com.twitter.mesos.scheduler.async;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Queues;

import com.twitter.common.util.BackoffStrategy;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.scheduler.async.TaskGroups.GroupKey;
import com.twitter.mesos.scheduler.base.JobKeys;

/**
 * A group of task IDs that are eligible for scheduling, but may be waiting for a backoff to expire.
 */
class TaskGroup {
  final GroupKey key;
  private final BackoffStrategy backoffStrategy;
  private final Queue<String> taskIds = Queues.newLinkedBlockingQueue();
  private final AtomicLong penaltyMs;

  TaskGroup(GroupKey key, BackoffStrategy backoffStrategy) {
    this.key = key;
    this.backoffStrategy = backoffStrategy;
    penaltyMs = new AtomicLong();
    resetPenaltyAndGet();
  }

  @Nullable
  String pop() {
    return taskIds.poll();
  }

  void remove(String taskId) {
    taskIds.remove(taskId);
  }

  void push(final String taskId) {
    taskIds.offer(taskId);
  }

  synchronized long resetPenaltyAndGet() {
    penaltyMs.set(backoffStrategy.calculateBackoffMs(0));
    return getPenaltyMs();
  }

  synchronized long penalizeAndGet() {
    penaltyMs.set(backoffStrategy.calculateBackoffMs(getPenaltyMs()));
    return getPenaltyMs();
  }

  // Begin methods used for debug interfaces.

  public String getName() {
    return JobKeys.toPath(Tasks.INFO_TO_JOB_KEY.apply(key.scrubbedCanonicalTask));
  }

  public Set<String> getTaskIds() {
    return ImmutableSet.copyOf(taskIds);
  }

  public long getPenaltyMs() {
    return penaltyMs.get();
  }
}
