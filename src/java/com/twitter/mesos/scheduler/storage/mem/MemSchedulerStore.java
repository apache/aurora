package com.twitter.mesos.scheduler.storage.mem;

import javax.annotation.Nullable;

import com.google.common.collect.Maps;

import com.twitter.mesos.scheduler.storage.SchedulerStore;

/**
 * An in-memory scheduler store.
 */
public class MemSchedulerStore implements SchedulerStore.Mutable.Transactioned {
  private static final String FRAMEWORK_ID_KEY = "framework_id";

  private final TransactionalMap<String, String> entries =
      TransactionalMap.wrap(Maps.<String, String>newHashMap());

  @Override
  public void commit() {
    entries.commit();
  }

  @Override
  public void rollback() {
    entries.rollback();
  }

  @Override
  public void saveFrameworkId(String newFrameworkId) {
    entries.put(FRAMEWORK_ID_KEY, newFrameworkId);
  }

  @Nullable
  @Override
  public String fetchFrameworkId() {
    return entries.get(FRAMEWORK_ID_KEY);
  }
}
