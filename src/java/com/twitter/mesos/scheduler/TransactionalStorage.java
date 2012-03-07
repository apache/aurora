package com.twitter.mesos.scheduler;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.twitter.common.base.Closure;
import com.twitter.mesos.scheduler.StateManagerVars.MutableState;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transactional wrapper around the persistent storage and mutable state.
 */
class TransactionalStorage {
  private boolean inTransaction = false;
  private final List<SideEffect> sideEffects = Lists.newLinkedList();

  private final Storage storage;
  private final MutableState mutableState;
  private final Closure<StoreProvider> transactionFinalizer;

  TransactionalStorage(Storage storage, MutableState mutableState,
      Closure<StoreProvider> transactionFinalizer) {

    this.storage = checkNotNull(storage);
    this.mutableState = checkNotNull(mutableState);
    this.transactionFinalizer = checkNotNull(transactionFinalizer);
  }

  void addSideEffect(SideEffect sideEffect) {
    Preconditions.checkState(inTransaction);
    sideEffects.add(sideEffect);
  }

  /**
   * Perform a unit of work in a transaction.  This supports nesting/reentrancy.
   *
   * Note: It is not strictly necessary for this method to be synchronized, provided that calling
   * code in StateManager is also properly synchronized.  However, we acquire an additional lock
   * here as a safeguard in the event that a new package-visible unsynchronized method is added.
   *
   * @param work Work to perform.
   * @param <T> Work return type
   * @param <E> Work exception type.
   * @return The work return value.
   * @throws E The work exception.
   */
  synchronized <T, E extends Exception> T doInTransaction(Work<T, E> work) throws E {
    if (inTransaction) {
      return execute(work);
    }

    try {
      inTransaction = true;
      T result = execute(work);
      executeSideEffects();
      return result;
    } finally {
      inTransaction = false;
      sideEffects.clear();
    }
  }

  /**
   * Starts the storage and executes a transaction.
   *
   * @param work Work to execute.
   */
  void start(Work.NoResult.Quiet work) {
    Preconditions.checkState(!inTransaction);

    try {
      inTransaction = true;
      executeStart(work);
      executeSideEffects();
    } finally {
      inTransaction = false;
      sideEffects.clear();
    }
  }

  /**
   * Prepares the storage.
   */
  void prepare() {
    Preconditions.checkState(!inTransaction);
    storage.prepare();
  }

  /**
   * Stops the storage.
   */
  void stop() {
    Preconditions.checkState(!inTransaction);
    storage.stop();
  }

  /**
   * Executes a transaction.
   *
   * @param work Transaction to execute.
   * @param <T> Return type.
   * @param <E> Exception type.
   * @return Return value from the transaction closure.
   * @throws E Exception thrown by transaction.
   */
  <T, E extends Exception> T execute(final Work<T, E> work) throws E {
    return storage.doInTransaction(new Work<T, E>() {
      @Override public T apply(StoreProvider storeProvider) throws E {
        T result = work.apply(storeProvider);
        transactionFinalizer.execute(storeProvider);
        return result;
      }
    });
  }

  private void executeStart(final Work.NoResult.Quiet work) {
    storage.start(new Work.NoResult.Quiet() {
      @Override protected void execute(StoreProvider storeProvider) {
        work.apply(storeProvider);
        transactionFinalizer.execute(storeProvider);
      }
    });
  }

  private void executeSideEffects() {
    for (SideEffect sideEffect : sideEffects) {
      sideEffect.mutate(mutableState);
    }
  }

  /**
   * Side effects are modifications of the internal state.
   */
  interface SideEffect {
    void mutate(MutableState state);
  }
}
