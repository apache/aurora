package com.twitter.mesos.scheduler;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.twitter.common.base.Closure;
import com.twitter.mesos.scheduler.StateManagerVars.MutableState;
import com.twitter.mesos.scheduler.events.PubsubEvent;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transactional wrapper around the persistent storage and mutable state.
 */
class TransactionalStorage {
  private boolean inTransaction = false;
  private final List<SideEffect> sideEffects = Lists.newLinkedList();
  private final List<PubsubEvent> events = Lists.newLinkedList();

  private final Storage storage;
  private final MutableState mutableState;
  private final Closure<MutableStoreProvider> transactionFinalizer;
  private final Closure<PubsubEvent> taskEventSink;

  TransactionalStorage(
      Storage storage,
      MutableState mutableState,
      Closure<MutableStoreProvider> transactionFinalizer,
      Closure<PubsubEvent> taskEventSink) {

    this.storage = checkNotNull(storage);
    this.mutableState = checkNotNull(mutableState);
    this.transactionFinalizer = checkNotNull(transactionFinalizer);
    this.taskEventSink = checkNotNull(taskEventSink);
  }

  void addSideEffect(SideEffect sideEffect) {
    Preconditions.checkState(inTransaction);
    sideEffects.add(Preconditions.checkNotNull(sideEffect));
  }

  void addTaskEvent(PubsubEvent notice) {
    Preconditions.checkState(inTransaction);
    events.add(Preconditions.checkNotNull(notice));
  }

  private void clearTransactionState() {
    inTransaction = false;
    sideEffects.clear();
    events.clear();
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
  synchronized <T, E extends Exception> T doInWriteTransaction(MutateWork<T, E> work) throws E {
    if (inTransaction) {
      return execute(work);
    }

    try {
      inTransaction = true;
      T result = execute(work);
      executeSideEffects();
      sendPubsubEvents();
      return result;
    } finally {
      clearTransactionState();
    }
  }

  /**
   * Starts the storage and executes a transaction.
   *
   * @param work Work to execute.
   */
  void start(MutateWork.NoResult.Quiet work) {
    Preconditions.checkState(!inTransaction);

    try {
      inTransaction = true;
      executeStart(work);
      executeSideEffects();
      sendPubsubEvents();
    } finally {
      clearTransactionState();
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
  <T, E extends Exception> T execute(final MutateWork<T, E> work) throws E {
    return storage.doInWriteTransaction(new MutateWork<T, E>() {
      @Override public T apply(MutableStoreProvider storeProvider) throws E {
        T result = work.apply(storeProvider);
        transactionFinalizer.execute(storeProvider);
        return result;
      }
    });
  }

  private void executeStart(final MutateWork.NoResult.Quiet work) {
    storage.start(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
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

  private void sendPubsubEvents() {
    for (PubsubEvent event : events) {
      taskEventSink.execute(event);
    }
  }

  /**
   * Side effects are modifications of the internal state.
   */
  interface SideEffect {
    void mutate(MutableState state);
  }
}
