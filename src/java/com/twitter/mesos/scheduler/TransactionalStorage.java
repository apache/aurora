package com.twitter.mesos.scheduler;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.twitter.common.base.Closure;
import com.twitter.common.util.Clock;
import com.twitter.mesos.scheduler.events.PubsubEvent;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.StorageBackfill;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transactional wrapper around the persistent storage and mutable state.
 */
class TransactionalStorage {
  @VisibleForTesting
  final Queue<PubsubEvent> events = Lists.newLinkedList();

  private AtomicBoolean inTransaction = new AtomicBoolean(false);

  private final Storage storage;
  private final TransactionFinalizer transactionFinalizer;
  private final Closure<PubsubEvent> taskEventSink;
  private final Clock clock;

  interface TransactionFinalizer {
    /**
     * Performs any work necessary to complete the transaction.
     * This is executed in the context of a write transaction, immediately after the work
     * executes normally.
     * NOTE: At present, this is executed for every nesting level of transactions, rather than
     * at the completion of the top-level transaction.
     * See comment in {@link #TransactionalStorage#executeSideEffectsAfter(SideEffectWork)}
     * for more detail.
     *
     * @param work Work to finalize.
     * @param storeProvider Mutable store reference.
     */
    void finalize(SideEffectWork<?, ?> work, MutableStoreProvider storeProvider);
  }

  TransactionalStorage(
      Storage storage,
      TransactionFinalizer transactionFinalizer,
      Closure<PubsubEvent> taskEventSink,
      Clock clock) {

    this.storage = checkNotNull(storage);
    this.transactionFinalizer = checkNotNull(transactionFinalizer);
    this.taskEventSink = checkNotNull(taskEventSink);
    this.clock = checkNotNull(clock);
  }

  /**
   * Backfills storage structs, ideally this is performed before other storage queries.
   */
  void backfill() {
    storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        StorageBackfill.backfill(storeProvider, clock);
      }
    });
  }

  /**
   * Perform a unit of work in a transaction.  This supports nesting/reentrancy.
   *
   * @param work Work to perform.
   * @param <T> Work return type
   * @param <E> Work exception type.
   * @return The work return value.
   * @throws E The work exception.
   */
  <T, E extends Exception> T doInWriteTransaction(SideEffectWork<T, E> work) throws E {
    return storage.doInWriteTransaction(executeSideEffectsAfter(work));
  }

  /**
   * Transactional work that has side effects external to the storage system.
   * Work may add side effect and pubsub events, which will be executed/sent upon normal
   * completion of the transaction.
   *
   * @param <T> Work return type.
   * @param <E> Work exception type.
   */
  abstract class SideEffectWork<T, E extends Exception> implements MutateWork<T, E> {
    protected final void addTaskEvent(PubsubEvent notice) {
      Preconditions.checkState(inTransaction.get());
      events.add(Preconditions.checkNotNull(notice));
    }
  }

  /**
   * Transactional work with side effects which does not throw checked exceptions.
   *
   * @param <T>   Work return type.
   */
  abstract class QuietSideEffectWork<T> extends SideEffectWork<T, RuntimeException> {
  }

  /**
   * Transactional work with side effects which does not throw checked exceptions or have a return
   * value.
   */
  abstract class NoResultSideEffectWork extends SideEffectWork<Void, RuntimeException> {
    @Override public final Void apply(MutableStoreProvider storeProvider) {
      execute(storeProvider);
      return null;
    }

    abstract void execute(MutableStoreProvider storeProvider);
  }

  /**
   * Starts the storage and executes a transaction.
   *
   * @param work Work to execute.
   */
  void start(final SideEffectWork<Void, RuntimeException> work) {
    Preconditions.checkState(!inTransaction.get());
    storage.start(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        executeSideEffectsAfter(work).apply(storeProvider);
      }
    });
  }

  /**
   * Prepares the storage.
   */
  void prepare() {
    Preconditions.checkState(!inTransaction.get());
    storage.prepare();
  }

  /**
   * Stops the storage.
   */
  void stop() {
    Preconditions.checkState(!inTransaction.get());
    storage.stop();
  }

  private <T, E extends Exception> MutateWork<T, E> executeSideEffectsAfter(
      final SideEffectWork<T, E> work) {

    return new MutateWork<T, E>() {
      @Override public T apply(MutableStoreProvider storeProvider) throws E {
        boolean topLevelTransaction = inTransaction.compareAndSet(false, true);

        try {
          T result = work.apply(storeProvider);

          // TODO(William Farner): Maintaining this since it matches prior behavior, but this
          // seems wrong.  Double-check whether this is necessary, or if only the top-level
          // transaction should be executing the finalizer.  Update doc on TransactionFinalizer
          // once this is assessed.
          transactionFinalizer.finalize(work, storeProvider);
          if (topLevelTransaction) {
            while (!events.isEmpty()) {
              taskEventSink.execute(events.remove());
            }
          }
          return result;
        } finally {
          if (topLevelTransaction) {
            inTransaction.set(false);
          }
        }
      }
    };
  }
}
