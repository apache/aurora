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
package org.apache.aurora.scheduler.state;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.twitter.common.base.Closure;

import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.Storage.Work;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wrapper around the persistent storage and mutable state.
 */
class SideEffectStorage {

  private final Queue<PubsubEvent> events = Lists.newLinkedList();
  @VisibleForTesting
  Queue<PubsubEvent> getEvents() {
    return events;
  }

  private AtomicBoolean inOperation = new AtomicBoolean(false);

  private final Storage storage;
  private final OperationFinalizer operationFinalizer;
  private final Closure<PubsubEvent> taskEventSink;

  interface OperationFinalizer {
    /**
     * Performs any work necessary to complete the operation.
     * This is executed in the context of a write operation, immediately after the work
     * executes normally.
     * NOTE: At present, this is executed for every nesting level of operations, rather than
     * at the completion of the top-level operation.
     * See comment in {@link #SideEffectStorage#executeSideEffectsAfter(SideEffectWork)}
     * for more detail.
     *
     * @param work Work to finalize.
     * @param storeProvider Mutable store reference.
     */
    void finalize(SideEffectWork<?, ?> work, MutableStoreProvider storeProvider);
  }

  SideEffectStorage(
      Storage storage,
      OperationFinalizer operationFinalizer,
      Closure<PubsubEvent> taskEventSink) {

    this.storage = checkNotNull(storage);
    this.operationFinalizer = checkNotNull(operationFinalizer);
    this.taskEventSink = checkNotNull(taskEventSink);
  }

  /**
   * Perform a unit of work in a mutating operation.  This supports nesting/reentrancy.
   *
   * @param work Work to perform.
   * @param <T> Work return type
   * @param <E> Work exception type.
   * @return The work return value.
   * @throws E The work exception.
   */
  <T, E extends Exception> T write(SideEffectWork<T, E> work) throws E {
    return storage.write(executeSideEffectsAfter(work));
  }

  <T, E extends Exception> T consistentRead(Work<T, E> work) throws StorageException, E {
    return storage.consistentRead(work);
  }

  /**
   * Work that has side effects external to the storage system.
   * Work may add side effect and pubsub events, which will be executed/sent upon normal
   * completion of the operation.
   *
   * @param <T> Work return type.
   * @param <E> Work exception type.
   */
  abstract class SideEffectWork<T, E extends Exception> implements MutateWork<T, E> {
    protected final void addTaskEvent(PubsubEvent notice) {
      Preconditions.checkState(inOperation.get());
      events.add(Preconditions.checkNotNull(notice));
    }
  }

  /**
   * Work with side effects which does not throw checked exceptions.
   *
   * @param <T>   Work return type.
   */
  abstract class QuietSideEffectWork<T> extends SideEffectWork<T, RuntimeException> {
  }

  /**
   * Work with side effects that does not have a return value.
   *
   * @param <E> Work exception type.
   */
  abstract class NoResultSideEffectWork<E extends Exception> extends SideEffectWork<Void, E> {

    @Override public final Void apply(MutableStoreProvider storeProvider) throws E {
      execute(storeProvider);
      return null;
    }

    abstract void execute(MutableStoreProvider storeProvider) throws E;
  }

  /**
   * Work with side effects which does not throw checked exceptions or have a return
   * value.
   */
  abstract class NoResultQuietSideEffectWork extends NoResultSideEffectWork<RuntimeException> {
  }

  private <T, E extends Exception> MutateWork<T, E> executeSideEffectsAfter(
      final SideEffectWork<T, E> work) {

    return new MutateWork<T, E>() {
      @Override public T apply(MutableStoreProvider storeProvider) throws E {
        boolean topLevelOperation = inOperation.compareAndSet(false, true);

        try {
          T result = work.apply(storeProvider);

          // TODO(William Farner): Maintaining this since it matches prior behavior, but this
          // seems wrong.  Double-check whether this is necessary, or if only the top-level
          // operation should be executing the finalizer.  Update doc on OperationFinalizer
          // once this is assessed.
          operationFinalizer.finalize(work, storeProvider);
          if (topLevelOperation) {
            while (!events.isEmpty()) {
              taskEventSink.execute(events.remove());
            }
          }
          return result;
        } finally {
          if (topLevelOperation) {
            inOperation.set(false);
          }
        }
      }
    };
  }
}
