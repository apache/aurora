/**
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
package org.apache.aurora.scheduler.storage;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.inject.Inject;
import javax.inject.Qualifier;
import javax.inject.Singleton;

import com.google.inject.Module;
import com.google.inject.PrivateModule;

import org.apache.aurora.common.util.StateMachine;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult.Quiet;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

import static java.util.Objects.requireNonNull;

/**
 * A non-volatile storage wrapper that enforces method call ordering.
 */
public class CallOrderEnforcingStorage implements NonVolatileStorage {

  /**
   * Identifies a storage whose call order should be enforced.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.PARAMETER, ElementType.METHOD })
  @Qualifier
  private @interface EnforceOrderOn { }

  private final NonVolatileStorage wrapped;
  private final EventSink eventSink;

  private enum State {
    CONSTRUCTED,
    PREPARED,
    READY,
    STOPPED
  }

  private final StateMachine<State> stateMachine = StateMachine.<State>builder("storage")
      .logTransitions()
      .initialState(State.CONSTRUCTED)
      .addState(
          State.CONSTRUCTED,
          State.PREPARED, State.STOPPED)
      .addState(
          State.PREPARED,
          State.READY, State.STOPPED)
      .addState(
          State.READY,
          State.STOPPED)
      .addState(
          State.STOPPED,
          // Allow cycles in STOPPED to prevent throwing and avoid the need for call-site checking.
          State.STOPPED)
      .build();

  @Inject
  CallOrderEnforcingStorage(@EnforceOrderOn NonVolatileStorage wrapped, EventSink eventSink) {
    this.wrapped = requireNonNull(wrapped);
    this.eventSink = requireNonNull(eventSink);
  }

  private void checkState(State state) throws StorageException {
    try {
      stateMachine.checkState(state);
    } catch (IllegalStateException e) {
      throw new TransientStorageException("Storage is not " + state, e);
    }
  }

  @Override
  public void prepare() throws StorageException {
    checkState(State.CONSTRUCTED);
    wrapped.prepare();
    stateMachine.transition(State.PREPARED);
  }

  @Override
  public void start(Quiet initializationLogic) throws StorageException {
    checkState(State.PREPARED);
    wrapped.start(initializationLogic);
    stateMachine.transition(State.READY);
    wrapped.write((NoResult.Quiet) storeProvider -> {
      Iterable<IScheduledTask> tasks = Tasks.LATEST_ACTIVITY.sortedCopy(
          storeProvider.getTaskStore().fetchTasks(Query.unscoped()));
      for (IScheduledTask task : tasks) {
        eventSink.post(TaskStateChange.initialized(task));
      }
    });
  }

  @Override
  public void stop() {
    wrapped.stop();
    stateMachine.transition(State.STOPPED);
  }

  @Override
  public <T, E extends Exception> T read(Work<T, E> work) throws StorageException, E {
    checkState(State.READY);
    return wrapped.read(work);
  }

  @Override
  public <T, E extends Exception> T write(MutateWork<T, E> work)
      throws StorageException, E {
    checkState(State.READY);
    return wrapped.write(work);
  }

  /**
   * Creates a binding module that will wrap a storage class with {@link CallOrderEnforcingStorage},
   * exposing the order-enforced storage as {@link Storage} and {@link NonVolatileStorage}.
   *
   * @param storageClass Non-volatile storage implementation class.
   * @return Binding module.
   */
  public static Module wrappingModule(final Class<? extends NonVolatileStorage> storageClass) {
    return new PrivateModule() {
      @Override
      protected void configure() {
        bind(Storage.class).to(CallOrderEnforcingStorage.class);
        bind(NonVolatileStorage.class).to(CallOrderEnforcingStorage.class);
        bind(CallOrderEnforcingStorage.class).in(Singleton.class);
        bind(NonVolatileStorage.class).annotatedWith(EnforceOrderOn.class).to(storageClass);
        expose(Storage.class);
        expose(NonVolatileStorage.class);
      }
    };
  }
}
