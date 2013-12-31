/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.storage;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.inject.BindingAnnotation;
import com.google.inject.Module;
import com.google.inject.PrivateModule;

import com.twitter.common.util.StateMachine;

import org.apache.aurora.scheduler.events.PubsubEvent.Interceptors.Event;
import org.apache.aurora.scheduler.events.PubsubEvent.Interceptors.SendNotification;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult.Quiet;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;

/**
 * A non-volatile storage wrapper that enforces method call ordering.
 */
public class CallOrderEnforcingStorage implements NonVolatileStorage {

  /**
   * Identifies a storage whose call order should be enforced.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.PARAMETER, ElementType.METHOD })
  @BindingAnnotation
  private @interface EnforceOrderOn { }

  private final NonVolatileStorage wrapped;

  private enum State {
    CONSTRUCTED,
    PREPARED,
    READY,
    STOPPED
  }

  private final StateMachine<State> stateMachine = StateMachine.<State>builder("storage")
      .logTransitions()
      .initialState(State.CONSTRUCTED)
      .addState(State.CONSTRUCTED, State.PREPARED)
      .addState(State.PREPARED, State.READY)
      .addState(State.READY, State.STOPPED)
      .build();

  @Inject
  CallOrderEnforcingStorage(@EnforceOrderOn NonVolatileStorage wrapped) {
    this.wrapped = wrapped;
  }

  private void checkInState(State state) throws StorageException {
    if (stateMachine.getState() != state) {
      throw new StorageException("Storage is not " + state);
    }
  }

  @Override
  public void prepare() throws StorageException {
    checkInState(State.CONSTRUCTED);
    wrapped.prepare();
    stateMachine.transition(State.PREPARED);
  }

  @SendNotification(after = Event.StorageStarted)
  @Override
  public void start(Quiet initializationLogic) throws StorageException {
    checkInState(State.PREPARED);
    wrapped.start(initializationLogic);
    stateMachine.transition(State.READY);
  }

  @Override
  public void stop() {
    wrapped.stop();
    stateMachine.transition(State.STOPPED);
  }

  @Override
  public <T, E extends Exception> T consistentRead(Work<T, E> work) throws StorageException, E {
    checkInState(State.READY);
    return wrapped.consistentRead(work);
  }

  @Override
  public <T, E extends Exception> T weaklyConsistentRead(Work<T, E> work)
      throws StorageException, E {

    checkInState(State.READY);
    return wrapped.weaklyConsistentRead(work);
  }

  @Override
  public <T, E extends Exception> T write(MutateWork<T, E> work)
      throws StorageException, E {
    checkInState(State.READY);
    return wrapped.write(work);
  }

  @Override
  public void snapshot() throws StorageException {
    checkInState(State.READY);
    wrapped.snapshot();
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
      @Override protected void configure() {
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
