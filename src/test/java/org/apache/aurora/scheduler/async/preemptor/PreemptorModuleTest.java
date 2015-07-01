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
package org.apache.aurora.scheduler.async.preemptor;

import com.google.common.base.Optional;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.twitter.common.application.StartupStage;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.scheduler.filter.AttributeAggregate;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.TaskAssigner;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PreemptorModuleTest extends EasyMockTest {

  private StorageTestUtil storageUtil;

  @Before
  public void setUp() {
    storageUtil = new StorageTestUtil(this);
    storageUtil.expectOperations();
  }

  private Injector createInjector(Module module) {
    return Guice.createInjector(
        module,
        new LifecycleModule(),
        new AbstractModule() {
          private <T> void bindMock(Class<T> clazz) {
            bind(clazz).toInstance(createMock(clazz));
          }

          @Override
          protected void configure() {
            bindMock(SchedulingFilter.class);
            bindMock(StateManager.class);
            bindMock(TaskAssigner.class);
            bindMock(Thread.UncaughtExceptionHandler.class);
            bind(Storage.class).toInstance(storageUtil.storage);
          }
        });
  }

  @Test
  public void testPreemptorDisabled() throws Exception {
    Injector injector = createInjector(new PreemptorModule(
        false,
        Amount.of(0L, Time.SECONDS),
        Amount.of(0L, Time.SECONDS)));

    control.replay();

    injector.getInstance(Key.get(ExceptionalCommand.class, StartupStage.class)).execute();

    injector.getBindings();
    assertEquals(
        Optional.absent(),
        injector.getInstance(Preemptor.class).attemptPreemptionFor(
            IAssignedTask.build(new AssignedTask()),
            AttributeAggregate.EMPTY,
            storageUtil.mutableStoreProvider));
  }
}
