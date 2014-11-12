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
package org.apache.aurora.scheduler.events;

import java.util.logging.Logger;

import com.google.common.eventbus.EventBus;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.junit.Before;
import org.junit.Test;

public class PubsubEventModuleTest extends EasyMockTest {

  private Logger logger;
  private Injector injector;

  @Before
  public void setUp() {
    logger = createMock(Logger.class);
    injector = Guice.createInjector(
        new PubsubEventModule(false, logger),
        new AbstractModule() {
          @Override
          protected void configure() {
            PubsubEventModule.bindSchedulingFilterDelegate(binder())
                .toInstance(createMock(SchedulingFilter.class));
          }
        });
  }

  @Test
  public void testHandlesDeadEvent() {
    logger.warning(String.format(PubsubEventModule.DEAD_EVENT_MESSAGE, "hello"));

    control.replay();

    injector.getInstance(EventBus.class).post("hello");
  }
}
