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
package org.apache.aurora.scheduler.http;

import java.lang.Thread.UncaughtExceptionHandler;

import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.testing.easymock.EasyMockTest;

import org.junit.Before;
import org.junit.Test;

public class QuitCallbackTest extends EasyMockTest {

  private Command shutdownCommand;
  private Runnable handler;

  @Before
  public void setUp() {
    shutdownCommand = createMock(Command.class);
    handler = new QuitCallback(
        new Lifecycle(shutdownCommand, createMock(UncaughtExceptionHandler.class)));
  }

  @Test
  public void testInvoke() {
    shutdownCommand.execute();

    control.replay();

    handler.run();
  }
}
