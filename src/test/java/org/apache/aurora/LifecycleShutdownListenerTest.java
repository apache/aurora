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
package org.apache.aurora;

import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;

import org.apache.aurora.GuavaUtils.LifecycleShutdownListener;
import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.junit.Before;
import org.junit.Test;

public class LifecycleShutdownListenerTest extends EasyMockTest {

  private static final Service NOOP_SERVICE = new AbstractService() {
    @Override
    protected void doStart() {
      // Noop.
    }

    @Override
    protected void doStop() {
      // Noop.
    }
  };

  private Command shutdown;
  private ServiceManager.Listener listener;

  @Before
  public void setUp() {
    shutdown = createMock(Command.class);
    listener = new LifecycleShutdownListener(new Lifecycle(shutdown));
  }

  @Test
  public void testShutdownOnFailure() {
    shutdown.execute();

    control.replay();

    listener.failure(NOOP_SERVICE);
  }
}
