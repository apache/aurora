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
package org.apache.aurora.scheduler.thrift;

import java.util.Optional;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.shiro.subject.Subject;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

public class AuditMessagesTest extends EasyMockTest {
  @Test
  public void testEmptySubject() {
    AuditMessages emptyMessages = new AuditMessages(Optional::empty);

    control.replay();

    assertThat(emptyMessages.killedBy("legacy").get(), containsString("legacy"));
    assertThat(emptyMessages.restartedBy("legacy").get(), containsString("legacy"));
    assertThat(emptyMessages.transitionedBy("legacy").get(), containsString("legacy"));
  }

  @Test
  public void testPresentSubject() {
    Subject subject = createMock(Subject.class);
    AuditMessages presentMessages = new AuditMessages(() -> Optional.of(subject));

    expect(subject.getPrincipal()).andReturn("shiro").times(3);

    control.replay();

    assertThat(presentMessages.killedBy("legacy").get(), containsString("shiro"));
    assertThat(presentMessages.restartedBy("legacy").get(), containsString("shiro"));
    assertThat(presentMessages.transitionedBy("legacy").get(), containsString("shiro"));
  }
}
