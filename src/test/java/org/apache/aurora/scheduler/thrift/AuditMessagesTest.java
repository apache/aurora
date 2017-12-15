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

import static org.apache.aurora.scheduler.thrift.AuditMessages.DEFAULT_USER;
import static org.easymock.EasyMock.expect;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class AuditMessagesTest extends EasyMockTest {
  @Test
  public void testEmptySubject() {
    AuditMessages emptyMessages = new AuditMessages(Optional::empty);

    control.replay();

    assertThat(
        emptyMessages.killedByRemoteUser(Optional.empty()).get(),
        containsString(DEFAULT_USER));
    assertThat(emptyMessages.restartedByRemoteUser().get(), containsString(DEFAULT_USER));
    assertThat(emptyMessages.transitionedBy().get(), containsString(DEFAULT_USER));
  }

  @Test
  public void testPresentSubject() {
    Subject subject = createMock(Subject.class);
    AuditMessages presentMessages = new AuditMessages(() -> Optional.of(subject));

    expect(subject.getPrincipal()).andReturn("shiro").times(3);

    control.replay();

    assertThat(presentMessages.killedByRemoteUser(Optional.empty()).get(), containsString("shiro"));
    assertThat(presentMessages.restartedByRemoteUser().get(), containsString("shiro"));
    assertThat(presentMessages.transitionedBy().get(), containsString("shiro"));
  }

  @Test
  public void testKilledByRemoteUserMessages() {
    Subject subject = createMock(Subject.class);
    AuditMessages messages = new AuditMessages(() -> Optional.of(subject));

    expect(subject.getPrincipal()).andReturn("shiro").times(2);

    control.replay();

    assertEquals(messages.killedByRemoteUser(
        Optional.of("Test message")).get(),
        "Killed by shiro.\nTest message");
    assertEquals(messages.killedByRemoteUser(Optional.empty()).get(), "Killed by shiro");
  }
}
