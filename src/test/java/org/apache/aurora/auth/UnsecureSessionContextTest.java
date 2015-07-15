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
package org.apache.aurora.auth;

import java.util.Optional;

import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.auth.UnsecureSessionContext.UNSECURE;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class UnsecureSessionContextTest extends EasyMockTest {
  private Subject subject;
  private PrincipalCollection principalCollection;

  @Before
  public void setUp() {
    subject = createMock(Subject.class);
    principalCollection = createMock(PrincipalCollection.class);
  }

  @Test
  public void testNonStringPrincipal() {
    expect(subject.getPrincipals()).andReturn(principalCollection);
    expect(principalCollection.oneByType(String.class)).andReturn(null);

    control.replay();

    assertEquals(UNSECURE, new UnsecureSessionContext(() -> Optional.of(subject)).getIdentity());
  }

  @Test
  public void testEmptySubject() {
    control.replay();

    assertEquals(UNSECURE, new UnsecureSessionContext(Optional::empty).getIdentity());
  }

  @Test
  public void testStringSubject() {
    expect(subject.getPrincipals()).andReturn(principalCollection);
    expect(principalCollection.oneByType(String.class)).andReturn("jsmith");

    control.replay();

    assertEquals("jsmith", new UnsecureSessionContext(() -> Optional.of(subject)).getIdentity());
  }
}
