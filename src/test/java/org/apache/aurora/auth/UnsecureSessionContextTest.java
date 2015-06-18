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

import javax.inject.Provider;

import com.google.common.base.Supplier;
import com.google.inject.util.Providers;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class UnsecureSessionContextTest extends EasyMockTest {
  private StatsProvider statsProvider;
  private Subject subject;
  private Provider<Subject> subjectProvider;

  private UnsecureSessionContext sessionContext;

  private Supplier<Integer> gauge;

  @Before
  public void setUp() {
    statsProvider = createMock(StatsProvider.class);
    subject = createMock(Subject.class);
    subjectProvider = Providers.of(subject);

  }

  private void constructAndReplay() {
    Capture<Supplier<Integer>> gaugeCapture = createCapture();
    expect(statsProvider.makeGauge(
        eq(UnsecureSessionContext.SHIRO_AUDIT_LOGGING_ENABLED),
        capture(gaugeCapture))).andReturn(null);

    control.replay();

    sessionContext = new UnsecureSessionContext(statsProvider);
    gauge = gaugeCapture.getValue();
    assertEquals(0, (int) gauge.get());
  }

  private void assertIdentityEquals(String identity) {
    assertEquals(identity, sessionContext.getIdentity());
  }

  @Test
  public void testNoSubjectProvider() {
    constructAndReplay();

    assertIdentityEquals(UnsecureSessionContext.UNSECURE);
  }

  @Test
  public void testSubjectProviderReturnsNull() {
    expect(subject.getPrincipals()).andReturn(new SimplePrincipalCollection());

    constructAndReplay();

    sessionContext.setSubjectProvider(subjectProvider);
    assertIdentityEquals(UnsecureSessionContext.UNSECURE);
    assertEquals(1, (int) gauge.get());
  }

  @Test
  public void testSubjectProviderReturnsValue() {
    String userName = "jsmith";

    expect(subject.getPrincipals()).andReturn(new SimplePrincipalCollection(userName, "realm"));

    constructAndReplay();

    sessionContext.setSubjectProvider(subjectProvider);
    assertIdentityEquals(userName);
    assertEquals(1, (int) gauge.get());
  }
}
