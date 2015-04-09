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
package org.apache.aurora.scheduler.http.api.security;

import java.io.File;

import javax.security.auth.kerberos.KerberosPrincipal;

import com.google.inject.Guice;
import com.google.inject.Module;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.easymock.EasyMock;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;

public class Kerberos5ShiroRealmModuleTest extends EasyMockTest {
  private static final KerberosPrincipal SERVER_PRINCIPAL =
      new KerberosPrincipal("HTTP/aurora.example.com@EXAMPLE.COM");

  private File serverKeytab;
  private GSSManager gssManager;

  private GSSCredential gssCredential;

  private Module module;

  @Before
  public void setUp() {
    serverKeytab = createMock(File.class);
    gssManager = createMock(GSSManager.class);
    gssCredential = createMock(GSSCredential.class);

    module = new Kerberos5ShiroRealmModule(serverKeytab, SERVER_PRINCIPAL, gssManager);
  }

  @Test
  public void testConfigure() throws Exception {
    expect(serverKeytab.getAbsolutePath()).andReturn("path.keytab");
    expect(
        gssManager.createCredential(
            EasyMock.<GSSName>isNull(),
            eq(GSSCredential.INDEFINITE_LIFETIME),
            anyObject(Oid[].class),
            eq(GSSCredential.ACCEPT_ONLY)))
        .andReturn(gssCredential);

    control.replay();

    Guice.createInjector(module).getInstance(Kerberos5Realm.class);
  }
}
