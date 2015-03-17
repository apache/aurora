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

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.testing.TearDown;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import com.twitter.common.stats.StatsProvider;

import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.scheduler.http.JettyServerModuleTest;
import org.apache.aurora.scheduler.http.api.ApiModule;
import org.apache.aurora.scheduler.thrift.aop.AnnotatedAuroraAdmin;
import org.apache.aurora.scheduler.thrift.aop.MockDecoratedThrift;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.shiro.config.Ini;
import org.apache.shiro.realm.text.IniRealm;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.http.api.ApiModule.API_PATH;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ApiSecurityIT extends JettyServerModuleTest {
  private static final Response OK = new Response().setResponseCode(ResponseCode.OK);

  private static final UsernamePasswordCredentials ROOT =
      new UsernamePasswordCredentials("root", "secret");
  private static final UsernamePasswordCredentials WFARNER =
      new UsernamePasswordCredentials("wfarner", "password");
  private static final UsernamePasswordCredentials UNPRIVILEGED =
      new UsernamePasswordCredentials("ksweeney", "12345");
  private static final UsernamePasswordCredentials BACKUP_SERVICE =
      new UsernamePasswordCredentials("backupsvc", "s3cret!!1");

  private static final UsernamePasswordCredentials INCORRECT =
      new UsernamePasswordCredentials("root", "wrong");
  private static final UsernamePasswordCredentials NONEXISTENT =
      new UsernamePasswordCredentials("nobody", "12345");

  private static final Set<Credentials> INVALID_CREDENTIALS =
      ImmutableSet.<Credentials>of(INCORRECT, NONEXISTENT);

  private static final Set<Credentials> VALID_CREDENTIALS =
      ImmutableSet.<Credentials>of(ROOT, WFARNER, UNPRIVILEGED, BACKUP_SERVICE);

  private Ini ini;
  private AnnotatedAuroraAdmin auroraAdmin;
  private StatsProvider statsProvider;

  @Before
  public void setUp() {
    ini = new Ini();

    Ini.Section users = ini.addSection(IniRealm.USERS_SECTION_NAME);
    users.put(ROOT.getUserName(), ROOT.getPassword() + ", admin");
    users.put(WFARNER.getUserName(), WFARNER.getPassword() + ", eng");
    users.put(UNPRIVILEGED.getUserName(), UNPRIVILEGED.getPassword());
    users.put(BACKUP_SERVICE.getUserName(), BACKUP_SERVICE.getPassword() + ", backupsvc");

    Ini.Section roles = ini.addSection(IniRealm.ROLES_SECTION_NAME);
    roles.put("admin", "*");
    roles.put("eng", "thrift.AuroraSchedulerManager:*");
    roles.put("backupsvc", "thrift.AuroraAdmin:listBackups");

    auroraAdmin = createMock(AnnotatedAuroraAdmin.class);
    statsProvider = createMock(StatsProvider.class);
    expect(statsProvider.makeCounter(anyString())).andStubReturn(new AtomicLong());
  }

  @Override
  protected Module getChildServletModule() {
    return Modules.combine(
        new ApiModule(),
        new ApiSecurityModule(new IniShiroRealmModule(ini)),
        new AbstractModule() {
          @Override
          protected void configure() {
            MockDecoratedThrift.bindForwardedMock(binder(), auroraAdmin);
            bind(StatsProvider.class).toInstance(statsProvider);
          }
        });
  }

  private AuroraAdmin.Client getUnauthenticatedClient() throws TTransportException {
    return getClient(null);
  }

  private AuroraAdmin.Client getClient(HttpClient httpClient) throws TTransportException {
    final TTransport httpClientTransport = new THttpClient(
        "http://" + httpServer.getHostName() + ":" + httpServer.getPort() + API_PATH,
        httpClient);
    addTearDown(new TearDown() {
      @Override
      public void tearDown() throws Exception {
        httpClientTransport.close();
      }
    });
    return new AuroraAdmin.Client(new TJSONProtocol(httpClientTransport));
  }

  private AuroraAdmin.Client getAuthenticatedClient(Credentials credentials)
      throws TTransportException {

    DefaultHttpClient defaultHttpClient = new DefaultHttpClient();

    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, credentials);
    defaultHttpClient.setCredentialsProvider(credentialsProvider);

    return getClient(defaultHttpClient);
  }

  @Test
  public void testReadOnlyScheduler() throws TException {
    expect(auroraAdmin.getRoleSummary()).andReturn(OK).times(3);

    replayAndStart();

    assertEquals(OK, getUnauthenticatedClient().getRoleSummary());
    assertEquals(OK, getAuthenticatedClient(ROOT).getRoleSummary());
    // Incorrect works because the server doesn't challenge for credentials to execute read-only
    // methods.
    assertEquals(OK, getAuthenticatedClient(INCORRECT).getRoleSummary());
  }

  private void assertKillTasksFails(AuroraAdmin.Client client) throws TException {
    try {
      client.killTasks(null, null, null);
      fail("killTasks should fail.");
    } catch (TTransportException e) {
      // Expected.
    }
  }

  @Test
  public void testAuroraSchedulerManager() throws TException, IOException {
    expect(auroraAdmin.killTasks(null, new Lock().setMessage("1"), null)).andReturn(OK);
    expect(auroraAdmin.killTasks(null, new Lock().setMessage("2"), null)).andReturn(OK);

    replayAndStart();

    assertEquals(OK,
        getAuthenticatedClient(WFARNER).killTasks(null, new Lock().setMessage("1"), null));
    assertEquals(OK,
        getAuthenticatedClient(ROOT).killTasks(null, new Lock().setMessage("2"), null));
    assertEquals(
        ResponseCode.AUTH_FAILED,
        getAuthenticatedClient(UNPRIVILEGED).killTasks(null, null, null).getResponseCode());
    assertEquals(
        ResponseCode.AUTH_FAILED,
        getAuthenticatedClient(BACKUP_SERVICE).killTasks(null, null, null).getResponseCode());

    assertKillTasksFails(getUnauthenticatedClient());
    assertKillTasksFails(getAuthenticatedClient(INCORRECT));
    assertKillTasksFails(getAuthenticatedClient(NONEXISTENT));
  }

  private void assertSnapshotFails(AuroraAdmin.Client client) throws TException {
    try {
      client.snapshot(null);
      fail("snapshot should fail");
    } catch (TTransportException e) {
      // Expected.
    }
  }

  @Test
  public void testAuroraAdmin() throws TException {
    expect(auroraAdmin.snapshot(null)).andReturn(OK);
    expect(auroraAdmin.listBackups(null)).andReturn(OK);

    replayAndStart();

    assertEquals(OK, getAuthenticatedClient(ROOT).snapshot(null));

    for (Credentials credentials : INVALID_CREDENTIALS) {
      assertSnapshotFails(getAuthenticatedClient(credentials));
    }

    for (Credentials credentials : Sets.difference(VALID_CREDENTIALS, ImmutableSet.of(ROOT))) {
      assertEquals(
          ResponseCode.AUTH_FAILED,
          getAuthenticatedClient(credentials).snapshot(null).getResponseCode());
    }

    assertEquals(OK, getAuthenticatedClient(BACKUP_SERVICE).listBackups(null));
  }
}
