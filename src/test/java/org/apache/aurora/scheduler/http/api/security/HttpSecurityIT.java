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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import javax.inject.Inject;
import javax.servlet.Filter;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;

import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.http.AbstractJettyTest;
import org.apache.aurora.scheduler.http.api.ApiModule;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.thrift.aop.AnnotatedAuroraAdmin;
import org.apache.aurora.scheduler.thrift.aop.MockDecoratedThrift;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.shiro.authc.credential.CredentialsMatcher;
import org.apache.shiro.authc.credential.SimpleCredentialsMatcher;
import org.apache.shiro.config.Ini;
import org.apache.shiro.realm.text.IniRealm;
import org.apache.shiro.web.filter.PathMatchingFilter;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.http.api.ApiModule.API_PATH;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HttpSecurityIT extends AbstractJettyTest {
  private static final Response OK = new Response().setResponseCode(ResponseCode.OK);

  private static final UsernamePasswordCredentials ROOT =
      new UsernamePasswordCredentials("root", "secret");
  private static final UsernamePasswordCredentials WFARNER =
      new UsernamePasswordCredentials("wfarner", "password");
  private static final UsernamePasswordCredentials UNPRIVILEGED =
      new UsernamePasswordCredentials("ksweeney", "12345");
  private static final UsernamePasswordCredentials BACKUP_SERVICE =
      new UsernamePasswordCredentials("backupsvc", "s3cret!!1");
  private static final UsernamePasswordCredentials DEPLOY_SERVICE =
      new UsernamePasswordCredentials("deploysvc", "0_0-x_0");

  private static final UsernamePasswordCredentials INCORRECT =
      new UsernamePasswordCredentials("root", "wrong");
  private static final UsernamePasswordCredentials NONEXISTENT =
      new UsernamePasswordCredentials("nobody", "12345");

  private static final Set<Credentials> INVALID_CREDENTIALS =
      ImmutableSet.of(INCORRECT, NONEXISTENT);

  private static final Set<Credentials> VALID_CREDENTIALS =
      ImmutableSet.of(ROOT, WFARNER, UNPRIVILEGED, BACKUP_SERVICE);

  private static final IJobKey ADS_STAGING_JOB = JobKeys.from("ads", "staging", "job");

  private static final Joiner COMMA_JOINER = Joiner.on(", ");
  private static final String ADMIN_ROLE = "admin";
  private static final String ENG_ROLE = "eng";
  private static final String BACKUP_ROLE = "backup";
  private static final String DEPLOY_ROLE = "deploy";
  private static final Named SHIRO_AFTER_AUTH_FILTER_ANNOTATION = Names.named("shiro_post_filter");

  private Ini ini;
  private Class<? extends CredentialsMatcher> credentialsMatcher;
  private AnnotatedAuroraAdmin auroraAdmin;
  private AtomicInteger afterAuthCalls;

  @Before
  public void setUp() {
    ini = new Ini();
    credentialsMatcher = SimpleCredentialsMatcher.class;

    Ini.Section users = ini.addSection(IniRealm.USERS_SECTION_NAME);
    users.put(ROOT.getUserName(), COMMA_JOINER.join(ROOT.getPassword(), ADMIN_ROLE));
    users.put(WFARNER.getUserName(), COMMA_JOINER.join(WFARNER.getPassword(), ENG_ROLE));
    users.put(UNPRIVILEGED.getUserName(), UNPRIVILEGED.getPassword());
    users.put(
        BACKUP_SERVICE.getUserName(),
        COMMA_JOINER.join(BACKUP_SERVICE.getPassword(), BACKUP_ROLE));
    users.put(
        DEPLOY_SERVICE.getUserName(),
        COMMA_JOINER.join(DEPLOY_SERVICE.getPassword(), DEPLOY_ROLE));

    Ini.Section roles = ini.addSection(IniRealm.ROLES_SECTION_NAME);
    roles.put(ADMIN_ROLE, "*");
    roles.put(ENG_ROLE, "thrift.AuroraSchedulerManager:*");
    roles.put(BACKUP_ROLE, "thrift.AuroraAdmin:listBackups");
    roles.put(
        DEPLOY_ROLE,
        "thrift.AuroraSchedulerManager:killTasks:"
            + ADS_STAGING_JOB.getRole()
            + ":"
            + ADS_STAGING_JOB.getEnvironment()
            + ":"
            + ADS_STAGING_JOB.getName());

    auroraAdmin = createMock(AnnotatedAuroraAdmin.class);
    afterAuthCalls = new AtomicInteger();
  }

  public static class CountingFilter extends PathMatchingFilter {
    private final AtomicInteger calls;

    @Inject
    public CountingFilter(AtomicInteger calls) {
      this.calls = calls;
    }

    @Override
    protected boolean onPreHandle(
        ServletRequest request,
        ServletResponse response,
        Object mappedValue) {

      calls.incrementAndGet();
      return true;
    }
  }

  @Override
  protected Function<ServletContext, Module> getChildServletModule() {
    Key<? extends Filter> afterAuthBinding =
        Key.get(CountingFilter.class, SHIRO_AFTER_AUTH_FILTER_ANNOTATION);
    return (servletContext) -> Modules.combine(
        new ApiModule(new ApiModule.Options()),
        new HttpSecurityModule(
            new IniShiroRealmModule(ini, credentialsMatcher),
            afterAuthBinding,
            servletContext),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(AtomicInteger.class).toInstance(afterAuthCalls);
            MockDecoratedThrift.bindForwardedMock(binder(), auroraAdmin);
          }
        });
  }

  private AuroraAdmin.Client getUnauthenticatedClient() throws TTransportException {
    return getClient(null);
  }

  private String formatUrl(String endpoint) {
    return "http://" + httpServer.getHost() + ":" + httpServer.getPort() + endpoint;
  }

  private AuroraAdmin.Client getClient(HttpClient httpClient) throws TTransportException {
    final TTransport httpClientTransport = new THttpClient(formatUrl(API_PATH), httpClient);
    addTearDown(httpClientTransport::close);
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
  public void testReadOnlyScheduler() throws TException, ServletException, IOException {
    expect(auroraAdmin.getRoleSummary()).andReturn(OK).times(3);

    replayAndStart();

    assertEquals(OK, getUnauthenticatedClient().getRoleSummary());
    assertEquals(OK, getAuthenticatedClient(ROOT).getRoleSummary());
    // Incorrect works because the server doesn't challenge for credentials to execute read-only
    // methods.
    assertEquals(OK, getAuthenticatedClient(INCORRECT).getRoleSummary());
    assertEquals(3, afterAuthCalls.get());
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
  public void testAuroraSchedulerManager() throws TException, ServletException, IOException {
    JobKey job = JobKeys.from("role", "env", "name").newBuilder();

    expect(auroraAdmin.killTasks(job, null, null)).andReturn(OK).times(2);
    expect(auroraAdmin.killTasks(ADS_STAGING_JOB.newBuilder(), null, null)).andReturn(OK);

    replayAndStart();

    assertEquals(
        OK,
        getAuthenticatedClient(WFARNER).killTasks(job, null, null));
    assertEquals(
        OK,
        getAuthenticatedClient(ROOT).killTasks(job, null, null));

    assertEquals(
        ResponseCode.INVALID_REQUEST,
        getAuthenticatedClient(UNPRIVILEGED).killTasks(null, null, null).getResponseCode());
    assertEquals(
        ResponseCode.AUTH_FAILED,
        getAuthenticatedClient(UNPRIVILEGED)
            .killTasks(job, null, null)
            .getResponseCode());
    assertEquals(
        ResponseCode.INVALID_REQUEST,
        getAuthenticatedClient(BACKUP_SERVICE).killTasks(null, null, null).getResponseCode());
    assertEquals(
        ResponseCode.AUTH_FAILED,
        getAuthenticatedClient(BACKUP_SERVICE)
            .killTasks(job, null, null)
            .getResponseCode());
    assertEquals(
        ResponseCode.AUTH_FAILED,
        getAuthenticatedClient(DEPLOY_SERVICE)
            .killTasks(job, null, null)
            .getResponseCode());
    assertEquals(
        OK,
        getAuthenticatedClient(DEPLOY_SERVICE).killTasks(
            ADS_STAGING_JOB.newBuilder(),
            null,
            null));

    assertKillTasksFails(getUnauthenticatedClient());
    assertKillTasksFails(getAuthenticatedClient(INCORRECT));
    assertKillTasksFails(getAuthenticatedClient(NONEXISTENT));
    assertTrue(afterAuthCalls.get() > 0);
  }

  private void assertSnapshotFails(AuroraAdmin.Client client) throws TException {
    try {
      client.snapshot();
      fail("snapshot should fail");
    } catch (TTransportException e) {
      // Expected.
    }
  }

  @Test
  public void testAuroraAdmin() throws TException, ServletException, IOException {
    expect(auroraAdmin.snapshot()).andReturn(OK);
    expect(auroraAdmin.listBackups()).andReturn(OK);

    replayAndStart();

    assertEquals(OK, getAuthenticatedClient(ROOT).snapshot());

    for (Credentials credentials : INVALID_CREDENTIALS) {
      assertSnapshotFails(getAuthenticatedClient(credentials));
    }

    for (Credentials credentials : Sets.difference(VALID_CREDENTIALS, ImmutableSet.of(ROOT))) {
      assertEquals(
          ResponseCode.AUTH_FAILED,
          getAuthenticatedClient(credentials).snapshot().getResponseCode());
    }

    assertEquals(OK, getAuthenticatedClient(BACKUP_SERVICE).listBackups());
    assertEquals(12, afterAuthCalls.get());
  }
}
