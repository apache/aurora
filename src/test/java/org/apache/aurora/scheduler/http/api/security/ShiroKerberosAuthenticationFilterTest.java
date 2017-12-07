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
import java.util.function.Function;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;

import com.google.inject.Module;
import com.google.inject.servlet.ServletModule;
import com.google.inject.util.Providers;
import com.sun.jersey.api.client.ClientResponse;

import org.apache.aurora.scheduler.http.AbstractJettyTest;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;

public class ShiroKerberosAuthenticationFilterTest extends AbstractJettyTest {
  private static final String PATH = "/test";

  private Subject subject;
  private HttpServlet mockServlet;

  private ShiroKerberosAuthenticationFilter filter;

  @Before
  public void setUp() {
    subject = createMock(Subject.class);
    mockServlet = createMock(HttpServlet.class);

    filter = new ShiroKerberosAuthenticationFilter(Providers.of(subject));
  }

  private HttpServlet getMockServlet() {
    return mockServlet;
  }

  @Override
  public Function<ServletContext, Module> getChildServletModule() {
    return (servletContext) -> new ServletModule() {
      @Override
      protected void configureServlets() {
        filter(PATH).through(filter);
        serve(PATH).with(new HttpServlet() {
          @Override
          protected void service(HttpServletRequest req, HttpServletResponse resp)
              throws ServletException, IOException {

            getMockServlet().service(req, resp);
            resp.setStatus(HttpServletResponse.SC_OK);
          }
        });
      }
    };
  }

  @Test
  public void testDoesNotPermitUnauthenticated() throws ServletException, IOException {
    replayAndStart();

    ClientResponse clientResponse = getRequestBuilder(PATH).get(ClientResponse.class);
    assertEquals(HttpServletResponse.SC_UNAUTHORIZED, clientResponse.getStatus());
    assertEquals(
        ShiroKerberosAuthenticationFilter.NEGOTIATE,
        clientResponse.getHeaders().getFirst(HttpHeaders.WWW_AUTHENTICATE));
  }

  @Test
  public void testRejectsMalformedMechanism() {
    replayAndStart();

    ClientResponse clientResponse = getRequestBuilder(PATH)
        .header(HttpHeaders.AUTHORIZATION, "Basic asdf")
        .get(ClientResponse.class);
    assertEquals(
        HttpServletResponse.SC_BAD_REQUEST,
        clientResponse.getStatus());
  }

  @Test
  public void testLoginFailure401() {
    subject.login(isA(AuthenticationToken.class));
    expectLastCall().andThrow(new AuthenticationException());

    replayAndStart();

    ClientResponse clientResponse = getRequestBuilder(PATH)
        .header(HttpHeaders.AUTHORIZATION, ShiroKerberosAuthenticationFilter.NEGOTIATE + " asdf")
        .get(ClientResponse.class);

    assertEquals(HttpServletResponse.SC_UNAUTHORIZED, clientResponse.getStatus());
    assertEquals(
        ShiroKerberosAuthenticationFilter.NEGOTIATE,
        clientResponse.getHeaders().getFirst(HttpHeaders.WWW_AUTHENTICATE));
  }

  @Test
  public void testLoginSuccess200() throws ServletException, IOException {
    subject.login(isA(AuthenticationToken.class));
    mockServlet.service(anyObject(HttpServletRequest.class), anyObject(HttpServletResponse.class));

    replayAndStart();

    ClientResponse clientResponse = getRequestBuilder(PATH)
        .header(HttpHeaders.AUTHORIZATION, ShiroKerberosAuthenticationFilter.NEGOTIATE + " asdf")
        .get(ClientResponse.class);

    assertEquals(HttpServletResponse.SC_OK, clientResponse.getStatus());
  }
}
