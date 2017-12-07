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
import org.apache.shiro.authz.UnauthenticatedException;
import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;

public class ShiroKerberosPermissiveAuthenticationFilterTest extends AbstractJettyTest {
  private static final String PATH = "/test";

  private HttpServlet mockServlet;

  private ShiroKerberosPermissiveAuthenticationFilter filter;

  @Before
  public void setUp() {
    mockServlet = createMock(HttpServlet.class);

    filter =
        new ShiroKerberosPermissiveAuthenticationFilter(Providers.of(createMock(Subject.class)));
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

            mockServlet.service(req, resp);
            resp.setStatus(HttpServletResponse.SC_OK);
          }
        });
      }
    };
  }

  @Test
  public void testPermitsUnauthenticated() throws ServletException, IOException {
    mockServlet.service(anyObject(HttpServletRequest.class), anyObject(HttpServletResponse.class));

    replayAndStart();

    ClientResponse clientResponse = getRequestBuilder(PATH).get(ClientResponse.class);
    assertEquals(HttpServletResponse.SC_OK, clientResponse.getStatus());
  }

  @Test
  public void testInterceptsUnauthenticatedException() throws ServletException, IOException {
    mockServlet.service(anyObject(HttpServletRequest.class), anyObject(HttpServletResponse.class));
    expectLastCall().andThrow(new UnauthenticatedException());

    replayAndStart();

    ClientResponse clientResponse = getRequestBuilder(PATH).get(ClientResponse.class);

    assertEquals(HttpServletResponse.SC_UNAUTHORIZED, clientResponse.getStatus());
    assertEquals(
        ShiroKerberosAuthenticationFilter.NEGOTIATE,
        clientResponse.getHeaders().getFirst(HttpHeaders.WWW_AUTHENTICATE));
  }
}
