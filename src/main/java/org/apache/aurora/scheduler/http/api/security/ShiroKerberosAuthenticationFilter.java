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
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;

import org.apache.aurora.scheduler.http.AbstractFilter;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.subject.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

public class ShiroKerberosAuthenticationFilter extends AbstractFilter {
  private static final Logger LOG =
      LoggerFactory.getLogger(ShiroKerberosAuthenticationFilter.class);

  /**
   * From http://tools.ietf.org/html/rfc4559.
   */
  public static final String NEGOTIATE = "Negotiate";

  private final Provider<Subject> subjectProvider;

  @Inject
  ShiroKerberosAuthenticationFilter(Provider<Subject> subjectProvider) {
    this.subjectProvider = requireNonNull(subjectProvider);
  }

  @Override
  protected void doFilter(
      HttpServletRequest request,
      HttpServletResponse response,
      FilterChain chain) throws IOException, ServletException {

    Optional<String> authorizationHeaderValue =
        Optional.ofNullable(request.getHeader(HttpHeaders.AUTHORIZATION));
    if (authorizationHeaderValue.isPresent()) {
      LOG.debug("Authorization header is present");
      AuthorizeHeaderToken token;
      try {
        token = new AuthorizeHeaderToken(authorizationHeaderValue.get());
      } catch (IllegalArgumentException e) {
        LOG.info("Malformed Authorize header: " + e.getMessage());
        response.sendError(HttpServletResponse.SC_BAD_REQUEST);
        return;
      }
      try {
        subjectProvider.get().login(token);
        chain.doFilter(request, response);
      } catch (AuthenticationException e) {
        LOG.warn("Login failed: " + e.getMessage());
        sendChallenge(response);
      }
    } else {
      handleUnauthenticated(request, response, chain);
    }
  }

  protected void handleUnauthenticated(
      HttpServletRequest request,
      HttpServletResponse response,
      FilterChain chain) throws IOException, ServletException {

    sendChallenge(response);
  }

  private void sendChallenge(HttpServletResponse response) throws IOException {
    response.setHeader(HttpHeaders.WWW_AUTHENTICATE, NEGOTIATE);
    response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
  }
}
