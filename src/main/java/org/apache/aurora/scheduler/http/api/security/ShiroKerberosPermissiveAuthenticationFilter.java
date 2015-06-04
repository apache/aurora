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

import javax.inject.Inject;
import javax.inject.Provider;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.shiro.authz.UnauthenticatedException;
import org.apache.shiro.subject.Subject;

/**
 * Allows unauthenticated requests to proceed. It's up to the decorated RPCs to enforce auth
 * requirement by throwing {@link UnauthenticatedException}.
 */
public class ShiroKerberosPermissiveAuthenticationFilter extends ShiroKerberosAuthenticationFilter {
  @Inject
  ShiroKerberosPermissiveAuthenticationFilter(Provider<Subject> subjectProvider) {
    super(subjectProvider);
  }

  @Override
  protected void handleUnauthenticated(
      HttpServletRequest request,
      HttpServletResponse response,
      FilterChain chain) throws IOException, ServletException {

    // Incoming request is unauthenticated, but some RPCs might be okay with that.
    try {
      chain.doFilter(request, response);
    } catch (UnauthenticatedException e) {
      super.handleUnauthenticated(request, response, chain);
    }
  }
}
