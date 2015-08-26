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
package org.apache.aurora.scheduler.http;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HttpHeaders;

import org.apache.aurora.common.base.MorePreconditions;

/*
 * A filter that adds CORS headers to HTTP responses. This filter enables CORS support for a single
 * domain.
 */
public class CorsFilter extends AbstractFilter {

  @VisibleForTesting
  static final String ALLOWED_METHODS = String.join(
      ",",
      HttpMethod.DELETE,
      HttpMethod.GET,
      HttpMethod.HEAD,
      HttpMethod.OPTIONS,
      HttpMethod.POST,
      HttpMethod.PUT);

  @VisibleForTesting
  static final String ALLOWED_HEADERS = String.join(
      ",",
      HttpHeaders.ACCEPT,
      HttpHeaders.CONTENT_TYPE,
      HttpHeaders.ORIGIN,
      HttpHeaders.X_REQUESTED_WITH);

  private final String allowedOriginDomain;

  /*
   * param allowedOriginDomain a domain for which CORS is enabled.
   */
  public CorsFilter(String allowedOriginDomain) {
    this.allowedOriginDomain = MorePreconditions.checkNotBlank(allowedOriginDomain);
  }

  @Override
  public void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws IOException, ServletException {

    response.setHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, allowedOriginDomain);
    response.setHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_METHODS, ALLOWED_METHODS);
    response.setHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS, ALLOWED_HEADERS);

    chain.doFilter(request, response);
  }
}
