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
package com.twitter.common.net.http.filters;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * A filter that allows subclass to omit implementations of {@link #init(FilterConfig)} and
 * {@link #destroy()}.
 */
public abstract class AbstractHttpFilter implements Filter {

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    // No-op by default.
  }

  @Override
  public final void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    doFilter((HttpServletRequest) request, (HttpServletResponse) response, chain);
  }

  /**
   * Convenience method to allow subclasses to avoid type casting that may be necessary when
   * implementing {@link #doFilter(ServletRequest, ServletResponse, FilterChain)}.
   *
   * @param request HTTP request.
   * @param response HTTP response.
   * @param chain Filter chain.
   * @throws IOException If there is an error reading the request or writing the response.
   * @throws ServletException If the filter or chain encounters an error handling the request.
   */
  public abstract void doFilter(
      HttpServletRequest request,
      HttpServletResponse response,
      FilterChain chain) throws IOException, ServletException;

  @Override
  public void destroy() {
    // No-op by default.
  }
}
