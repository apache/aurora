/**
 * Copyright 2013 Apache Software Foundation
 *
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

import com.twitter.common.net.http.filters.AbstractHttpFilter;

/**
 * A filter that maps string template servlet paths to UI client pages. This is needed because
 * AngularJS's resource loader is confused when there is a trailing / at the end of URL's.
 */
public class UIRedirectFilter extends AbstractHttpFilter {

  @Override
  public void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws IOException, ServletException {

    if ("/scheduler/".equals(request.getRequestURI())) {
      response.sendRedirect("/scheduler");
    } else {
      chain.doFilter(request, response);
    }
  }
}
