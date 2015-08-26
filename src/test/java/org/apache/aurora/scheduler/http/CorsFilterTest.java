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

import com.google.common.net.HttpHeaders;

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.replay;

public class CorsFilterTest extends EasyMockTest {

  private static final String DUMMY_ALLOWED_ORIGIN_DOMAIN = "test";

  private HttpServletRequest request;
  private HttpServletResponse response;
  private FilterChain chain;

  @Before
  public void setUp() {
    request = createMock(HttpServletRequest.class);
    response = createMock(HttpServletResponse.class);
    chain = createMock(FilterChain.class);
  }

  @Test
  public void testCorsSupport() throws IOException, ServletException {
    CorsFilter corsFilter = new CorsFilter(DUMMY_ALLOWED_ORIGIN_DOMAIN);

    response.setHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN, DUMMY_ALLOWED_ORIGIN_DOMAIN);
    response.setHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_METHODS, CorsFilter.ALLOWED_METHODS);
    response.setHeader(HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS, CorsFilter.ALLOWED_HEADERS);
    chain.doFilter(request, response);

    replay(response);

    corsFilter.doFilter(request, response, chain);
  }
}
