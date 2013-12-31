/*
 * Copyright 2013 Twitter, Inc.
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
package com.twitter.aurora.scheduler.http;

import java.io.IOException;

import javax.inject.Inject;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import com.twitter.common.net.http.filters.AbstractHttpFilter;

/**
 * An HTTP filter that will redirect the request to the leading scheduler.
 */
public class LeaderRedirectFilter extends AbstractHttpFilter {

  private final LeaderRedirect redirector;

  @Inject
  LeaderRedirectFilter(LeaderRedirect redirector) {
    this.redirector = Preconditions.checkNotNull(redirector);
  }

  @Override
  public void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws IOException, ServletException {

    Optional<String> leaderRedirect = redirector.getRedirectTarget(request);
    if (leaderRedirect.isPresent()) {
      response.sendRedirect(leaderRedirect.get());
    } else {
      chain.doFilter(request, response);
    }
  }
}
