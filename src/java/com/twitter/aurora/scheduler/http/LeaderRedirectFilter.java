package com.twitter.aurora.scheduler.http;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;

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
