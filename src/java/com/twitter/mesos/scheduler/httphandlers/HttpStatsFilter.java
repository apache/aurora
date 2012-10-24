package com.twitter.mesos.scheduler.httphandlers;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import com.twitter.common.net.http.filters.AbstractHttpFilter;
import com.twitter.common.stats.SlidingStats;

/**
 * An HTTP filter that exports counts and timing for requests based on response code.
 */
public class HttpStatsFilter extends AbstractHttpFilter {

  private final LoadingCache<Integer, SlidingStats> counters = CacheBuilder.newBuilder()
      .build(new CacheLoader<Integer, SlidingStats>() {
        @Override public SlidingStats load(Integer status) {
          return new SlidingStats("http_" + status + "_responses", "nanos");
        }
      });

  private static class ResponseWithStatus extends HttpServletResponseWrapper {
    // 200 response code is the default if none is explicitly set.
    int wrappedStatus = 200;

    ResponseWithStatus(HttpServletResponse resp) {
      super(resp);
    }

    @Override public void setStatus(int sc) {
      super.setStatus(sc);
      wrappedStatus = sc;
    }

    @Override public void setStatus(int sc, String sm) {
      super.setStatus(sc, sm);
      wrappedStatus = sc;
    }
  }

  @Override
  public void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws IOException, ServletException {

    long start = System.nanoTime();
    ResponseWithStatus wrapper = new ResponseWithStatus(response);
    chain.doFilter(request, wrapper);
    counters.getUnchecked(wrapper.wrappedStatus).accumulate(System.nanoTime() - start);
  }
}
