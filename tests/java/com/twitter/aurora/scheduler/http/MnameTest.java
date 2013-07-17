package com.twitter.aurora.scheduler.http;

import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import com.twitter.aurora.gen.AssignedTask;

import static org.junit.Assert.assertEquals;

public class MnameTest {

  @Test
  public void testRedirectPort() {
    assertEquals(Optional.absent(), getRedirectPort(null));
    assertEquals(Optional.absent(), getRedirectPort(ImmutableMap.<String, Integer>of()));
    assertEquals(Optional.absent(), getRedirectPort(ImmutableMap.of("thrift", 5)));
    assertEquals(Optional.of(5), getRedirectPort(ImmutableMap.of("health", 5, "http", 6)));
    assertEquals(Optional.of(6), getRedirectPort(ImmutableMap.of("http", 6)));
    assertEquals(Optional.of(7), getRedirectPort(ImmutableMap.of("HTTP", 7)));
    assertEquals(Optional.of(8), getRedirectPort(ImmutableMap.of("web", 8)));
  }

  private Optional<Integer> getRedirectPort(Map<String, Integer> ports) {
    return Mname.getRedirectPort(new AssignedTask().setAssignedPorts(ports));
  }
}
