package com.twitter.nexus.executor;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/**
 * Tests functionality of the SocketManager.
 *
 * @author wfarner
 */
public class SocketManagerTest {

  @Test
  public void socketManagerInvalidPortRange() {
    try {
      new SocketManager(11, 10);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }

    try {
      new SocketManager(10, 10);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }

    try {
      new SocketManager(900000, 1000000);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testLeaseSocket() throws Exception {
    SocketManager manager = new SocketManager(50000, 60000);
    int port = manager.leaseSocket();
    checkInRange(port, 50000, 60000);
  }

  @Test
  public void testExhaustSockets() throws Exception {
    int minRange = 50000;
    int maxRange = 50005;
    SocketManager manager = new SocketManager(minRange, maxRange);
    Set<Integer> ports = Sets.newHashSet();
    for (int i = minRange; i <= maxRange; i++) {
      int port = manager.leaseSocket();
      checkInRange(port, minRange, maxRange);
      ports.add(port);
    }

    try {
      manager.leaseSocket();
      fail();
    } catch (SocketManager.SocketLeaseException e) {
      // Expected.
    }
  }

  @Test
  public void testReleaseSoccket() throws Exception {
    int minRange = 50000;
    int maxRange = 50005;
    SocketManager manager = new SocketManager(minRange, maxRange);
    Set<Integer> ports = Sets.newHashSet();
    for (int i = minRange; i <= maxRange; i++) {
      int port = manager.leaseSocket();
      checkInRange(port, minRange, maxRange);
      ports.add(port);
    }

    // Return a socket - since the pool was exhausted, the next leased socket should be the same
    // one that was just returned.
    int returnPort = Iterables.get(ports, 1);
    manager.returnPort(returnPort);
    assertThat(manager.leaseSocket(), is(returnPort));
  }

  private static void checkInRange(int value, int min, int max) {
    assertThat(value >= min, is(true));
    assertThat(value <= max, is(true));
  }
}
