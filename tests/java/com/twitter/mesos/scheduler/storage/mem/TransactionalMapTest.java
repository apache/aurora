package com.twitter.mesos.scheduler.storage.mem;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TransactionalMapTest {

  private TransactionalMap<Integer, String> map;

  @Before
  public void setUp() {
    map = TransactionalMap.wrap(Maps.<Integer, String>newHashMap());
  }

  @Test
  public void testCommitAndRollback() {
    test(ImmutableMap.<Integer, String>of(),
        new Runnable() {
          @Override public void run() {
            // Test no-op case.
          }
        });
    test(ImmutableMap.of(1, "a", 2, "c"),
        new Runnable() {
          @Override public void run() {
            map.put(1, "a");
            map.put(2, "b");
            map.put(2, "c");
            map.put(3, "d");
            map.remove(3);
          }
        });
    test(ImmutableMap.of(2, "c"),
        new Runnable() {
          @Override public void run() {
            map.remove(1);
          }
        });
    test(ImmutableMap.<Integer, String>of(),
        new Runnable() {
          @Override public void run() {
            map.clear();
          }
        });
    test(ImmutableMap.<Integer, String>of(),
        new Runnable() {
          @Override public void run() {
            map.put(1, "a");
            map.put(2, "b");
            map.put(2, "c");
            map.put(3, "d");
            assertEquals(ImmutableMap.of(1, "a", 2, "c", 3, "d"), ImmutableMap.copyOf(map));
            map.clear();
          }
        });
    test(ImmutableMap.<Integer, String>of(1, "d", 2, "b", 6, "a"),
        new Runnable() {
          @Override public void run() {
            map.put(1, "a");
            map.put(2, "b");
            map.putAll(ImmutableMap.of(6, "a", 1, "d"));
          }
        });
  }

  /**
   * Runs a test sequence, performs a rollback, runs the sequence again, and commits.
   * The test is successful if the map's state prior to the test matches the state after rollback,
   * and matches {@code expected} after commit.
   *
   * @param expected Expected map contents after running the sequence and committing.
   * @param sequence Test sequence to run.
   */
  private void test(Map<Integer, String> expected, Runnable sequence) {
    Map<Integer, String> before = ImmutableMap.copyOf(map);
    sequence.run();
    map.rollback();
    assertEqual(before, map);

    sequence.run();
    map.commit();
    assertEqual(expected, map);
  }

  private static void assertEqual(Map<Integer, String> expected, Map<Integer, String> actual) {
    assertEquals(
        ImmutableMap.copyOf(expected),
        ImmutableMap.copyOf(actual));
    assertEquals(
        ImmutableSet.copyOf(expected.keySet()),
        ImmutableSet.copyOf(actual.keySet()));
    assertEquals(
        ImmutableSet.copyOf(expected.entrySet()),
        ImmutableSet.copyOf(actual.entrySet()));
  }
}
