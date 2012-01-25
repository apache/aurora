package com.twitter.mesos.executor;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import com.twitter.mesos.executor.ProcessScanner.ProcessInfo;

import static junit.framework.Assert.assertEquals;

public class ProcessScannerTest {

  @Test
  public void testValidInput() {
    Set<ProcessInfo> result;

    // test valid input
    result = ProcessScanner.parseOutput("123 abc 100,200\n456 zxc 300");
    Set<ProcessInfo> expected = ImmutableSet.of(
        new ProcessInfo(123, "abc", ImmutableSet.of(100, 200)),
        new ProcessInfo(456, "zxc", ImmutableSet.of(300))
    );
    assertEquals(expected, result);

    // test valid input with extra new lines
    result = ProcessScanner.parseOutput("123 abc 100,200\n\n456 zxc 300");
    assertEquals(expected, result);

    // test good and bad input interleaved on separate lines
    result = ProcessScanner.parseOutput(
        "123 abc 100,200\nbcfh 123\n456 zxc 300\n\n123 abc 200 200");
    assertEquals(expected, result);

    // test a mesos task has multiple process
    result = ProcessScanner.parseOutput("123 abc 123\n456 abc 456");
    expected = ImmutableSet.of(
        new ProcessInfo(123, "abc", ImmutableSet.of(123)),
        new ProcessInfo(456, "abc", ImmutableSet.of(456)));
    assertEquals(expected, result);

    // test a mesos task that is not listening on any port
    result = ProcessScanner.parseOutput("123 abc \n456 abc 456");
    expected = ImmutableSet.of(
        new ProcessInfo(123, "abc", ImmutableSet.<Integer>of()),
        new ProcessInfo(456, "abc", ImmutableSet.of(456)));
    assertEquals(expected, result);
  }

  @Test
  public void testInvalidInput() {
    Set<ProcessInfo> result;
    // test bad input
    result = ProcessScanner.parseOutput("fndslagdfjsg fndsogajn \njfiosd");
    assertEquals(0, result.size());

    // test bad input of incorrect pid and task id order
    result = ProcessScanner.parseOutput("abc 123");
    assertEquals(0, result.size());

    // test empty input
    result = ProcessScanner.parseOutput("");
    assertEquals(0, result.size());

    // test bad input with non-numeric pid
    result = ProcessScanner.parseOutput("123a tab");
    assertEquals(0, result.size());

    // test bad input with extra component
    result = ProcessScanner.parseOutput("123 abc 1 abc");
    assertEquals(0, result.size());

    // test bad input with invalid port
    result = ProcessScanner.parseOutput("123 abc 1aaa");
    assertEquals(0, result.size());
  }
}
