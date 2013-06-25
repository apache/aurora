package com.twitter.mesos.scheduler.base;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CommandUtilTest {
  @Test
  public void testUriBasename() {
    test("c", "c", ImmutableMap.<String, String>of());
    test("c", "/a/b/c", ImmutableMap.of("FOO", "1"));
    test("foo.zip", "hdfs://twitter.com/path/foo.zip", ImmutableMap.of("PATH", "/bin:/usr/bin"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadUri() {
    CommandUtil.create("a/b/c/");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyUri() {
    CommandUtil.create("");
  }

  private void test(String basename, String uri, Map<String, String> env) {
    CommandInfo expectedCommand = CommandInfo.newBuilder()
        .addUris(URI.newBuilder().setValue(uri).setExecutable(true))
        .setValue("./" + basename)
        .build();
    assertEquals(expectedCommand, CommandUtil.create(uri));
  }
}
