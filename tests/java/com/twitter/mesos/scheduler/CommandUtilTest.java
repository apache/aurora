package com.twitter.mesos.scheduler;

import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CommandUtilTest {

  @Test
  public void testUriBasename() {
    test("c", "c");
    test("c", "/a/b/c");
    test("foo.zip", "hdfs://twitter.com/path/foo.zip");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadUri() {
    CommandUtil.create("a/b/c/");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyUri() {
    CommandUtil.create("");
  }

  private void test(String basename, String uri) {
    CommandInfo expectedCommand = CommandInfo.newBuilder()
        .addUris(URI.newBuilder().setValue(uri).setExecutable(true))
        .setValue("./" + basename)
        .build();
    assertEquals(expectedCommand, CommandUtil.create(uri));
  }
}
