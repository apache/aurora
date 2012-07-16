package com.twitter.mesos.scheduler;

import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.Environment;
import org.apache.mesos.Protos.Environment.Variable;
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
    CommandUtil.create("a/b/c/", ImmutableMap.<String, String>of());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyUri() {
    CommandUtil.create("", ImmutableMap.<String, String>of());
  }

  private void test(String basename, String uri, Map<String, String> env) {
    CommandInfo expectedCommand = CommandInfo.newBuilder()
        .addUris(URI.newBuilder().setValue(uri).setExecutable(true))
        .setValue("./" + basename)
        .setEnvironment(Environment.newBuilder().addAllVariables(Iterables.transform(env.entrySet(),
            new Function<Map.Entry<String, String>, Variable>() {
              @Override public Variable apply(Entry<String, String> input) {
                return Variable.newBuilder()
                    .setName(input.getKey())
                    .setValue(input.getValue())
                    .build();
              }
            })))
        .build();
    assertEquals(expectedCommand, CommandUtil.create(uri, env));
  }
}
