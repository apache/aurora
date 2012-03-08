package com.twitter.mesos.scheduler.identity;

import java.io.IOException;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;

import net.lag.craijce.CraiJCE;
import net.lag.jaramiko.Message;
import net.lag.jaramiko.PKey;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class AuthorizedKeySetTest {

  private static final byte[] DATA = "hello world".getBytes();

  @Test
  public void testLoadKeys() throws Exception {
    // all good
    assertEquals(1, load("valid1").keys.size());
    assertEquals(3, load("valid3").keys.size());
    assertEquals(4, load("team-role").keys.size());

    // all bad
    assertEquals(0, load("invalid1").keys.size());
    assertEquals(0, load("invalid2").keys.size());

    // mixed good and bad
    assertEquals(1, load("valid2").keys.size());
  }

  @Test
  public void testExternallySignedData() throws Exception {
    AuthorizedKeySet keySet = load("valid1");
    byte[] helloWorldValid1Sig = Resources.toByteArray(
        Resources.getResource(AuthorizedKeySetTest.class, "hello_world.valid1.sig"));
    assertTrue(keySet.verify(DATA, helloWorldValid1Sig));
  }

  @Test
  public void testCorruptedSignedData() throws Exception {
    AuthorizedKeySet keySet = load("valid1");
    byte[] helloWorldCorruptSig = Resources.toByteArray(
        Resources.getResource(AuthorizedKeySetTest.class, "hello_world.corrupt.sig"));
    assertFalse(keySet.verify(DATA, helloWorldCorruptSig));
  }

  @Test
  public void testEmptyKeySet() throws Exception {
    testVerify(new AuthorizedKeySet(ImmutableList.<PKey>of()),
               "authorized_keys.priv/valid2.priv", false);
    testVerify(load(), "authorized_keys.priv/valid3.priv", false);
  }

  @Test
  public void testInternallySignedData() throws Exception {
    AuthorizedKeySet keySet = load("valid2");
    testVerify(keySet, "authorized_keys.priv/valid2.priv", true);
    testVerify(keySet, "authorized_keys.priv/valid3.priv", false);
    testVerify(keySet, "authorized_keys.priv/valid3-2.priv", false);
  }

  @Test
  public void testSignedWithSeveralRoles() throws Exception {
    AuthorizedKeySet keySet = load("valid3");
    testVerify(keySet, "authorized_keys.priv/valid3.priv", true);
    testVerify(keySet, "authorized_keys.priv/valid3-2.priv", true);
  }

  private void testVerify(AuthorizedKeySet keySet, String privkeyResource, boolean valid)
      throws Exception {
    PKey pk = PKey.readPrivateKeyFromStream(getClass().getResourceAsStream(privkeyResource), null);
    Message helloWorldSig = pk.signSSHData(new CraiJCE(), DATA);
    assertEquals(valid, keySet.verify(DATA, helloWorldSig.toByteArray()));
  }

  private AuthorizedKeySet load(String... pubkeyResources) throws Exception {
    return AuthorizedKeySet.createFromKeys(
        Iterables.concat(Iterables.transform(ImmutableList.copyOf(pubkeyResources),
            new Function<String, Iterable<String>>() {
              @Override public Iterable<String> apply(String pubkeyResources) {
                try {
                  return Resources.readLines(
                      Resources.getResource(AuthorizedKeySetTest.class,
                                            String.format("authorized_keys/%s", pubkeyResources)),
                      Charsets.US_ASCII);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
            })));
  }
}
