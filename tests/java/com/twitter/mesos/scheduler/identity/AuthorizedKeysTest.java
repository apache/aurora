package com.twitter.mesos.scheduler.identity;

import java.util.Arrays;
import java.util.List;

import net.lag.crai.Crai;
import net.lag.craijce.CraiJCE;

import org.junit.Ignore;


@Ignore("TODO(wfarner): Update this test")
public class AuthorizedKeysTest {
  private static final String resources_dir = "com/twitter/mesos/scheduler/identity";
  private static final String authorized_keys_dir = "authorized_keys";
  private static final String helloWorldValid1SigFilename  = "hello_world.valid1.sig";
  private static final String helloWorldCorruptSigFilename = "hello_world.corrupt.sig";

  private static final String valid2PrivKey = "authorized_keys.priv/valid2.priv";
  private static final String valid3PrivKey1 = "authorized_keys.priv/valid3.priv";
  private static final String valid3PrivKey2 = "authorized_keys.priv/valid3-2.priv";

  private static final Crai craijce = new CraiJCE();

  private static final List<String> ValidRoles =
    Arrays.asList("team-role", "valid1", "valid2", "valid3");
  private static final List<String> ValidUsers =
    Arrays.asList("valid1", "valid2", "valid3");

  private static byte[] helloWorldValid1Sig;
  private static byte[] helloWorldCorruptSig;
  private static AuthorizedKeySet keyManager;

/*
  @BeforeClass
  public static void setUp() throws IOException {
    helloWorldValid1Sig = Resources.toByteArray(
      Resources.getResource(AuthorizedKeysTest.class, helloWorldValid1SigFilename));
    helloWorldCorruptSig = Resources.toByteArray(
      Resources.getResource(AuthorizedKeysTest.class, helloWorldCorruptSigFilename));
    keyManager = AuthorizedKeysParser.loadDir(new File(
      Resources.getResource(AuthorizedKeysTest.class, authorized_keys_dir).getFile()));
  }

  @Test
  public void testArityOfRoles() {
    assertEquals(keyManager.numberKeys("valid1"), 1);
    assertEquals(keyManager.numberKeys("valid2"), 1);
    assertEquals(keyManager.numberKeys("valid3"), 2);
    assertEquals(keyManager.numberKeys("team-role"), 4);
    assertEquals(keyManager.numberKeys("invalid1"), -1);
    assertEquals(keyManager.numberKeys("invalid2"), -1);
  }

  @Test
  public void testExternallySignedData() {
    assertTrue(keyManager.verify("valid1",
                                 "hello world".getBytes(),
                                 helloWorldValid1Sig));
  }

  @Test
  public void testCorruptedSignedData() {
    assertFalse(keyManager.verify("valid1",
                                 "hello world".getBytes(),
                                  helloWorldCorruptSig));
  }

  @Test
  public void testInternallySignedData() throws SSHException, IOException {
    PKey rk = PKey.readPrivateKeyFromStream(
        getClass().getResourceAsStream(valid2PrivKey), null);
    Message hello_world_sig = rk.signSSHData(craijce, "hello world".getBytes());
    assertTrue(keyManager.verify("valid2",
                                 "hello world".getBytes(),
                                 hello_world_sig.toByteArray()));
  }

  @Test
  public void testSignedWithSeveralRoles() throws SSHException, IOException {
    PKey rk1 = PKey.readPrivateKeyFromStream(
        getClass().getResourceAsStream(valid3PrivKey1), null);
    PKey rk2 = PKey.readPrivateKeyFromStream(
        getClass().getResourceAsStream(valid3PrivKey2), null);

    Message hello_world_sig1 = rk1.signSSHData(craijce, "hello world".getBytes());
    Message hello_world_sig2 = rk1.signSSHData(craijce, "hello world".getBytes());

    assertTrue(keyManager.verify("valid3",
                                 "hello world".getBytes(),
                                 hello_world_sig1.toByteArray()));
    assertTrue(keyManager.verify("valid3",
                                 "hello world".getBytes(),
                                 hello_world_sig2.toByteArray()));
    assertTrue(keyManager.verify("team-role",
                                 "hello world".getBytes(),
                                 hello_world_sig1.toByteArray()));
    assertTrue(keyManager.verify("team-role",
                                 "hello world".getBytes(),
                                  hello_world_sig2.toByteArray()));
  }
*/

}
