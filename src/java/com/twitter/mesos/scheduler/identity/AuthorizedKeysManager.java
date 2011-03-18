package com.twitter.mesos.scheduler.identity;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.lang.NegativeArraySizeException;

import net.lag.jaramiko.PKey;
import net.lag.jaramiko.Message;
import net.lag.jaramiko.SSHException;
import net.lag.crai.Crai;
import net.lag.craijce.CraiJCE;

import com.google.common.collect.Multimap;
import com.google.common.collect.HashMultimap;
import com.twitter.common.collections.Pair;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manages keys imported from /etc/ssh/authorized_keys.
 *
 * @author Brian Wickman
 */
public class AuthorizedKeysManager {
  private final Multimap<Pair<String, String>, PKey> rolesMap = HashMultimap.create();
  private final Crai crai = new CraiJCE();

  /**
   * Associate a public key {@code key} to a (role, user) pair.
   *
   * @param role Role account name.
   * @param user User name.
   * @param key  Public key.
   */
  public void add(String role, String user, PKey key) {
    checkNotNull(role);
    checkNotNull(user);
    checkNotNull(key);

    rolesMap.put(Pair.of(role, user), key);
  }

  /**
   * Determine if we know about a (role, user) pair.  Returns number of
   * keys associated with (role, user) pair.  Return -1 if the (role, user)
   * has not been registered.
   *
   * @param role Role account name.
   * @param user User name.
   */
  public int numberKeys(String role, String user) {
    checkNotNull(role);
    checkNotNull(user);
    Pair key = Pair.of(role, user);
    if (!rolesMap.containsKey(key)) return -1;
    return rolesMap.get(key).size();
  }

  /**
   * Given a (role, user) pair, verify that the provided signature is a valid
   * signature of the data.
   *
   * @param role Role account name.
   * @param user User name.
   * @param data Data that was signed.
   * @param signature Signature of the data.
   */
  public boolean verify(String role, String user, byte[] data, byte[] signature) {
    checkNotNull(data);
    checkNotNull(signature);

    if (numberKeys(role, user) <= 0) return false;

    Message sig = new Message(signature);

    for (PKey key : rolesMap.get(Pair.of(role, user))) {
      sig.rewind();
      try {
        if (key.verifySSHSignature(crai, data, sig)) return true;
      } catch (SSHException ssh) {
        continue;
      } catch (NegativeArraySizeException neg) {
        continue;
      }
    }
    return false;
  }

  public AuthorizedKeysManager() {}
}
