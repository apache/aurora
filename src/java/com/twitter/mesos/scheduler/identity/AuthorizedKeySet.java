package com.twitter.mesos.scheduler.identity;

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import net.lag.crai.Crai;
import net.lag.craijce.CraiJCE;
import net.lag.jaramiko.DSSKey;
import net.lag.jaramiko.Message;
import net.lag.jaramiko.PKey;
import net.lag.jaramiko.RSAKey;
import net.lag.jaramiko.SSHException;

import org.apache.commons.lang.StringUtils;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manages state around public keys of role accounts.
 *
 * @author Brian Wickman
 */
public class AuthorizedKeySet {

  private static final Logger LOG = Logger.getLogger(AuthorizedKeySet.class.getName());

  @VisibleForTesting final ImmutableSet<PKey> keys;
  private final Crai crai = new CraiJCE();

  /**
   * Creates a new key set that manages the given {@code keys}.
   *
   * @param keys Keys to store in the set.
   */
  public AuthorizedKeySet(Iterable<PKey> keys) {
    this.keys = ImmutableSet.copyOf(keys);
  }

  /**
   * Given a role, verify that the provided signature is a valid signature
   * of the data from a key associated with the role.
   *
   * @param data Data that was signed.
   * @param signature Signature of the data.
   * @return {@code true} if the data is signed by a key in this keyset.
   */
  public boolean verify(byte[] data, byte[] signature) {
    checkNotNull(data);
    checkNotNull(signature);

    Message sig = new Message(signature);

    for (PKey key : keys) {
      sig.rewind();
      try {
        if (key.verifySSHSignature(crai, data, sig)) {
          return true;
        }
      } catch (SSHException e) {
        LOG.log(Level.WARNING, "Failed to perform signature verification.", e);
      } catch (NegativeArraySizeException e) {
        // TODO(John Sirois): Patch jaramiko to handle and throw some form of SSHException
        LOG.log(Level.WARNING, "Failed to perform signature verification.", e);
      }
    }
    return false;
  }

  /**
   * Creates a key set associated with multiple keys.
   * Expected to be in Twitter's authorized keys format, specifically: ssh-{rsa,dss} {key blob}
   * ({user@machine}).
   *
   * @param lines Key lines to load, in the OpenSSH key file format.
   * @return a keyset comprised of keys parsed from the given {@code lines}.
   * @throws KeyParseException If there was a problem parsing the line as an SSH key.
   */
  public static AuthorizedKeySet createFromKeys(Iterable<String> lines)
      throws KeyParseException {
    checkNotNull(lines);

    Set<PKey> keys = Sets.newHashSet();
    for (String line : lines) {
      String[] fields = StringUtils.split(line);
      if (fields.length < 2 || fields.length > 3) {
        LOG.warning("Invalid number of fields on line: " + line);
        continue;
      }

      PKey key;
      try {
        if ("ssh-rsa".equals(fields[0])) {
          key = RSAKey.createFromBase64(fields[1]);
        } else if ("ssh-dss".equals(fields[0])) {
          key = DSSKey.createFromBase64(fields[1]);
        } else {
          LOG.warning(String.format("Unknown key type: %s", fields[0]));
          continue;
        }
      } catch (SSHException e) {
        LOG.log(Level.WARNING, "Failed to create key for line: " + line, e);
        continue;
      } catch (NumberFormatException e) {
        // TODO(John Sirois): Patch jaramiko to handle and throw some form of SSHException
        LOG.log(Level.WARNING, "Failed to create key for line: " + line, e);
        continue;
      }
      keys.add(key);
    }

    return new AuthorizedKeySet(keys);
  }

  /**
   * Indicates failure to load SSH key information from a data source.
   */
  public static class KeyParseException extends Exception {
    public KeyParseException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}
