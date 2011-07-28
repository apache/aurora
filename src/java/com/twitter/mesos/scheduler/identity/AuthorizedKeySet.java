package com.twitter.mesos.scheduler.identity;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import net.lag.crai.Crai;
import net.lag.craijce.CraiJCE;
import net.lag.jaramiko.DSSKey;
import net.lag.jaramiko.Message;
import net.lag.jaramiko.PKey;
import net.lag.jaramiko.RSAKey;
import net.lag.jaramiko.SSHException;

import org.apache.commons.lang.StringUtils;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;

/**
 * Manages state around public keys of role accounts.
 *
 * @author Brian Wickman
 */
public class AuthorizedKeySet {

  private static final Logger LOG = Logger.getLogger(AuthorizedKeySet.class.getName());

  private final Set<PKey> keys;
  private final Crai crai = new CraiJCE();

  /**
   * Creates a new key set that manages the given {@code keys}.
   *
   * @param keys Keys to store in the set.
   */
  public AuthorizedKeySet(Set<PKey> keys) {
    this.keys = ImmutableSet.copyOf(keys);
  }

  /**
   * Given a role, verify that the provided signature is a valid signature
   * of the data from a key associated with the role.
   *
   * @param data Data that was signed.
   * @param signature Signature of the data.
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
      }
    }
    return false;
  }

  /**
   * Identical to {@link #createFromKeys(Iterable)}, but for a single key.
   *
   * @param line Key line to load, in the OpenSSH key file format.
   * @throws KeyParseException If there was a problem parsing the line as an SSH key.
   */
  public static AuthorizedKeySet createFromKey(String line) throws KeyParseException {
    checkNotBlank(line);
    return createFromKeys(Arrays.asList(line));
  }

  /**
   * Creates a key set associatd with multiple keys.
   * Expected to be in Twitter's authorized keys format specifically ssh-{rsa,dss} {key blob}
   * {user@machine}
   *
   * @param lines Key lines to load, in the OpenSSH key file format.
   * @throws KeyParseException If there was a problem parsing the line as an SSH key.
   */
  public static AuthorizedKeySet createFromKeys(Iterable<String> lines)
      throws KeyParseException {
    checkNotNull(lines);

    Set<PKey> keys = Sets.newHashSet();
    for (String line : lines) {
      String[] fields = StringUtils.split(line);
      if (fields.length != 3) {
        LOG.warning("Invalid number of fields on line: " + line);
        continue;
      }

      String[] user = StringUtils.split(fields[2], "@");
      if (user.length != 2) {
        LOG.warning("Invalid user: " + fields[2]);
        // Invalid user.
        continue;
      }

      PKey key;
      try {
        if (fields[0].equals("ssh-rsa")) {
          key = RSAKey.createFromBase64(fields[1]);
        } else if (fields[0].equals("ssh-dss")) {
          key = DSSKey.createFromBase64(fields[1]);
        } else {
          throw new KeyParseException("Unsupported key type: " + fields[0]);
        }
      } catch (SSHException e) {
        throw new KeyParseException("Failed to create key", e);
      }
      keys.add(key);
    }

    return new AuthorizedKeySet(keys);
  }

  /**
   * Creaes a key set, by loading keys from an OpenSSH {@code authorized_keys} file.
   * See {@link #createFromKeys(Iterable)} for more information.
   *
   * @param file File to load keys from, in the OpenSSH key file format.
   * @throws KeyParseException If there was a problem parsing the line as an SSH key.
   */
  public static AuthorizedKeySet createFromFile(File file) throws KeyParseException {
    try {
      return createFromKeys(Files.readLines(file, Charsets.US_ASCII));
    } catch (IOException e) {
      throw new KeyParseException("Failed to read file " + file, e);
    }
  }

  /**
   * Indicates failure to load SSH key information from a data source.
   */
  public static class KeyParseException extends Exception {
    public KeyParseException(String msg) {
      super(msg);
    }

    public KeyParseException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}
