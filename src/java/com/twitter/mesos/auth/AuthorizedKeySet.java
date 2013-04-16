package com.twitter.mesos.auth;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import net.lag.crai.Crai;
import net.lag.craijce.CraiJCE;
import net.lag.jaramiko.DSSKey;
import net.lag.jaramiko.Message;
import net.lag.jaramiko.PKey;
import net.lag.jaramiko.RSAKey;
import net.lag.jaramiko.SSHException;

import org.apache.commons.codec.binary.Base64;

import com.twitter.common.base.Either;
import com.twitter.common.base.Either.Transformer;
import com.twitter.common.base.Function;
import com.twitter.common.collections.Pair;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manages state around public keys of role accounts.
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

    // TODO(vinod): This is a hack because verifySSHSignature below may throw
    // OutOfMemory error if its not fed a valid RSA/DSA signature.
    // NOTE: getInt() on the signature returns the length of type of
    // the signature (e.g. ssh-rsa, ssh-dsa).
    int length = sig.getInt();
    if (length > 100) {
      LOG.log(Level.WARNING, "Invalid signature. Length: " + length);
      return false;
    }

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
   *
   * @param lines Key lines to load, in the OpenSSH key file format.
   * @return a keyset comprised of keys parsed from the given {@code lines}.
   * @throws KeyParseException If there was a problem parsing the line as an SSH key.
   */
  public static AuthorizedKeySet createFromKeys(Iterable<String> lines)
      throws KeyParseException {
    checkNotNull(lines);

    return new AuthorizedKeySet(Optional.presentInstances(Iterables.transform(lines,
        new Function<String, Optional<PKey>>() {
          @Override public Optional<PKey> apply(String item) {
            return parseKey(item).map(
                new Transformer<Pair<String, Exception>, PKey, Optional<PKey>>() {
                  @Override public Optional<PKey> mapLeft(Pair<String, Exception> failure) {
                    LOG.log(Level.WARNING, failure.getFirst(), failure.getSecond());
                    return Optional.absent();
                  }
                  @Override public Optional<PKey> mapRight(PKey key) {
                    return Optional.of(key);
                  }
                });
          }
        })));
  }

  private static final Splitter PUB_KEY_SPLITTER = Splitter.on(CharMatcher.WHITESPACE).limit(3);

  private static Either<Pair<String, Exception>, PKey> parseKey(String line) {
    List<String> fields = ImmutableList.copyOf(PUB_KEY_SPLITTER.split(line));
    if (fields.size() < 2) {
      return failure("Invalid number of fields on line: " + line, null);
    }

    String algorithm = fields.get(0);
    String encodedKey = fields.get(1);
    try {
      if ("ssh-rsa".equals(algorithm)) {
        return Either.right(RSAKey.createFromData(Base64.decodeBase64(encodedKey)));
      } else if ("ssh-dss".equals(algorithm)) {
        return Either.right(DSSKey.createFromData(Base64.decodeBase64(encodedKey)));
      } else {
        return failure("Unknown key type: " + algorithm, null);
      }
    } catch (SSHException e) {
      return failure("Failed to create key for line: " + line, e);
    } catch (NumberFormatException e) {
      // TODO(John Sirois): Patch jaramiko to handle and throw some form of SSHException
      return failure("Failed to create key for line: " + line, e);
    } catch (NegativeArraySizeException e) {
      // TODO(John Sirois): Patch jaramiko to handle and throw some form of SSHException
      return failure("Failed to create key for line: " + line, e);
    }
  }

  private static Either<Pair<String, Exception>, PKey> failure(
      String message,
      @Nullable Exception exception) {

    return Either.left(Pair.of(message, exception));
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
