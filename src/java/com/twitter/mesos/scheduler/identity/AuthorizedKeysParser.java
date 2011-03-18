package com.twitter.mesos.scheduler.identity;

import com.twitter.mesos.scheduler.identity.AuthorizedKeysManager;

import java.io.File;
import java.io.InputStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.lang.NumberFormatException;
import java.util.logging.Logger;

import net.lag.jaramiko.DSSKey;
import net.lag.jaramiko.RSAKey;
import net.lag.jaramiko.PKey;
import net.lag.jaramiko.Util;

import com.google.common.io.Files;
import com.google.common.base.Charsets;
import org.apache.commons.lang.StringUtils;

/**
 * Parses output from Twitter's LDAP update users script.
 *
 * @author Brian Wickman
 */
public class AuthorizedKeysParser {
  private static final Logger LOG = Logger.getLogger(AuthorizedKeysParser.class.getName());

  /**
   * Load keys from file as role, and deposit them into keyMgr. Expected to be in
   * Twitter's authorized keys format specifically ssh-{rsa,dss} {key blob} {user@machine}
   *
   * @param keyMgr
   * @param file
   * @param role
   */
  public static void loadFileInto(AuthorizedKeysManager keyMgr, File file, String role) {
    try {
      List<String> lines = Files.readLines(file, Charsets.US_ASCII);
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

        if (fields[0].equals("ssh-rsa")) {
          try {
            PKey key = RSAKey.createFromBase64(fields[1]);
            keyMgr.add(role, user[0], key);
          } catch (NumberFormatException x) {
            LOG.warning("Unable to parse RSAKey from: " + fields[1]);
          }
        } else if (fields[0].equals("ssh-dss")) {
          try {
            PKey key = DSSKey.createFromBase64(fields[1]);
            keyMgr.add(role, user[0], key);
          } catch (NumberFormatException x) {
            LOG.warning("Unable to parse DSSKey from: " + fields[1]);
          }
        } else {
          LOG.warning("Unsupported key type: " + fields[0]);
        }
      }
    } catch (IOException x) {
      LOG.warning("Unable to read file: " + file.getPath());
    }
  }

  /**
   * Load all files from directory.  One file per role, and each line of each file represents
   * an individual user.  There can be multiple keys for each (role, user) pair.
   *
   * @param dir
   */
  public static AuthorizedKeysManager loadDir(File dir) {
    AuthorizedKeysManager keyMgr = new AuthorizedKeysManager();

    if (!dir.isDirectory()) {
      LOG.warning("Got file instead of directory: " + dir.getPath());
      return null;
    }

    File files[] = dir.listFiles();
    for (int i = 0; i < files.length; i++) {
      LOG.info("Loading role: " + files[i].getName());
      loadFileInto(keyMgr, files[i], files[i].getName());
    }

    return keyMgr;
  }
}
