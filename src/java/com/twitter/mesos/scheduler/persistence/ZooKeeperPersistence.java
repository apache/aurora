package com.twitter.mesos.scheduler.persistence;

import com.google.common.base.Preconditions;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Persistence layer that stores data in ZooKeeper.
 *
 * @author wfarner
 */
public class ZooKeeperPersistence implements PersistenceLayer<byte[]> {
  private final static Logger LOG = Logger.getLogger(ZooKeeperPersistence.class.getName());

  private final String path;
  private final int version;

  private final ZooKeeperClient zkClient;

  public ZooKeeperPersistence(ZooKeeperClient zkClient, String path, int version) {
    this.zkClient = Preconditions.checkNotNull(zkClient);
    this.version = version;
    this.path = MorePreconditions.checkNotBlank(path);
  }

  @Override
  public byte[] fetch() throws PersistenceException {
    Stat stat = statFile();

    try {
      return getZooKeeper().getData(path, false, stat);
    } catch (KeeperException e) {
      logAndThrow(String.format("Failed to read path (%s) from ZooKeeper,", path), e);
    } catch (InterruptedException e) {
      logAndThrow(String.format("Interrupted while reading path (%s) from ZooKeeper.", path), e);
    }

    return null;
  }

  @Override
  public void commit(byte[] data) throws PersistenceException {
    if (statFile() == null) {
      try {
        getZooKeeper().create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
      } catch (KeeperException e) {
        logAndThrow(String.format("Failed to create persistence node (%s) in ZooKeeper.", path), e);
      } catch (InterruptedException e) {
        logAndThrow(String.format("Interrupted while creating node (%s) in ZooKeeper.", path), e);
      }
    } else {
      try {
        Stat stat = getZooKeeper().setData(path, data, version);
        LOG.info("Successfully committed to ZooKeeper: " + stat);
      } catch (KeeperException e) {
        logAndThrow("Error while storing data to ZooKeeper.", e);
      } catch (InterruptedException e) {
        logAndThrow("Interrupted while storing data to ZooKeeper.", e);
      }
    }
  }

  private ZooKeeper getZooKeeper() throws PersistenceException {
    try {
      return zkClient.get();
    } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
      logAndThrow("Failed to connect to ZooKeeper", e);
    } catch (InterruptedException e) {
      logAndThrow("Interrupted while connecting to ZooKeeper." , e);
    }

    // This should never be reached.
    throw new RuntimeException("Unexpected state.");
  }

  private Stat statFile() throws PersistenceException {
    try {
      return getZooKeeper().exists(path, false);
    } catch (KeeperException e) {
      logAndThrow(String.format("Failed to stat path (%s) in ZooKeeper.", path), e);
    } catch (InterruptedException e) {
      logAndThrow(String.format(
          "Interrupted while checking whether path (%s) exists in ZooKeeper", path), e);
    }

    // This should never be reached.
    throw new RuntimeException("Unexpected state.");
  }

  private static void logAndThrow(String msg, Throwable t) throws PersistenceException {
    LOG.log(Level.SEVERE, msg, t);
    throw new PersistenceException(msg, t);
  }
}
