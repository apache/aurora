package com.twitter.mesos.angrybird;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.common.testing.junit4.TearDownTestCase;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.testing.TearDownRegistry;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.ZooKeeperUtils;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Vinod Kone
 */
public class AngryBirdZooKeeperTest extends TearDownTestCase {

  private static final Logger LOG = Logger.getLogger(AngryBirdZooKeeperTest.class.getName());

  private AngryBirdZooKeeperServer zkServer;
  private static final List<ACL> ACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;
  private Map<ZooKeeperClient, String> zkPaths;
  private static String pathPrefix = "/dummy/sequence";

  @Before
  public final void setUp() throws Exception {
    final ShutdownRegistry shutdownRegistry = new TearDownRegistry(this);

    zkServer = new AngryBirdZooKeeperServer(0, shutdownRegistry);
    zkServer.startNetwork();

    zkPaths = new HashMap<ZooKeeperClient, String>();
  }

  @Test
  public void testEndpointExpiration() throws Exception {

    // Create some sequential nodes.
    ZooKeeperClient zkClient1 = createZKClient("1@1234:81".getBytes());
    ZooKeeperClient zkClient2 = createZKClient("2@1234:82".getBytes());
    ZooKeeperClient zkClient3 = createZKClient("3@1235:81".getBytes());

    // Expire the leader's session.
    zkServer.expireSession("1234", 81);

    // Make sure non-matching sessions are not expired.
    zkClient2.get().exists(zkPaths.get(zkClient2), null);
    zkClient3.get().exists(zkPaths.get(zkClient3), null);

    // Check that the matching session has expired.
    try {
      zkClient1.get().exists(zkPaths.get(zkClient1), null);
      fail("Matching session has not expired!");
    } catch (KeeperException e) {}
  }

  @Test
  public void testLeaderExpiration() throws Exception {

    // Create some sequential nodes.
    ZooKeeperClient zkClient1 = createZKClient();
    ZooKeeperClient zkClient2 = createZKClient();
    ZooKeeperClient zkClient3 = createZKClient();

    // Expire the leader's session.
    zkServer.expireCandidateSession(pathPrefix, true);

    // Make sure the follower sessions are not expired.
    zkClient2.get().exists(zkPaths.get(zkClient2), null);
    zkClient3.get().exists(zkPaths.get(zkClient3), null);

    // Check that leader's session has expired.
    try {
      zkClient1.get().exists(zkPaths.get(zkClient1), null);
      fail("Leader session has not expired!");
    } catch (KeeperException e) {}
  }

  @Test
  public void testFollowerExpiration() throws Exception {

    // Create some sequential nodes.
    ZooKeeperClient zkClient1 = createZKClient();
    ZooKeeperClient zkClient2 = createZKClient();
    ZooKeeperClient zkClient3 = createZKClient();

    // Expire the follower's session.
    zkServer.expireCandidateSession(pathPrefix, false);

    // Make sure the leader's session is not expired.
    zkClient1.get().exists(zkPaths.get(zkClient1), null);

    int numfailed = 0;
    // Check that only one of the followers's sessions has expired.
    try {
      zkClient2.get().exists(zkPaths.get(zkClient2), null);
    } catch (KeeperException e) {
      numfailed++;
    }
    try {
      zkClient3.get().exists(zkPaths.get(zkClient3), null);
    } catch (KeeperException e) {
      numfailed++;
    }
    assertEquals(1, numfailed);
  }


  private ZooKeeperClient createZKClient() throws Exception {
    return createZKClient(null);
  }

  // Creates a zookeeper client and creates a znode with the given path.
  private ZooKeeperClient createZKClient(byte[] data) throws Exception {
    final ZooKeeperClient zkClient = zkServer.createClient();

    // Create prefix path if it doesn't exist.
    ZooKeeperUtils.ensurePath(zkClient, ACL, pathPrefix);

    // Create the ephemeral node.
    String path = zkClient.get().create(pathPrefix, data, ACL, CreateMode.EPHEMERAL_SEQUENTIAL);

    LOG.info("Created ephemeral node: " + path);
    zkPaths.put(zkClient, path);

    return zkClient;
  }
}
