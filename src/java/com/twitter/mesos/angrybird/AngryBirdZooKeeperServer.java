package com.twitter.mesos.angrybird;

import java.io.IOException;
import java.text.ParseException;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.net.HostAndPort;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ZKDatabase;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.zookeeper.testing.ZooKeeperTestServer;

/**
 * ZooKeeper server test harness for angrybird.
 */
public class AngryBirdZooKeeperServer extends ZooKeeperTestServer {

  private static final Logger LOG = Logger.getLogger(AngryBirdZooKeeperServer.class.getName());

  public AngryBirdZooKeeperServer(int port, ShutdownRegistry shutdownRegistry) throws IOException {
    super(port, shutdownRegistry);
  }

  /**
   * Expires the zookeeper session of the given end point.
   * For now, this only supports those endpoints that store their host:port in the znode.
   *
   * @param host ipaddress of the endpoint stored in the znode
   * @param port port of the endpoint stored in the znode
   * @return Returns the session id of the znode that matches the endpoint
   */
  public final Long expireSession(String host, int port) {
    Long sessionId = getSessionID(host, port);
    return closeSession(sessionId);
  }

  /**
   * Expires zookeeper sessions of candidates that uses sequential
   * zk nodes for leader election.
   *
   * @param path the zookeeper path that is used for leader election
   * @param isLeader are we looking for the leader or follower session?
   * @return Returns the sesion id of the matching candidate.
   */
  public final Long expireCandidateSession(String path, boolean isLeader) {
    Long sessionId = isLeader ? getLeaderSessionID(path) : getFollowerSessionID(path);
    return closeSession(sessionId);
  }

  private Long closeSession(@Nullable Long sessionId) {
    if (sessionId == null) {
      LOG.warning("No session found for expiration!");
      return null;
    }

    LOG.info("Closing session: " + sessionId);
    zooKeeperServer.closeSession(sessionId.longValue());

    return sessionId;
  }

  /**
   * Returns the session whose corresponding znode has "host:port" as its data
   * i.e, for master and log.
   *
   * @param host ip address of the endpoint
   * @param port endpoint port
   * @return Returns session id of the corresponding zk session if a match is found.
   * Otherwise null returned.
   */
  @Nullable
  private Long getSessionID(String host, int port) {
    // TODO(vinod): Instead of (host, port) args use the more generic byte[] as args
    // so that comparison can be made on znodes that are ServerSet ephemerals

    ZKDatabase zkDb = zooKeeperServer.getZKDatabase();

    for (Long sessionId : zkDb.getSessions()) {
      for (String path: zkDb.getEphemerals(sessionId)) {
        LOG.info("SessionId:" + sessionId + " Path:" + path);
        try {
          String data = new String(zkDb.getData(path, new Stat(), null));
          LOG.info("Data in znode: " + data);

          HostAndPort endpoint = parseEndpoint(data);
          LOG.info("Extracted endpoint " + endpoint);

          if (endpoint.getHostText().equals(host) && endpoint.getPort() == port) {
            LOG.info(String.format(
                "Matching session id %s found for endpoint %s:%s", sessionId, host, port));
            return sessionId;
          }
        } catch (NoNodeException e) {
          LOG.severe("Exception getting data for Path:" + path + " : " + e);
        } catch (ParseException e) {
          LOG.severe("Exception parsing data: " + e);
        } catch (NumberFormatException e) {
          LOG.severe("Exception in url format " + e);
        }
      }
    }

    return null;
  }

  /**
   * Return the session id of the leader candidate
   * NOTE: Leader is assumed to be the node with the minimum sequence number
   *
   * @param zkPath Znode path prefix of the candidates
   * @return Returns session id of the corresponding zk session if a match is found.
   * Otherwise returns null.
   */
  @Nullable
  private Long getLeaderSessionID(String zkPath) {
    ZKDatabase zkDb = zooKeeperServer.getZKDatabase();
    Long leaderSessionId = null;
    Long masterSeq = Long.MAX_VALUE;

    // Reg-ex pattern for sequence numbers in znode paths.
    Pattern pattern = Pattern.compile("\\d+$");

    // First find the session id of the leading scheduler.
    for (Long sessionId : zkDb.getSessions()) {
      for (String path: zkDb.getEphemerals(sessionId)) {
        if (path.contains(zkPath)) {
          try {
            // Get the sequence number.
            Matcher matcher = pattern.matcher(path);
            if (matcher.find()) {
              LOG.info("Pattern matched path: " + path + " session: " + sessionId);
              Long seq = Long.parseLong(matcher.group());
              if (seq < masterSeq) {
                masterSeq = seq;
                leaderSessionId = sessionId;
              }
            }
          } catch (NumberFormatException e) {
            LOG.severe("Exception formatting sequence number " + e);
          }
        }
      }
    }

    if (leaderSessionId != null) {
      LOG.info(String.format("Found session leader for %s: %s", zkPath, leaderSessionId));
    }

    return leaderSessionId;
  }

  /**
   * Return the session id of a follower candidate
   * NOTE: Follower is selected at random.
   *
   * @param zkPath Znode path prefix of the candidates
   * @return Returns session id of the corresponding zk session if a match is found.
   * Otherwise returns null.
   */
  @Nullable
  private Long getFollowerSessionID(String zkPath) {
    Long leaderSessionId = getLeaderSessionID(zkPath);
    if (leaderSessionId == null) {
      return null;
    }

    ZKDatabase zkDb = zooKeeperServer.getZKDatabase();

    for (Long sessionId : zkDb.getSessions()) {
      if (sessionId.equals(leaderSessionId)) {
        continue;
      }
      for (String path: zkDb.getEphemerals(sessionId)) {
        if (path.contains(zkPath)) {
          LOG.info(String.format("Found session follower for %s: %s", zkPath, sessionId));
          return sessionId;
        }
      }
    }

    return null;
  }

  private HostAndPort parseEndpoint(String data) throws ParseException {

    int index = data.indexOf("@");
    if (index != -1) {
      try {
        return HostAndPort.fromString(data.substring(index + 1));
      } catch (IllegalArgumentException e) {
        throw new ParseException("Failed to parse endpoint " + data, index + 1);
      }
    } else {
      // TODO(vinod): Implement parsing for other znode data formats
      throw new ParseException("Unknown znode data: " + data, index);
    }
  }
}
