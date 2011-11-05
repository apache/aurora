package com.twitter.mesos.angrybird;

import java.io.IOException;
import java.text.ParseException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ZKDatabase;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.collections.Pair;
import com.twitter.common.zookeeper.testing.ZooKeeperTestServer;

public class AngryBirdZooKeeperServer extends ZooKeeperTestServer {

  private static final Logger LOG = Logger.getLogger(AngryBirdZooKeeperServer.class.getName());

  public AngryBirdZooKeeperServer(int port, ShutdownRegistry shutdownRegistry) throws IOException {
    super(port, shutdownRegistry);
  }

  /**
   * Expires the zookeeper session of the given end point.
   * For now, this only supports those endpoints that store their host:port
   * in the znode (e.g: candidates participating in leader election)
   * @param host
   * @param port
   * @return Returns the session id of the znode that matches the endpoint
   */
  public final String expireClientSession(String host, int port) {
    Long sessionId =  getSessionID(host, port);

    if(sessionId == null) {
      return null;
    }

    LOG.log(Level.INFO, "Closing session "+ sessionId + " for endpoint " + host + ":" + port);
    zooKeeperServer.closeSession(sessionId.longValue());

    return sessionId.toString();
  }

  // TOOD(vinod): Instead of (host, port) args use the more generic byte[] as args
  // so that comparison can be made on znodes that are ServerSet ephemerals
  private final Long getSessionID(String host, int port) {
    ZKDatabase zk_db = zooKeeperServer.getZKDatabase();

    for (Long sessionId : zk_db.getSessions()) {
      for (String path: zk_db.getEphemerals(sessionId)) {
        LOG.log(Level.INFO, "SessionId:"+sessionId+" Path:" + path);
        try {
          String data = new String(zk_db.getData(path, new Stat(), null));
          LOG.log(Level.INFO, "Data in znode: "+data);

          Pair<String, Integer> endPoints = parseEndPoints(data);
          LOG.log(Level.INFO, "Extracted "+ endPoints.getFirst() + ":" + endPoints.getSecond());

          if(endPoints.getFirst().equals(host) && endPoints.getSecond().intValue() == port) {
            LOG.log(Level.INFO, "Matching sessionId:"+ sessionId +
                    " found for endpoint " + host + ":" + port);
            return sessionId;
          }
        } catch (NoNodeException e) {
          LOG.log(Level.SEVERE, "Exception getting data for Path:" + path + " : " + e);
        } catch (ParseException e) {
          LOG.log(Level.SEVERE, "Exception parsing data: "+ e);
        } catch (NumberFormatException e) {
          LOG.log(Level.SEVERE, "Exception in url format "+ e);
        }
      }
    }

    return null;
  }

  private final Pair<String, Integer> parseEndPoints(String data)
    throws ParseException, NumberFormatException {
    int index = -1;

    if((index = data.indexOf("@")) != -1) {
      data = data.substring(index+1);
      index  = data.indexOf(":");
      if(index == -1) {
        throw new ParseException("Error parsing url: " + data, index);
      }
      return Pair.of(data.substring(0, index), Integer.parseInt(data.substring(index+1)));
    }
    else {
      // TODO(vinod): Implement parsing for other znode data formats
      throw new ParseException("Unknown znode data: " + data,index);
    }
  }
}