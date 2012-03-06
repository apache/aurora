package com.twitter.mesos.angrybird;

import java.io.IOException;

import com.twitter.mesos.angrybird.gen.ExpireCandidateRequest;
import com.twitter.mesos.angrybird.gen.ExpireRequest;
import com.twitter.mesos.angrybird.gen.ExpireResponse;
import com.twitter.mesos.angrybird.gen.RestartResponse;
import com.twitter.mesos.angrybird.gen.ServerPortResponse;
import com.twitter.mesos.angrybird.gen.ShutdownResponse;
import com.twitter.mesos.angrybird.gen.StartupResponse;
import com.twitter.mesos.angrybird.gen.ZooKeeperThriftServer;
import com.twitter.util.Future;

import static com.twitter.mesos.angrybird.gen.Candidate.LEADER;
import static com.twitter.mesos.angrybird.gen.ResponseCode.ERROR;
import static com.twitter.mesos.angrybird.gen.ResponseCode.OK;

/**
 * Thrift interface for the angrybird ZooKeeper server.
 */
public class AngryBirdZooKeeperThriftService implements ZooKeeperThriftServer.ServiceIface {

  private final AngryBirdZooKeeperServer zkServer;

  /**
   * Creates a new angrybird thrift server
   *
   * @param zkServer Thrift server to interact with.
   */
  public AngryBirdZooKeeperThriftService(AngryBirdZooKeeperServer zkServer) {
    this.zkServer = zkServer;
  }

  @Override
  public Future<ServerPortResponse> getZooKeeperServerPort() {
    ServerPortResponse response = new ServerPortResponse();

    int port = zkServer.getPort();

    response.setResponseCode(OK)
        .setPort(port);

    return Future.value(response);
  }

  @Override
  public Future<ExpireResponse> expireSession(ExpireRequest expireRequest) {
    ExpireResponse response = new ExpireResponse();

    Long sessionId = zkServer.expireSession(expireRequest.host, expireRequest.port);

    if (sessionId != null) {
      response.setResponseCode(OK).setSessionid(sessionId.longValue());
    } else {
      response.setResponseCode(ERROR).setSessionid(0);
    }

    return Future.value(response);
  }

  @Override
  public Future<ExpireResponse> expireCandidateSession(ExpireCandidateRequest request) {
    ExpireResponse response = new ExpireResponse();

    Long sessionId = zkServer.expireCandidateSession(request.zk_path,
        request.candidate == LEADER);

    if (sessionId != null) {
      response.setResponseCode(OK).setSessionid(sessionId.longValue());
    } else {
      response.setResponseCode(ERROR).setSessionid(0);
    }

    return Future.value(response);
  }

  @Override
  public Future<StartupResponse> startup() {
    StartupResponse response = new StartupResponse();

    try {
      zkServer.startNetwork();
      response.setResponseCode(OK);
    } catch (IOException e) {
      response.setResponseCode(ERROR);
    } catch (InterruptedException e) {
      response.setResponseCode(ERROR);
    }
    return Future.value(response);
  }

  @Override
  public Future<ShutdownResponse> shutdown() {
    ShutdownResponse response = new ShutdownResponse();

    zkServer.shutdownNetwork();

    response.setResponseCode(OK);

    return Future.value(response);
  }

  @Override
  public Future<RestartResponse> restart() {
    RestartResponse response = new RestartResponse();

    try {
      zkServer.shutdownNetwork();
      zkServer.restartNetwork();
      response.setResponseCode(OK);
    } catch (IOException e) {
      response.setResponseCode(ERROR);
    } catch (InterruptedException e) {
      response.setResponseCode(ERROR);
    }
    return Future.value(response);
  }
}
