package com.twitter.mesos.angrybird;

import java.io.IOException;

import com.twitter.mesos.angrybird.gen.ExpireResponse;
import com.twitter.mesos.angrybird.gen.ExpireRequest;
import com.twitter.mesos.angrybird.gen.ServerPortResponse;
import com.twitter.mesos.angrybird.gen.ShutdownResponse;
import com.twitter.mesos.angrybird.gen.StartupResponse;
import com.twitter.mesos.angrybird.gen.RestartResponse;
import com.twitter.mesos.angrybird.gen.ZooKeeperThriftServer;

import static com.twitter.mesos.angrybird.gen.ResponseCode.OK;
import static com.twitter.mesos.angrybird.gen.ResponseCode.ERROR;

import com.twitter.util.Future;

public class AngryBirdZooKeeperThriftService implements ZooKeeperThriftServer.ServiceIface {

  private final AngryBirdZooKeeperServer zk_server;

  public AngryBirdZooKeeperThriftService(AngryBirdZooKeeperServer zk_server) {
    this.zk_server = zk_server;
  }

  @Override
  public Future<ServerPortResponse> getZooKeeperServerPort() {
    ServerPortResponse response = new ServerPortResponse();

    int port = zk_server.getPort();

    response.setResponseCode(OK)
        .setPort(port);

    return Future.value(response);
  }

  @Override
  public Future<ExpireResponse> expireSession(ExpireRequest expireRequest) {
    ExpireResponse response = new ExpireResponse();

    String sessionId = zk_server.expireClientSession(expireRequest.host, expireRequest.port);

    if (sessionId != null) {
      response.setResponseCode(OK)
          .setSessionid(sessionId);
    } else {
      response.setResponseCode(ERROR)
          .setSessionid("");
    }

    return Future.value(response);
  }

  @Override
  public Future<StartupResponse> startup() {
    StartupResponse response = new StartupResponse();

    try {
      zk_server.startNetwork();
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

    zk_server.shutdownNetwork();

    response.setResponseCode(OK);

    return Future.value(response);
  }

  @Override
  public Future<RestartResponse> restart() {
    RestartResponse response = new RestartResponse();

    try {
      zk_server.restartNetwork();
      response.setResponseCode(OK);
    } catch (IOException e) {
      response.setResponseCode(ERROR);
    } catch (InterruptedException e) {
      response.setResponseCode(ERROR);
    }
    return Future.value(response);
  }
}