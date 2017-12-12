/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.events;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.ImmutableMap;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.events.WebhookInfo.WebhookInfoBuilder;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseHeaders;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.channel.DefaultKeepAliveStrategy;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class WebhookTest {
  private static final String STATIC_URL = "http://localhost:8080/";
  private static final Integer TIMEOUT = 5000;
  private static final Map<String, String> HEADERS = ImmutableMap.of(
      "Content-Type", "application/vnd.kafka.json.v1+json",
      "Producer-Type", "reliable"
  );
  private static final IScheduledTask TASK = TaskTestUtil.makeTask("id", TaskTestUtil.JOB);
  private static final TaskStateChange CHANGE = TaskStateChange.initialized(TASK);
  private static final TaskStateChange CHANGE_OLD_STATE = TaskStateChange
      .transition(TASK, ScheduleStatus.FAILED);
  private static final TaskStateChange CHANGE_LOST = TaskStateChange.transition(
      TaskTestUtil.addStateTransition(TASK, ScheduleStatus.LOST, 1L), ScheduleStatus.LOST);
  private static final String CHANGE_JSON = CHANGE_OLD_STATE.toJson();
  private static final String CHANGE_LOST_JSON  = CHANGE_LOST.toJson();
  // Below are test fixtures for WebhookInfoBuilders. Callers will need to specify the desired
  // targetURL and build manually to get the desired WebhookInfo. We do this because we allocate
  // an ephemeral port for our test Jetty server, meaning we cannot specify WebhookInfo statically.
  // Test fixture for WebhookInfo without a whitelist, thus all task statuses are implicitly
  // whitelisted.
  private static final WebhookInfoBuilder WEBHOOK_INFO_BUILDER = WebhookInfo
      .newBuilder()
      .setHeaders(HEADERS)
      .setTimeout(TIMEOUT);
  // Test fixture for WebhookInfo in which only "LOST" and "FAILED" task statuses are explicitly
  // whitelisted.
  private static final WebhookInfoBuilder WEBHOOK_INFO_WITH_WHITELIST_BUILDER = WebhookInfo
      .newBuilder()
      .setHeaders(HEADERS)
      .setTimeout(TIMEOUT)
      .addWhitelistedStatus("LOST")
      .addWhitelistedStatus("FAILED");
  // Test fixture for WebhookInfo in which all task statuses are whitelisted by wildcard character.
  private static final WebhookInfoBuilder WEBHOOK_INFO_WITH_WILDCARD_WHITELIST_BUILDER =
      WebhookInfo
          .newBuilder()
          .setHeaders(HEADERS)
          .setTimeout(TIMEOUT)
          .addWhitelistedStatus("*");

  private Server jettyServer;
  private AsyncHttpClient httpClient;
  private FakeStatsProvider statsProvider;

  /**
   * Wraps an {@link AsyncHandler} to allow the caller to wait for
   * {@link AsyncHandler#onThrowable} to complete. This is necessary because exceptions cause the
   * future to complete before {@code onThrowable} can finish. See AURORA-1961 for context.
   */
  static class WebhookOnThrowableHandler<T> implements AsyncHandler<T> {
    private final AsyncHandler<T> handler;
    private final CountDownLatch latch = new CountDownLatch(1);

    private boolean onThrowableFinished = false;

    WebhookOnThrowableHandler(AsyncHandler<T> handler) {
      this.handler = handler;
    }

    boolean hasOnThrowableFinished(long timeout, TimeUnit unit) {
      try {
        latch.await(timeout, unit);
      } catch (InterruptedException e) {
        // No-op
      }

      return onThrowableFinished;
    }

    @Override
    public void onThrowable(Throwable t) {
      handler.onThrowable(t);
      onThrowableFinished = true;
      latch.countDown();
    }

    @Override
    public State onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
      return handler.onBodyPartReceived(bodyPart);
    }

    @Override
    public State onStatusReceived(HttpResponseStatus responseStatus) throws Exception {
      return handler.onStatusReceived(responseStatus);
    }

    @Override
    public State onHeadersReceived(HttpResponseHeaders headers) throws Exception {
      return handler.onHeadersReceived(headers);
    }

    @Override
    public T onCompleted() throws Exception {
      return handler.onCompleted();
    }
  }

  /**
   * Wrap the DefaultAsyncHttpClient for testing so we can complete futures synchronously before
   * validating assertions. Otherwise, we would have to call `Thread.sleep` in our tests after
   * each TaskStateChange.
   */
  static class WebhookAsyncHttpClientWrapper extends DefaultAsyncHttpClient {

    WebhookAsyncHttpClientWrapper(DefaultAsyncHttpClientConfig config) {
      super(config);
    }

    @Override
    public <T> ListenableFuture<T> executeRequest(org.asynchttpclient.Request request,
                                                  AsyncHandler<T> handler) {

      WebhookOnThrowableHandler<T> wrapped = new WebhookOnThrowableHandler<>(handler);
      ListenableFuture<T> future = super.executeRequest(request, wrapped);
      try {
        future.get();
        future.done();
      } catch (InterruptedException | ExecutionException e) {
        // The future threw an exception, wait up to 60 seconds for onThrowable to complete.
        wrapped.hasOnThrowableFinished(60, TimeUnit.SECONDS);
      }
      return future;
    }
  }

  @Before
  public void setUp() throws Exception {
    DefaultAsyncHttpClientConfig testConfig = new DefaultAsyncHttpClientConfig.Builder()
        .setConnectTimeout(TIMEOUT)
        .setHandshakeTimeout(TIMEOUT)
        .setSslSessionTimeout(TIMEOUT)
        .setReadTimeout(TIMEOUT)
        .setRequestTimeout(TIMEOUT)
        .setKeepAliveStrategy(new DefaultKeepAliveStrategy())
        .build();
    httpClient = new WebhookAsyncHttpClientWrapper(testConfig);
    statsProvider = new FakeStatsProvider();
    jettyServer = new Server(0); // Start Jetty server with ephemeral port
  }

  @After
  public void tearDown() throws Exception {
    jettyServer.stop();
  }

  @Test
  public void testTaskChangedStateNoOldState() throws Exception {
    WebhookInfo webhookInfo = buildWebhookInfoWithJettyPort(WEBHOOK_INFO_BUILDER);
    Webhook webhook = new Webhook(httpClient, webhookInfo, statsProvider);

    // Should be a noop as oldState is MIA so this test would have throw an exception.
    // If it does not, then we are good.
    webhook.taskChangedState(CHANGE);
  }

  @Test
  public void testTaskChangedWithOldStateSuccess() throws Exception {
    jettyServer.setHandler(createHandlerThatExpectsContent(CHANGE_JSON));
    jettyServer.start();
    WebhookInfo webhookInfo = buildWebhookInfoWithJettyPort(WEBHOOK_INFO_BUILDER);
    Webhook webhook = new Webhook(httpClient, webhookInfo, statsProvider);

    webhook.taskChangedState(CHANGE_OLD_STATE);

    assertEquals(1, statsProvider.getLongValue(Webhook.ATTEMPTS_STAT_NAME));
    assertEquals(1, statsProvider.getLongValue(Webhook.SUCCESS_STAT_NAME));
    assertEquals(0, statsProvider.getLongValue(Webhook.ERRORS_STAT_NAME));
    assertEquals(0, statsProvider.getLongValue(Webhook.USER_ERRORS_STAT_NAME));
  }

  @Test
  public void testTaskChangedWithOldStateUserError() throws Exception {
    // We expect CHANGE_JSON but get CHANGE_LOST which causes an error code to be returned.
    jettyServer.setHandler(createHandlerThatExpectsContent(CHANGE_JSON));
    jettyServer.start();
    WebhookInfo webhookInfo = buildWebhookInfoWithJettyPort(WEBHOOK_INFO_BUILDER);
    Webhook webhook = new Webhook(httpClient, webhookInfo, statsProvider);

    webhook.taskChangedState(CHANGE_LOST);

    assertEquals(1, statsProvider.getLongValue(Webhook.ATTEMPTS_STAT_NAME));
    assertEquals(0, statsProvider.getLongValue(Webhook.SUCCESS_STAT_NAME));
    assertEquals(0, statsProvider.getLongValue(Webhook.ERRORS_STAT_NAME));
    assertEquals(1, statsProvider.getLongValue(Webhook.USER_ERRORS_STAT_NAME));
  }

  @Test
  public void testTaskChangedWithOldStateError() throws Exception {
    // Don't start Jetty server, send the request to an invalid port to force a ConnectError
    WebhookInfo webhookInfo = buildWebhookInfoWithJettyPort(WEBHOOK_INFO_BUILDER, -1);
    Webhook webhook = new Webhook(httpClient, webhookInfo, statsProvider);

    webhook.taskChangedState(CHANGE_OLD_STATE);

    assertEquals(1, statsProvider.getLongValue(Webhook.ATTEMPTS_STAT_NAME));
    assertEquals(0, statsProvider.getLongValue(Webhook.SUCCESS_STAT_NAME));
    assertEquals(1, statsProvider.getLongValue(Webhook.ERRORS_STAT_NAME));
    assertEquals(0, statsProvider.getLongValue(Webhook.USER_ERRORS_STAT_NAME));
  }

  @Test
  public void testTaskChangeInWhiteList() throws Exception {
    // Verifying TaskStateChange in the whitelist is sent to the configured endpoint.
    jettyServer.setHandler(createHandlerThatExpectsContent(CHANGE_LOST_JSON));
    jettyServer.start();
    WebhookInfo webhookInfo = buildWebhookInfoWithJettyPort(WEBHOOK_INFO_WITH_WHITELIST_BUILDER);
    Webhook webhook = new Webhook(httpClient, webhookInfo, statsProvider);

    webhook.taskChangedState(CHANGE_LOST);

    assertEquals(1, statsProvider.getLongValue(Webhook.ATTEMPTS_STAT_NAME));
    assertEquals(1, statsProvider.getLongValue(Webhook.SUCCESS_STAT_NAME));
    assertEquals(0, statsProvider.getLongValue(Webhook.ERRORS_STAT_NAME));
    assertEquals(0, statsProvider.getLongValue(Webhook.USER_ERRORS_STAT_NAME));
  }

  @Test
  public void testTaskChangeNotInWhiteList() throws Exception {
    // Verifying TaskStateChange not in the whitelist is not sent to the configured endpoint.
    jettyServer.setHandler(createHandlerThatExpectsContent(CHANGE_JSON));
    jettyServer.start();
    WebhookInfo webhookInfo = buildWebhookInfoWithJettyPort(WEBHOOK_INFO_WITH_WHITELIST_BUILDER);
    Webhook webhook = new Webhook(httpClient, webhookInfo, statsProvider);

    webhook.taskChangedState(CHANGE_OLD_STATE);
  }

  private static final String TEST_CONFIG = "{\n"
      + "  \"headers\": {\n"
      + "    \"Content-Type\": \"application/vnd.kafka.json.v1+json\",\n"
      + "    \"Producer-Type\": \"reliable\"\n"
      + "  },\n"
      + "  \"targetURL\": \"http://localhost:8080/\",\n"
      + "  \"timeoutMsec\": 5000\n"
      + "}\n";

  @Test
  public void testParsingWebhookInfo() throws Exception {
    WebhookInfo webhookInfo = WEBHOOK_INFO_BUILDER
        .setTargetURL(STATIC_URL)
        .build();

    WebhookInfo parsedWebhookInfo = WebhookModule.parseWebhookConfig(TEST_CONFIG);
    // Verifying the WebhookInfo parsed from webhook.json file is identical to the WebhookInfo
    // built from WebhookInfoBuilder.
    assertEquals(parsedWebhookInfo.toString(), webhookInfo.toString());
    // Verifying all attributes were parsed correctly.
    assertEquals(parsedWebhookInfo.getHeaders(), webhookInfo.getHeaders());
    assertEquals(parsedWebhookInfo.getTargetURI(), webhookInfo.getTargetURI());
    assertEquals(parsedWebhookInfo.getConnectonTimeoutMsec(),
        webhookInfo.getConnectonTimeoutMsec());
    assertEquals(parsedWebhookInfo.getWhitelistedStatuses(), webhookInfo.getWhitelistedStatuses());
  }

  @Test
  public void testWebhookInfo() throws Exception {
    WebhookInfo webhookInfo = WEBHOOK_INFO_BUILDER
        .setTargetURL(STATIC_URL)
        .build();

    assertEquals(webhookInfo.toString(),
        "WebhookInfo{headers={"
            + "Content-Type=application/vnd.kafka.json.v1+json, "
            + "Producer-Type=reliable"
            + "}, "
            + "targetURI=http://localhost:8080/, "
            + "connectTimeoutMsec=5000, "
            + "whitelistedStatuses=null"
            + "}");
    // Verifying all attributes were set correctly.
    assertEquals(webhookInfo.getHeaders(), HEADERS);
    assertEquals(webhookInfo.getTargetURI(), URI.create(STATIC_URL));
    assertEquals(webhookInfo.getConnectonTimeoutMsec(), TIMEOUT);
    assertFalse(webhookInfo.getWhitelistedStatuses().isPresent());
  }

  @Test
  public void testWebhookInfoWithWhiteList() throws Exception {
    WebhookInfo webhookInfoWithWhitelist = WEBHOOK_INFO_WITH_WHITELIST_BUILDER
        .setTargetURL(STATIC_URL)
        .build();

    assertEquals(webhookInfoWithWhitelist.toString(),
        "WebhookInfo{headers={"
            + "Content-Type=application/vnd.kafka.json.v1+json, "
            + "Producer-Type=reliable"
            + "}, "
            + "targetURI=http://localhost:8080/, "
            + "connectTimeoutMsec=5000, "
            + "whitelistedStatuses=[LOST, FAILED]"
            + "}");
    // Verifying all attributes were set correctly.
    assertEquals(webhookInfoWithWhitelist.getHeaders(), HEADERS);
    assertEquals(webhookInfoWithWhitelist.getTargetURI(), URI.create(STATIC_URL));
    assertEquals(webhookInfoWithWhitelist.getConnectonTimeoutMsec(), TIMEOUT);
    List<ScheduleStatus> statuses = webhookInfoWithWhitelist.getWhitelistedStatuses().get();
    assertEquals(statuses.size(), 2);
    assertEquals(statuses.get(0), ScheduleStatus.LOST);
    assertEquals(statuses.get(1), ScheduleStatus.FAILED);
  }

  @Test
  public void testWebhookInfoWithWildcardWhitelist() throws Exception {
    WebhookInfo webhookInfoWithWildcardWhitelist = WEBHOOK_INFO_WITH_WILDCARD_WHITELIST_BUILDER
        .setTargetURL(STATIC_URL)
        .build();

    assertEquals(webhookInfoWithWildcardWhitelist.toString(),
        "WebhookInfo{headers={"
            + "Content-Type=application/vnd.kafka.json.v1+json, "
            + "Producer-Type=reliable"
            + "}, "
            + "targetURI=http://localhost:8080/, "
            + "connectTimeoutMsec=5000, "
            + "whitelistedStatuses=null"
            + "}");
    // Verifying all attributes were set correctly.
    assertEquals(webhookInfoWithWildcardWhitelist.getHeaders(), HEADERS);
    assertEquals(webhookInfoWithWildcardWhitelist.getTargetURI(), URI.create(STATIC_URL));
    assertEquals(webhookInfoWithWildcardWhitelist.getConnectonTimeoutMsec(), TIMEOUT);
    assertFalse(webhookInfoWithWildcardWhitelist.getWhitelistedStatuses().isPresent());
  }

  /** Create a Jetty handler that expects a request with a given content body. */
  private AbstractHandler createHandlerThatExpectsContent(String expected) {
    return new AbstractHandler() {
      @Override
      public void handle(String target, Request baseRequest, HttpServletRequest request,
                         HttpServletResponse response) throws IOException, ServletException {
        String body = request.getReader().lines().collect(Collectors.joining());
        if (validateRequest(request) && body.equals(expected)) {
          response.setStatus(HttpServletResponse.SC_OK);
        } else {
          response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
        baseRequest.setHandled(true);
      }
    };
  }

  /** Validate that the request is what we are expecting to send out (ex. POST, headers). */
  private boolean validateRequest(HttpServletRequest request) {
    // Validate general fields are what we expect (POST, headers).
    if (!request.getMethod().equals("POST")) {
      return false;
    }

    for (Map.Entry<String, String> header : HEADERS.entrySet()) {
      String expectedKey = header.getKey();
      String expectedValue = header.getValue();

      if (!expectedValue.equals(request.getHeader(expectedKey))) {
        return false;
      }
    }

    return true;
  }

  /**
   * Need this method to build `WebhookInfo` for testing with the running Jetty port. `jettyServer`
   * should have been started before this method is called.
   */
  private WebhookInfo buildWebhookInfoWithJettyPort(WebhookInfoBuilder builder) {
    return buildWebhookInfoWithJettyPort(builder, jettyServer.getURI().getPort());
  }

  private WebhookInfo buildWebhookInfoWithJettyPort(WebhookInfoBuilder builder, int port) {
    String fullUrl = String.format("http://localhost:%d", port);
    return builder
        .setTargetURL(fullUrl)
        .build();
  }
}
