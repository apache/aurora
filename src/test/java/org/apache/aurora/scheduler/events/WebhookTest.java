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

import com.google.common.collect.ImmutableMap;

import org.apache.aurora.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class WebhookTest extends EasyMockTest {

  private static final IScheduledTask TASK = TaskTestUtil.makeTask("id", TaskTestUtil.JOB);
  private static final TaskStateChange CHANGE = TaskStateChange.initialized(TASK);
  private static final TaskStateChange CHANGE_WITH_OLD_STATE = TaskStateChange
      .transition(TASK, ScheduleStatus.FAILED);
  private static final String CHANGE_JSON = CHANGE_WITH_OLD_STATE.toJson();
  // Test fixture for WebhookInfo without a whitelist, thus all task statuses are implicitly
  // whitelisted.
  private static final WebhookInfo WEBHOOK_INFO = WebhookInfo.newBuilder()
      .setHeader("Content-Type", "application/vnd.kafka.json.v1+json")
      .setHeader("Producer-Type", "reliable")
      .setTargetURL("http://localhost:5000/")
      .setTimeout(50)
      .build();
  // Test fixture for WebhookInfo in which only "LOST" and "FAILED" task statuses are explicitly
  // whitelisted.
  private static final WebhookInfo WEBHOOK_INFO_WITH_WHITELIST = WebhookInfo.newBuilder()
      .setHeader("Content-Type", "application/vnd.kafka.json.v1+json")
      .setHeader("Producer-Type", "reliable")
      .setTargetURL("http://localhost:5000/")
      .setTimeout(50)
      .addWhitelistedStatus("LOST")
      .addWhitelistedStatus("FAILED")
      .build();
  // Test fixture for WebhookInfo in which all task statuses are whitelisted by wildcard character.
  private static final WebhookInfo WEBHOOK_INFO_WITH_WILDCARD_WHITELIST = WebhookInfo.newBuilder()
      .setHeader("Content-Type", "application/vnd.kafka.json.v1+json")
      .setHeader("Producer-Type", "reliable")
      .setTargetURL("http://localhost:5000/")
      .setTimeout(50)
      .addWhitelistedStatus("*")
      .build();

  private CloseableHttpClient httpClient;
  private Webhook webhook;

  @Before
  public void setUp() {
    httpClient = createMock(CloseableHttpClient.class);
    webhook = new Webhook(httpClient, WEBHOOK_INFO);
  }

  @Test
  public void testTaskChangedStateNoOldState() throws Exception {
    // Should be a noop as oldState is MIA so this test would have throw an exception.
    // If it does not, then we are good.
    control.replay();

    webhook.taskChangedState(CHANGE);
  }

  @Test
  public void testTaskChangedWithOldState() throws Exception {
    CloseableHttpResponse httpResponse = createMock(CloseableHttpResponse.class);
    HttpEntity entity = createMock(HttpEntity.class);

    Capture<HttpPost> httpPostCapture = createCapture();
    expect(entity.isStreaming()).andReturn(false);
    expect(httpResponse.getEntity()).andReturn(entity);
    httpResponse.close();
    expectLastCall().once();
    expect(httpClient.execute(capture(httpPostCapture))).andReturn(httpResponse);

    control.replay();

    webhook.taskChangedState(CHANGE_WITH_OLD_STATE);

    assertTrue(httpPostCapture.hasCaptured());
    assertEquals(httpPostCapture.getValue().getURI(), new URI("http://localhost:5000/"));
    assertEquals(EntityUtils.toString(httpPostCapture.getValue().getEntity()), CHANGE_JSON);
    Header[] producerTypeHeader = httpPostCapture.getValue().getHeaders("Producer-Type");
    assertEquals(producerTypeHeader.length, 1);
    assertEquals(producerTypeHeader[0].getName(), "Producer-Type");
    assertEquals(producerTypeHeader[0].getValue(), "reliable");
    Header[] contentTypeHeader = httpPostCapture.getValue().getHeaders("Content-Type");
    assertEquals(contentTypeHeader.length, 1);
    assertEquals(contentTypeHeader[0].getName(), "Content-Type");
    assertEquals(contentTypeHeader[0].getValue(), "application/vnd.kafka.json.v1+json");
    assertNotNull(httpPostCapture.getValue().getHeaders("Timestamp"));
  }

  @Test
  public void testTaskChangeInWhiteList() throws Exception {
    CloseableHttpResponse httpResponse = createMock(CloseableHttpResponse.class);
    HttpEntity entity = createMock(HttpEntity.class);

    Capture<HttpPost> httpPostCapture = createCapture();
    expect(entity.isStreaming()).andReturn(false);
    expect(httpResponse.getEntity()).andReturn(entity);
    httpResponse.close();
    expectLastCall().once();
    expect(httpClient.execute(capture(httpPostCapture))).andReturn(httpResponse);

    control.replay();

    // Verifying TaskStateChange in the whitelist is sent to the configured endpoint.
    Webhook webhookWithWhitelist = new Webhook(httpClient, WEBHOOK_INFO_WITH_WHITELIST);
    TaskStateChange taskStateChangeInWhitelist = TaskStateChange
        .transition(TaskTestUtil.addStateTransition(TASK, ScheduleStatus.LOST, 1L),
            ScheduleStatus.RUNNING);
    webhookWithWhitelist.taskChangedState(taskStateChangeInWhitelist);

    assertTrue(httpPostCapture.hasCaptured());
    assertEquals(httpPostCapture.getValue().getURI(), new URI("http://localhost:5000/"));
    assertEquals(EntityUtils.toString(httpPostCapture.getValue().getEntity()),
        taskStateChangeInWhitelist.toJson());
    Header[] producerTypeHeader = httpPostCapture.getValue().getHeaders("Producer-Type");
    assertEquals(producerTypeHeader.length, 1);
    assertEquals(producerTypeHeader[0].getName(), "Producer-Type");
    assertEquals(producerTypeHeader[0].getValue(), "reliable");
    Header[] contentTypeHeader = httpPostCapture.getValue().getHeaders("Content-Type");
    assertEquals(contentTypeHeader.length, 1);
    assertEquals(contentTypeHeader[0].getName(), "Content-Type");
    assertEquals(contentTypeHeader[0].getValue(), "application/vnd.kafka.json.v1+json");
    assertNotNull(httpPostCapture.getValue().getHeaders("Timestamp"));
  }

  @Test
  public void testTaskChangeNotInWhiteList() throws Exception {
    control.replay();

    // Verifying TaskStateChange not in the whitelist is not sent to the configured endpoint.
    Webhook webhookWithWhitelist = new Webhook(httpClient, WEBHOOK_INFO_WITH_WHITELIST);
    webhookWithWhitelist.taskChangedState(CHANGE_WITH_OLD_STATE);
  }

  @Test
  public void testCatchHttpClientException() throws Exception {
    // IOException should be silenced.
    Capture<HttpPost> httpPostCapture = createCapture();
    expect(httpClient.execute(capture(httpPostCapture)))
        .andThrow(new IOException());
    control.replay();

    webhook.taskChangedState(CHANGE_WITH_OLD_STATE);
  }

  @Test
  public void testParsingWebhookInfo() throws Exception {
    WebhookInfo parsedWebhookInfo = WebhookModule.parseWebhookConfig(
        WebhookModule.readWebhookFile());
    // Verifying the WebhookInfo parsed from webhook.json file is identical to the WebhookInfo
    // built from WebhookInfoBuilder.
    assertEquals(parsedWebhookInfo.toString(), WEBHOOK_INFO.toString());
    // Verifying all attributes were parsed correctly.
    assertEquals(parsedWebhookInfo.getHeaders(), WEBHOOK_INFO.getHeaders());
    assertEquals(parsedWebhookInfo.getTargetURI(), WEBHOOK_INFO.getTargetURI());
    assertEquals(parsedWebhookInfo.getConnectonTimeoutMsec(),
        WEBHOOK_INFO.getConnectonTimeoutMsec());
    assertEquals(parsedWebhookInfo.getWhitelistedStatuses(), WEBHOOK_INFO.getWhitelistedStatuses());
    control.replay();
  }

  @Test
  public void testWebhookInfo() throws Exception {
    assertEquals(WEBHOOK_INFO.toString(),
        "WebhookInfo{headers={"
            + "Content-Type=application/vnd.kafka.json.v1+json, "
            + "Producer-Type=reliable"
            + "}, "
            + "targetURI=http://localhost:5000/, "
            + "connectTimeoutMsec=50, "
            + "whitelistedStatuses=null"
            + "}");
    // Verifying all attributes were set correctly.
    Map<String, String> headers = ImmutableMap.of(
        "Content-Type", "application/vnd.kafka.json.v1+json",
        "Producer-Type", "reliable");
    assertEquals(WEBHOOK_INFO.getHeaders(), headers);
    URI targetURI = new URI("http://localhost:5000/");
    assertEquals(WEBHOOK_INFO.getTargetURI(), targetURI);
    Integer timeoutMsec = 50;
    assertEquals(WEBHOOK_INFO.getConnectonTimeoutMsec(), timeoutMsec);
    assertFalse(WEBHOOK_INFO.getWhitelistedStatuses().isPresent());
    control.replay();
  }

  @Test
  public void testWebhookInfoWithWhiteList() throws Exception {
    assertEquals(WEBHOOK_INFO_WITH_WHITELIST.toString(),
        "WebhookInfo{headers={"
            + "Content-Type=application/vnd.kafka.json.v1+json, "
            + "Producer-Type=reliable"
            + "}, "
            + "targetURI=http://localhost:5000/, "
            + "connectTimeoutMsec=50, "
            + "whitelistedStatuses=[LOST, FAILED]"
            + "}");
    // Verifying all attributes were set correctly.
    Map<String, String> headers = ImmutableMap.of(
        "Content-Type", "application/vnd.kafka.json.v1+json",
        "Producer-Type", "reliable");
    assertEquals(WEBHOOK_INFO_WITH_WHITELIST.getHeaders(), headers);
    URI targetURI = new URI("http://localhost:5000/");
    assertEquals(WEBHOOK_INFO_WITH_WHITELIST.getTargetURI(), targetURI);
    Integer timeoutMsec = 50;
    assertEquals(WEBHOOK_INFO_WITH_WHITELIST.getConnectonTimeoutMsec(), timeoutMsec);
    List<ScheduleStatus> statuses = WEBHOOK_INFO_WITH_WHITELIST.getWhitelistedStatuses().get();
    assertEquals(statuses.size(), 2);
    assertEquals(statuses.get(0), ScheduleStatus.LOST);
    assertEquals(statuses.get(1), ScheduleStatus.FAILED);
    control.replay();
  }

  @Test
  public void testWebhookInfoWithWildcardWhitelist() throws Exception {
    assertEquals(WEBHOOK_INFO_WITH_WILDCARD_WHITELIST.toString(),
        "WebhookInfo{headers={"
            + "Content-Type=application/vnd.kafka.json.v1+json, "
            + "Producer-Type=reliable"
            + "}, "
            + "targetURI=http://localhost:5000/, "
            + "connectTimeoutMsec=50, "
            + "whitelistedStatuses=null"
            + "}");
    // Verifying all attributes were set correctly.
    Map<String, String> headers = ImmutableMap.of(
        "Content-Type", "application/vnd.kafka.json.v1+json",
        "Producer-Type", "reliable");
    assertEquals(WEBHOOK_INFO_WITH_WILDCARD_WHITELIST.getHeaders(), headers);
    URI targetURI = new URI("http://localhost:5000/");
    assertEquals(WEBHOOK_INFO_WITH_WILDCARD_WHITELIST.getTargetURI(), targetURI);
    Integer timeoutMsec = 50;
    assertEquals(WEBHOOK_INFO_WITH_WILDCARD_WHITELIST.getConnectonTimeoutMsec(), timeoutMsec);
    assertFalse(WEBHOOK_INFO_WITH_WILDCARD_WHITELIST.getWhitelistedStatuses().isPresent());
    control.replay();
  }
}
