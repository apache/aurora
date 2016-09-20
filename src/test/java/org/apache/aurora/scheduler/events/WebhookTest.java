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
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.aurora.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.http.Header;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.EntityUtils;
import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class WebhookTest extends EasyMockTest {

  private static final IScheduledTask TASK = TaskTestUtil.makeTask("id", TaskTestUtil.JOB);
  private final TaskStateChange change = TaskStateChange.initialized(TASK);
  private final TaskStateChange changeWithOldState = TaskStateChange
      .transition(TASK, ScheduleStatus.FAILED);
  private final String changeJson = changeWithOldState.toJson();

  private HttpClient httpClient;
  private Webhook webhook;

  @Before
  public void setUp() {
    WebhookInfo webhookInfo = WebhookModule.parseWebhookConfig(WebhookModule.readWebhookFile());
    httpClient = createMock(HttpClient.class);
    webhook = new Webhook(httpClient, webhookInfo);
  }

  @Test
  public void testTaskChangedStateNoOldState() throws Exception {
    // Should be a noop as oldState is MIA so this test would have throw an exception.
    // If it does not, then we are good.
    control.replay();
    webhook.taskChangedState(change);
  }

  @Test
  public void testTaskChangedWithOldState() throws Exception {
    Capture<HttpPost> httpPostCapture = createCapture();
    expect(httpClient.execute(capture(httpPostCapture))).andReturn(null);

    control.replay();

    webhook.taskChangedState(changeWithOldState);

    assertTrue(httpPostCapture.hasCaptured());
    assertEquals(httpPostCapture.getValue().getURI(), new URI("http://localhost:5000/"));
    assertEquals(EntityUtils.toString(httpPostCapture.getValue().getEntity()), changeJson);
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
  public void testCatchHttpClientException() throws Exception {
    // IOException should be silenced.
    Capture<HttpPost> httpPostCapture = createCapture();
    expect(httpClient.execute(capture(httpPostCapture)))
        .andThrow(new IOException());
    control.replay();

    webhook.taskChangedState(changeWithOldState);
  }

  @Test
  public void testWebhookInfo() throws Exception {
    WebhookInfo parsedWebhookInfo = WebhookModule.parseWebhookConfig(
        WebhookModule.readWebhookFile());
    assertEquals(parsedWebhookInfo.toString(),
        "WebhookInfo{headers={"
            + "Content-Type=application/vnd.kafka.json.v1+json, "
            + "Producer-Type=reliable"
            + "}, "
            + "targetURI=http://localhost:5000/, "
            + "connectTimeoutMsec=50"
            + "}");
    // Verifying all attributes were parsed correctly.
    Map<String, String> headers = ImmutableMap.of(
        "Content-Type", "application/vnd.kafka.json.v1+json",
        "Producer-Type", "reliable");
    assertEquals(parsedWebhookInfo.getHeaders(), headers);
    URI targetURI = new URI("http://localhost:5000/");
    assertEquals(parsedWebhookInfo.getTargetURI(), targetURI);
    Integer timeoutMsec = 50;
    assertEquals(parsedWebhookInfo.getConnectonTimeoutMsec(), timeoutMsec);

    control.replay();
  }
}
