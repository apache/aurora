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

import com.google.common.eventbus.EventBus;

import org.apache.aurora.common.testing.easymock.EasyMockTest;

import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WebhookTest extends EasyMockTest {
  private static final IScheduledTask TASK = TaskTestUtil.makeTask("id", TaskTestUtil.JOB);
  private Webhook realWebhook;
  private Webhook webhook;
  private EventBus eventBus;

  @Before
  public void setUp() {
    webhook = createMock(Webhook.class);
    eventBus = new EventBus();
    eventBus.register(webhook);
    WebhookInfo webhookInfo = WebhookModule.parseWebhookConfig(
        "{\"headers\": {\"Producer-Type\": \"reliable\","
            + " \"Content-Type\": \"application/vnd.kafka.json.v1+json\"},"
            + " \"timeoutMsec\": 1,"
            + " \"targetURL\": \"http://localhost:5000/\"}"
    );
    realWebhook = new Webhook(webhookInfo);
  }

  private final TaskStateChange change = TaskStateChange.initialized(TASK);
  private final String changeJson = TaskStateChange.initialized(TASK).toJson();

  @Test
  public void testTaskChangedState() {
    webhook.taskChangedState(change);

    control.replay();

    eventBus.post(change);
  }

  @Test
  public void testCallEndpoint() {
    control.replay();

    realWebhook.callEndpoint(changeJson);
  }

  @Test
  public void testWebhookInfo() {
    WebhookInfo webhookInfo = WebhookModule.parseWebhookConfig(WebhookModule.readWebhookFile());
    assertEquals(webhookInfo.toString(),
        "WebhookInfo{headers={"
            + "Content-Type=application/vnd.kafka.json.v1+json, "
            + "Producer-Type=reliable"
            + "}, "
            + "targetURL=http://localhost:5000/, "
            + "connectTimeout=5"
            + "}");
    control.replay();
  }
}
