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
import java.io.UnsupportedEncodingException;
import java.time.Instant;

import com.google.common.eventbus.Subscribe;

import com.google.inject.Inject;

import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watches TaskStateChanges and send events to configured endpoint.
 */
public class Webhook implements EventSubscriber {

  private static final Logger LOG = LoggerFactory.getLogger(Webhook.class);

  private final WebhookInfo webhookInfo;
  private final HttpClient httpClient;

  @Inject
  Webhook(HttpClient httpClient, WebhookInfo webhookInfo) {
    this.webhookInfo = webhookInfo;
    this.httpClient = httpClient;
    LOG.info("Webhook enabled with info" + this.webhookInfo);
  }

  private HttpPost createPostRequest(TaskStateChange stateChange)
      throws UnsupportedEncodingException {
    String eventJson = stateChange.toJson();
    HttpPost post = new HttpPost();
    post.setURI(webhookInfo.getTargetURI());
    post.setHeader("Timestamp", Long.toString(Instant.now().toEpochMilli()));
    post.setEntity(new StringEntity(eventJson));
    webhookInfo.getHeaders().entrySet().forEach(
        e -> post.setHeader(e.getKey(), e.getValue()));
    return post;
  }

  /**
   * Watches all TaskStateChanges and send them best effort to a configured endpoint.
   * <p>
   * This is used to expose an external event bus.
   *
   * @param stateChange State change notification.
   */
  @Subscribe
  public void taskChangedState(TaskStateChange stateChange) {
    LOG.debug("Got an event: " + stateChange.toString());
    // Old state is not present because a scheduler just failed over. In that case we do not want to
    // resend the entire state.
    if (stateChange.getOldState().isPresent()) {
      try {
        HttpPost post = createPostRequest(stateChange);
        try {
          httpClient.execute(post);
        }  catch (IOException exp) {
          LOG.error("Error sending a Webhook event", exp);
        }
      } catch (UnsupportedEncodingException exp) {
        LOG.error("HttpPost exception when creating an HTTP Post request", exp);
      }
    }
  }
}
