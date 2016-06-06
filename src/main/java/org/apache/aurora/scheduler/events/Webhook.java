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

import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Instant;

import com.google.common.eventbus.Subscribe;

import com.google.inject.Inject;

import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watches TaskStateChanges and send events to configured endpoint.
 */
public class Webhook implements EventSubscriber {

  private static final Logger LOG = LoggerFactory.getLogger(Webhook.class);

  private static final String CALL_METHOD = "POST";
  private final WebhookInfo webhookInfo;

  @Inject
  Webhook(WebhookInfo webhookInfo) {
    this.webhookInfo = webhookInfo;
    LOG.debug("Webhook enabled with info" + this.webhookInfo);
  }

  private HttpURLConnection initializeConnection() {
    try {
      final HttpURLConnection connection = (HttpURLConnection) new URL(
          this.webhookInfo.getTargetURL()).openConnection();
      connection.setRequestMethod(CALL_METHOD);
      connection.setConnectTimeout(this.webhookInfo.getConnectonTimeout());

      webhookInfo.getHeaders().entrySet().forEach(
          e -> connection.setRequestProperty(e.getKey(), e.getValue()));
      connection.setRequestProperty("TimeStamp", Long.toString(Instant.now().toEpochMilli()));
      connection.setDoOutput(true);
      return connection;
    } catch (Exception e) {
      // Do nothing since we are just doing best-effort here.
      LOG.error("Exception trying to initialize a connection:", e);
      return null;
    }
  }

  /**
   * Calls a specified endpoint with the provided string representing an internal event.
   *
   * @param eventJson String represenation of task state change.
   */
  public void callEndpoint(String eventJson) {
    HttpURLConnection connection = this.initializeConnection();
    if (connection == null) {
      LOG.error("Received a null object when trying to initialize an HTTP connection");
    } else {
      try {
        try (DataOutputStream wr = new DataOutputStream(connection.getOutputStream())) {
          wr.writeBytes(eventJson);
          LOG.debug("Sending message " + eventJson
              + " with connection info " + connection.toString()
              + " with WebhookInfo " + this.webhookInfo.toString());
        } catch (Exception e) {
          InputStream errorStream = connection.getErrorStream();
          if (errorStream != null) {
            errorStream.close();
          }
          LOG.error("Exception writing via HTTP connection", e);
        }
        // Don't care about reading input so just performing basic close() operation.
        connection.getInputStream().close();
      } catch (Exception e) {
        LOG.error("Exception when sending a task change event", e);
      }
    }
    LOG.debug("Done with Webhook call");
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
    String eventJson = stateChange.toJson();
    callEndpoint(eventJson);
  }
}
