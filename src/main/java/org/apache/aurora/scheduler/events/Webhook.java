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

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.eventbus.Subscribe;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStateChange;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.Response;
import org.asynchttpclient.util.HttpConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Watches TaskStateChanges and send events to configured endpoint.
 */
public class Webhook extends AbstractIdleService implements EventSubscriber {
  @VisibleForTesting
  static final String ATTEMPTS_STAT_NAME = "webhooks_attempts";
  @VisibleForTesting
  static final String SUCCESS_STAT_NAME = "webhooks_success";
  @VisibleForTesting
  static final String ERRORS_STAT_NAME = "webhooks_errors";
  @VisibleForTesting
  static final String USER_ERRORS_STAT_NAME = "webhooks_user_errors";

  private static final Logger LOG = LoggerFactory.getLogger(Webhook.class);

  private final WebhookInfo webhookInfo;
  private final AsyncHttpClient httpClient;
  private final Predicate<ScheduleStatus> isWhitelisted;

  private final AtomicLong attemptsCounter;
  private final AtomicLong successCounter;
  private final AtomicLong errorsCounter;
  private final AtomicLong userErrorsCounter;

  @Inject
  Webhook(AsyncHttpClient httpClient, WebhookInfo webhookInfo, StatsProvider statsProvider) {
    this.webhookInfo = requireNonNull(webhookInfo);
    this.httpClient = requireNonNull(httpClient);
    this.attemptsCounter = statsProvider.makeCounter(ATTEMPTS_STAT_NAME);
    this.successCounter = statsProvider.makeCounter(SUCCESS_STAT_NAME);
    this.errorsCounter = statsProvider.makeCounter(ERRORS_STAT_NAME);
    this.userErrorsCounter = statsProvider.makeCounter(USER_ERRORS_STAT_NAME);
    this.isWhitelisted = status -> !webhookInfo.getWhitelistedStatuses().isPresent()
        || webhookInfo.getWhitelistedStatuses().get().contains(status);
    LOG.info("Webhook enabled with info" + this.webhookInfo);
  }

  private BoundRequestBuilder createRequest(TaskStateChange stateChange) {
    return httpClient.preparePost(webhookInfo.getTargetURI().toString())
        .setBody(stateChange.toJson())
        .setSingleHeaders(webhookInfo.getHeaders())
        .addHeader("Timestamp", Long.toString(Instant.now().toEpochMilli()));
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
    LOG.debug("Got an event: {}", stateChange);
    // Ensure that this state change event is a transition, and not an event from when the scheduler
    // first initializes. In that case we do not want to resend the entire state. This check also
    // ensures that only whitelisted statuses will be sent to the configured endpoint.
    if (stateChange.isTransition() && isWhitelisted.apply(stateChange.getNewState())) {
      attemptsCounter.incrementAndGet();
      try {
        // We don't care about the response body, so only listen for the HTTP status code.
        createRequest(stateChange).execute(new AsyncCompletionHandler<Integer>() {
          @Override
          public void onThrowable(Throwable t) {
            errorsCounter.incrementAndGet();
            LOG.error("Error sending a Webhook event", t);
          }

          @Override
          public State onStatusReceived(HttpResponseStatus status) throws Exception {
            if (status.getStatusCode() == HttpConstants.ResponseStatusCodes.OK_200) {
              successCounter.incrementAndGet();
            } else {
              userErrorsCounter.incrementAndGet();
            }

            // Abort after we get the status because that is all we use for processing.
            return State.ABORT;
          }

          @Override
          public Integer onCompleted(Response response) throws Exception {
            // We do not care about the full response.
            return 0;
          }
        });
      } catch (Exception e) {
        LOG.error("Error making Webhook request", e);
        errorsCounter.incrementAndGet();
      }
    }
  }

  @Override
  protected void startUp() throws Exception {
    // No-op
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down async Webhook client.");
    httpClient.close();
  }
}
