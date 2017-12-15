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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.aurora.gen.ScheduleStatus;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import static java.util.Objects.requireNonNull;

/**
 * Defines configuration for Webhook.
 */
public class WebhookInfo {
  private final Integer connectTimeoutMsec;
  private final Map<String, String> headers;
  private final URI targetURI;
  private final Optional<List<ScheduleStatus>> whitelistedStatuses;

  /**
   * Return key:value pairs of headers to set for every connection.
   *
   * @return Map
   */
  public Map<String, String> getHeaders() {
    return this.headers;
  }

  /**
   * Returns URI where to post events.
   *
   * @return URI
   */
  URI getTargetURI() {
    return targetURI;
  }

  /**
   * Returns connection timeout to set when POSTing an event.
   *
   * @return Integer value.
   */
  Integer getConnectonTimeoutMsec() {
    return connectTimeoutMsec;
  }

  /**
   * Returns an optional list of task statuses to be subscribed by the webhook.
   *
   * @return an optional list of ScheduleStatus.
   */
  Optional<List<ScheduleStatus>> getWhitelistedStatuses() {
    return whitelistedStatuses;
  }

  private static final Predicate<List<String>> IS_ALL_WHITELISTED = statuses ->
      !Optional.ofNullable(statuses).isPresent()
          || Optional.ofNullable(statuses).get().stream().anyMatch(status -> "*".equals(status));

  @JsonCreator
  public WebhookInfo(
       @JsonProperty("headers") Map<String, String> headers,
       @JsonProperty("targetURL") String targetURL,
       @JsonProperty("timeoutMsec") Integer timeout,
       @JsonProperty("statuses") List<String> statuses) throws URISyntaxException {

    this.headers = ImmutableMap.copyOf(headers);
    this.targetURI = new URI(requireNonNull(targetURL));
    this.connectTimeoutMsec = requireNonNull(timeout);
    this.whitelistedStatuses = IS_ALL_WHITELISTED.apply(statuses) ? Optional.empty()
        : Optional.ofNullable(statuses).map(
            s -> ImmutableList.copyOf(s.stream()
                .map(ScheduleStatus::valueOf)
                .collect(Collectors.toList())));
  }

  WebhookInfo(WebhookInfoBuilder builder) throws URISyntaxException {
    this(builder.headers, builder.targetURL, builder.timeout, builder.statuses);
  }

  @VisibleForTesting
  static WebhookInfoBuilder newBuilder() {
    return new WebhookInfoBuilder();
  }

  static class WebhookInfoBuilder {
    private Integer timeout;
    private Map<String, String> headers;
    private String targetURL;
    private List<String> statuses;

    public WebhookInfoBuilder setTimeout(Integer timeout) {
      this.timeout = timeout;
      return this;
    }

    public WebhookInfoBuilder setHeader(String key, String value) {
      if (headers == null) {
        headers = new LinkedHashMap<>();
      }
      headers.put(key, value);
      return this;
    }

    /**
     * This method will add the supplied headers to the current headers.
     *
     * @param values The headers to add.
     * @return The modified builder.
     */
    public WebhookInfoBuilder setHeaders(Map<String, String> values) {
      for (Map.Entry<String, String> entry : values.entrySet()) {
        setHeader(entry.getKey(), entry.getValue());
      }

      return this;
    }

    public WebhookInfoBuilder setTargetURL(String targetURL) {
      this.targetURL = targetURL;
      return this;
    }

    public WebhookInfoBuilder addWhitelistedStatus(String status) {
      if (statuses == null) {
        statuses = new ArrayList<>();
      }
      statuses.add(status);
      return this;
    }

    public WebhookInfo build() {
      try {
        return new WebhookInfo(this);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("headers", headers.toString())
        .add("targetURI", targetURI.toString())
        .add("connectTimeoutMsec", connectTimeoutMsec)
        .add("whitelistedStatuses", whitelistedStatuses.orElse(null))
        .toString();
  }
}
