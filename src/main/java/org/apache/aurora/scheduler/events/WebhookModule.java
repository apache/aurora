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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.config.validators.ReadableFile;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.channel.DefaultKeepAliveStrategy;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

import static org.asynchttpclient.Dsl.asyncHttpClient;

/**
 * Binding module for webhook management.
 */
public class WebhookModule extends AbstractModule {

  private static final Logger LOG = LoggerFactory.getLogger(WebhookModule.class);

  @VisibleForTesting
  static final String WEBHOOK_CONFIG_PATH = "org/apache/aurora/scheduler/webhook.json";

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-webhook_config",
        validateValueWith = ReadableFile.class,
        description = "Path to webhook configuration file.")
    public File webhookConfigFile = null;
  }

  private final Optional<String> webhookConfig;

  public WebhookModule(Options options) {
    this.webhookConfig = Optional.fromNullable(options.webhookConfigFile)
        .transform(f -> {
          try {
            return Files.asCharSource(options.webhookConfigFile, StandardCharsets.UTF_8).read();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @VisibleForTesting
  WebhookModule(Optional<String> webhookConfig) {
    this.webhookConfig = webhookConfig;
  }

  @Override
  protected void configure() {
    if (webhookConfig.isPresent()) {
      WebhookInfo webhookInfo = parseWebhookConfig(webhookConfig.get());
      DefaultAsyncHttpClientConfig config = new DefaultAsyncHttpClientConfig.Builder()
          .setConnectTimeout(webhookInfo.getConnectonTimeoutMsec())
          .setHandshakeTimeout(webhookInfo.getConnectonTimeoutMsec())
          .setSslSessionTimeout(webhookInfo.getConnectonTimeoutMsec())
          .setReadTimeout(webhookInfo.getConnectonTimeoutMsec())
          .setRequestTimeout(webhookInfo.getConnectonTimeoutMsec())
          .setKeepAliveStrategy(new DefaultKeepAliveStrategy())
          .build();
      AsyncHttpClient httpClient = asyncHttpClient(config);

      bind(WebhookInfo.class).toInstance(webhookInfo);
      bind(AsyncHttpClient.class).toInstance(httpClient);
      PubsubEventModule.bindSubscriber(binder(), Webhook.class);
      bind(Webhook.class).in(Singleton.class);

      SchedulerServicesModule.addSchedulerActiveServiceBinding(binder())
          .to(Webhook.class);
    }
  }

  @VisibleForTesting
  static WebhookInfo parseWebhookConfig(String config) {
    checkArgument(!Strings.isNullOrEmpty(config), "Webhook configuration cannot be empty");
    try {
      return new ObjectMapper().readValue(config, WebhookInfo.class);
    } catch (IOException e) {
      LOG.error("Error parsing Webhook configuration file.");
      throw new RuntimeException(e);
    }
  }
}
