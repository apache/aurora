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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.inject.AbstractModule;

import com.google.inject.Singleton;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.args.constraints.CanRead;
import org.apache.aurora.common.args.constraints.Exists;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Binding module for webhook management.
 */
public class WebhookModule extends AbstractModule {

  private static final Logger LOG = LoggerFactory.getLogger(WebhookModule.class);

  @VisibleForTesting
  static final String WEBHOOK_CONFIG_PATH = "org/apache/aurora/scheduler/webhook.json";

  @CmdLine(name = "webhook_config", help = "Path to webhook configuration file.")
  @Exists
  @CanRead
  private static final Arg<File> WEBHOOK_CONFIG_FILE = Arg.create();

  private final boolean enableWebhook;

  public WebhookModule() {
    this(WEBHOOK_CONFIG_FILE.hasAppliedValue());
  }

  @VisibleForTesting
  private WebhookModule(boolean enableWebhook) {
    this.enableWebhook = enableWebhook;
  }

  @Override
  protected void configure() {
    if (enableWebhook) {
      WebhookInfo webhookInfo = parseWebhookConfig(readWebhookFile());
      int timeout = webhookInfo.getConnectonTimeoutMsec();
      RequestConfig config = RequestConfig.custom()
          .setConnectTimeout(timeout) // establish connection with server eg time to TCP handshake.
          .setConnectionRequestTimeout(timeout)  // get a connection from internal pool.
          .setSocketTimeout(timeout) // wait for data after connection was established.
          .build();
      ConnectionKeepAliveStrategy connectionStrategy = new DefaultConnectionKeepAliveStrategy();
      HttpClient client =
          HttpClientBuilder.create()
              .setDefaultRequestConfig(config)
              // being explicit about using default Keep-Alive strategy.
              .setKeepAliveStrategy(connectionStrategy)
              .build();

      bind(WebhookInfo.class).toInstance(webhookInfo);
      bind(HttpClient.class).toInstance(client);
      PubsubEventModule.bindSubscriber(binder(), Webhook.class);
      bind(Webhook.class).in(Singleton.class);
    }
  }

  @VisibleForTesting
  static String readWebhookFile() {
    try {
      return WEBHOOK_CONFIG_FILE.hasAppliedValue()
          ? Files.toString(WEBHOOK_CONFIG_FILE.get(), StandardCharsets.UTF_8)
          : Resources.toString(
              Webhook.class.getClassLoader().getResource(WEBHOOK_CONFIG_PATH),
              StandardCharsets.UTF_8);
    } catch (IOException e) {
      LOG.error("Error loading webhook configuration file.");
      throw Throwables.propagate(e);
    }
  }

  @VisibleForTesting
  static WebhookInfo parseWebhookConfig(String config) {
    checkArgument(!Strings.isNullOrEmpty(config), "Webhook configuration cannot be empty");
    try {
      return new ObjectMapper().readValue(config, WebhookInfo.class);
    } catch (IOException e) {
      LOG.error("Error parsing Webhook configuration file.");
      throw Throwables.propagate(e);
    }
  }
}
