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
package org.apache.aurora.scheduler;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.inject.AbstractModule;

import org.apache.aurora.scheduler.TierManager.TierManagerImpl;
import org.apache.aurora.scheduler.TierManager.TierManagerImpl.TierConfig;
import org.apache.aurora.scheduler.config.validators.ReadableFile;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Binding module for tier management.
 */
public class TierModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(TierModule.class);

  @VisibleForTesting
  static final String TIER_CONFIG_PATH = "org/apache/aurora/scheduler/tiers.json";

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-tier_config",
        validateValueWith = ReadableFile.class,
        description =
            "Configuration file defining supported task tiers, task traits and behaviors.")
    public File tierConfigFile;
  }

  private final TierConfig tierConfig;

  public TierModule(Options options) {
    this(parseTierConfig(readTierFile(options)));
  }

  @VisibleForTesting
  public TierModule(TierConfig tierConfig) {
    this.tierConfig = requireNonNull(tierConfig);
  }

  @Override
  protected void configure() {
    bind(TierManager.class).toInstance(new TierManagerImpl(tierConfig));
  }

  static String readTierFile(Options options) {
    try {
      File tierConfig = options.tierConfigFile;
      return tierConfig == null
          ? Resources.toString(
              TierModule.class.getClassLoader().getResource(TIER_CONFIG_PATH),
              StandardCharsets.UTF_8)
          : Files.toString(tierConfig, StandardCharsets.UTF_8);
    } catch (IOException e) {
      LOG.error("Error loading tier configuration file.");
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  static TierConfig parseTierConfig(String config) {
    checkArgument(!Strings.isNullOrEmpty(config), "configuration cannot be empty");
    try {
      return new ObjectMapper().readValue(config, TierConfig.class);
    } catch (IOException e) {
      LOG.error("Error parsing tier configuration file.");
      throw new RuntimeException(e);
    }
  }
}
