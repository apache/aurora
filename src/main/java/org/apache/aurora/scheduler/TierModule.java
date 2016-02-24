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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.google.inject.AbstractModule;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.args.constraints.CanRead;
import org.apache.aurora.scheduler.TierManager.TierManagerImpl;
import org.apache.aurora.scheduler.TierManager.TierManagerImpl.TierConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Binding module for tier management.
 */
public class TierModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(TierModule.class);

  @CanRead
  @CmdLine(name = "tier_config",
      help = "Configuration file defining supported task tiers, task traits and behaviors.")
  private static final Arg<File> TIER_CONFIG_FILE = Arg.create();

  private final TierConfig tierConfig;

  public TierModule() {
    this(parseTierConfig(readTierFile()));
  }

  @VisibleForTesting
  public TierModule(TierConfig tierConfig) {
    this.tierConfig = requireNonNull(tierConfig);
  }

  @Override
  protected void configure() {
    bind(TierManager.class).toInstance(new TierManagerImpl(tierConfig));
  }

  static Optional<String> readTierFile() {
    if (TIER_CONFIG_FILE.hasAppliedValue()) {
      try {
        return Optional.of(Files.toString(TIER_CONFIG_FILE.get(), StandardCharsets.UTF_8));
      } catch (IOException e) {
        LOG.error("Error loading tier configuration file.");
        throw Throwables.propagate(e);
      }
    }

    return Optional.<String>absent();
  }

  @VisibleForTesting
  static TierConfig parseTierConfig(Optional<String> config) {
    Optional<TierConfig> map = config.transform(input -> {
      try {
        return new ObjectMapper().readValue(input, TierConfig.class);
      } catch (IOException e) {
        LOG.error("Error parsing tier configuration file.");
        throw Throwables.propagate(e);
      }
    });
    return map.or(TierConfig.EMPTY);
  }
}
