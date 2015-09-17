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
package org.apache.aurora.common.util;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.aurora.common.base.MorePreconditions;

import static java.util.Objects.requireNonNull;

/**
 * Handles loading of a build properties file, and provides keys to look up known values in the
 * properties.
 */
public class BuildInfo {

  private static final Logger LOG = Logger.getLogger(BuildInfo.class.getName());

  private static final String DEFAULT_BUILD_PROPERTIES_PATH = "build.properties";

  private final String resourcePath;

  private Map<String, String> properties = ImmutableMap.of();

  /**
   * Creates a build info container that will use the default properties file path.
   */
  public BuildInfo() {
    this(DEFAULT_BUILD_PROPERTIES_PATH);
  }

  /**
   * Creates a build info container, reading from the given path.
   *
   * @param resourcePath The resource path to read build properties from.
   */
  public BuildInfo(String resourcePath) {
    this.resourcePath = MorePreconditions.checkNotBlank(resourcePath);
    fetchProperties();
  }

  @VisibleForTesting
  public BuildInfo(Map<String, String> properties) {
    this.resourcePath = null;
    this.properties = requireNonNull(properties);
  }

  private void fetchProperties() {
    LOG.info("Fetching build properties from " + resourcePath);
    InputStream in = ClassLoader.getSystemResourceAsStream(resourcePath);
    if (in == null) {
      LOG.warning("Failed to fetch build properties from " + resourcePath);
      return;
    }

    try {
      Properties buildProperties = new Properties();
      buildProperties.load(in);
      properties = Maps.fromProperties(buildProperties);
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Failed to load properties file " + resourcePath, e);
    }
  }

  /**
   * Fetches the properties stored in the resource location.
   *
   * @return A map of the loaded properties, or an empty Map if there was a problem loading
   *    the specified properties resource.
   */
  public Map<String, String> getProperties() {
    return properties;
  }
}
