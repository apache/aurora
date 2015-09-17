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
package org.apache.aurora.common.util.testing;

import java.util.Map;

import com.google.common.collect.Maps;

import org.apache.aurora.common.util.BuildInfo;

/**
 * A fixture to generate fake BuildInfo properties
 */
public class FakeBuildInfo {

  public static final String DATE = "DATE";
  public static final String GIT_REVISION = "GIT_REVISION";
  public static final String GIT_TAG = "GIT_TAG";

  private FakeBuildInfo() {
  }

  public static BuildInfo generateBuildInfo() {
    Map<String, String> buildProperties = Maps.newHashMap();
    buildProperties.put(DATE, DATE);
    buildProperties.put(GIT_REVISION, GIT_REVISION);
    buildProperties.put(GIT_TAG, GIT_TAG);
    return new BuildInfo(buildProperties);
  }

}
