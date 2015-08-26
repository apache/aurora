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
package org.apache.aurora.common.application.http;

import javax.servlet.Filter;

import org.apache.aurora.common.base.MorePreconditions;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Configuration tuple for an HTTP filter.
 */
public class HttpFilterConfig {
  public final Class<? extends Filter> filterClass;
  public final String pathSpec;

  /**
   * Creates a new filter configuration.
   *
   * @param filterClass Filter class.
   * @param pathSpec Path spec that the filter should match.
   */
  public HttpFilterConfig(Class<? extends Filter> filterClass, String pathSpec) {
    this.pathSpec = MorePreconditions.checkNotBlank(pathSpec);
    this.filterClass = checkNotNull(filterClass);
  }
}
