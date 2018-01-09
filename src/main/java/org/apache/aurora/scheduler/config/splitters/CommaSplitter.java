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

package org.apache.aurora.scheduler.config.splitters;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.beust.jcommander.converters.IParameterSplitter;

// Workaround for default comma splitter returning [""] when an empty
// string is passed as an argument. This custom splitter may be removed
// after https://github.com/cbeust/jcommander/pull/422 has landed.
public class CommaSplitter implements IParameterSplitter {
  @Override
  public List<String> split(String value) {
    if ("".equals(value) || "''".equals(value)) {
      return Collections.emptyList();
    }
    return Arrays.asList(value.split(","));
  }
}
