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

package org.apache.aurora.scheduler.config.converters;

import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.BaseConverter;

import org.apache.aurora.gen.DockerParameter;

public class DockerParameterConverter extends BaseConverter<DockerParameter> {
  public DockerParameterConverter(String optionName) {
    super(optionName);
  }

  @Override
  public DockerParameter convert(String value) {
    int pos = value.indexOf('=');
    if (pos == -1 || pos == 0 || pos == value.length() - 1) {
      throw new ParameterException(getErrorString(value, "formatted as name=value"));
    }

    return new DockerParameter(value.substring(0, pos), value.substring(pos + 1));
  }
}
