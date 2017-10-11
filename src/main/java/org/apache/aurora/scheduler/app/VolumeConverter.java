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
package org.apache.aurora.scheduler.app;

import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.BaseConverter;
import com.google.common.base.Joiner;

import org.apache.aurora.gen.Mode;
import org.apache.aurora.gen.Volume;

/**
 * Converter to transform a string in host:container:mode form to a VolumeConfig object.
 */
public class VolumeConverter extends BaseConverter<Volume> {

  public VolumeConverter() {
    super("");
  }

  public VolumeConverter(String optionName) {
    super(optionName);
  }

  @Override
  public Volume convert(String raw) {
    String[] split = raw.split(":");
    if (split.length != 3) {
      throw new ParameterException(
          getErrorString(raw, "must be in the format of 'host:container:mode'"));
    }

    Mode mode;
    try {
      mode = Mode.valueOf(split[2].toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new ParameterException(
          getErrorString(raw, "Read/Write spec must be in " + Joiner.on(", ").join(Mode.values())),
          e);
    }
    return new Volume(split[1], split[0], mode);
  }
}
