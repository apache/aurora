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
package org.apache.aurora.scheduler.config.validators;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.ParameterException;

public class NotNegativeNumber implements IValueValidator<Number> {
  @Override
  public void validate(String name, Number value) throws ParameterException {
    if (value.longValue() < 0) {
      throw new ParameterException(String.format("%s must not be negative", name));
    }
  }
}
