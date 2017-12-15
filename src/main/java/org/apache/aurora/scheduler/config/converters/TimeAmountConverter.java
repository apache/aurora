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

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.beust.jcommander.ParameterException;
import com.beust.jcommander.converters.BaseConverter;
import com.google.common.collect.ImmutableList;

import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.scheduler.config.types.TimeAmount;

public class TimeAmountConverter extends BaseConverter<TimeAmount> {
  private static final Pattern AMOUNT_PATTERN = Pattern.compile("(\\d+)([A-Za-z]+)");

  public TimeAmountConverter(String optionName) {
    super(optionName);
  }

  @Override
  public TimeAmount convert(String raw) {
    Matcher matcher = AMOUNT_PATTERN.matcher(raw);

    if (!matcher.matches()) {
      throw new ParameterException(getErrorString(raw, "must be of the format 1ns, 2secs, etc."));
    }

    Optional<Time> unit = Stream.of(Time.values())
        .filter(value -> value.toString().equals(matcher.group(2)))
        .findFirst();
    if (unit.isPresent()) {
      return new TimeAmount(Long.parseLong(matcher.group(1)), unit.get());
    } else {
      throw new ParameterException(
          getErrorString(raw, "one of " + ImmutableList.copyOf(Time.values())));
    }
  }
}
