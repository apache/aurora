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
package org.apache.aurora.common.args.parsers;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;

import org.apache.aurora.common.args.ArgParser;

/**
 * A parser that handles closed ranges. For the input "4-6", it will capture [4, 5, 6].
 */
@ArgParser
public class RangeParser extends NonParameterizedTypeParser<Range<Integer>> {
  @Override
  public Range<Integer> doParse(String raw) throws IllegalArgumentException {
    ImmutableList<String> numbers =
        ImmutableList.copyOf(Splitter.on('-').omitEmptyStrings().split(raw));
    try {
      int from = Integer.parseInt(numbers.get(0));
      int to = Integer.parseInt(numbers.get(1));
      if (numbers.size() != 2) {
        throw new IllegalArgumentException("Failed to parse the range:" + raw);
      }
      if (to < from) {
        return Range.closed(to, from);
      } else {
        return Range.closed(from, to);
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Failed to parse the range:" + raw, e);
    }
  }
}
