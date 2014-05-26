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
package org.apache.aurora;

import com.google.common.base.Function;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;

/**
 * Utility functions that are useful for working with protocol buffer messages.
 */
public final class Protobufs {

  private Protobufs() {
    // Utility class.
  }

  /**
   * Function to call {@link #toString(Message)} on message objects.
   */
  public static final Function<Message, String> SHORT_TOSTRING = new Function<Message, String>() {
    @Override
    public String apply(Message message) {
      return Protobufs.toString(message);
    }
  };

  /**
   * Alternative to the default protobuf toString implementation, which omits newlines.
   *
   * @param message Message to print.
   * @return String representation of the message.
   */
  public static String toString(Message message) {
    return TextFormat.shortDebugString(message);
  }
}
