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
package org.apache.aurora.scheduler.base;

import java.util.regex.Pattern;

import javax.annotation.Nullable;

import static org.apache.aurora.gen.apiConstants.GOOD_IDENTIFIER_PATTERN_JVM;

/**
 * Utility class for validation of strings provided by end-users.
 */
public final class UserProvidedStrings {
  private static final Pattern GOOD_IDENTIFIER = Pattern.compile(GOOD_IDENTIFIER_PATTERN_JVM);
  private static final int MAX_IDENTIFIER_LENGTH = 255;

  private UserProvidedStrings() {
    // Utility class.
  }

  /**
   * Verifies that an identifier is an acceptable name component.
   *
   * @param identifier Identifier to check.
   * @return false if the identifier is null or invalid.
   */
  public static boolean isGoodIdentifier(@Nullable String identifier) {
    return identifier != null
        && GOOD_IDENTIFIER.matcher(identifier).matches()
        && identifier.length() <= MAX_IDENTIFIER_LENGTH;
  }
}
