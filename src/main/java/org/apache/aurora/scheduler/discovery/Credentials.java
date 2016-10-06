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
package org.apache.aurora.scheduler.discovery;

import java.util.Arrays;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import org.apache.aurora.common.base.MorePreconditions;
import org.apache.commons.lang.builder.EqualsBuilder;

import static java.util.Objects.requireNonNull;

/**
 * Encapsulates a user's ZooKeeper credentials.
 */
public final class Credentials {

  /**
   * Creates a set of credentials for the ZooKeeper digest authentication mechanism.
   *
   * @param username the username to authenticate with
   * @param password the password to authenticate with
   * @return a set of credentials that can be used to authenticate the zoo keeper client
   */
  public static Credentials digestCredentials(String username, String password) {
    MorePreconditions.checkNotBlank(username);
    Preconditions.checkNotNull(password);

    // TODO(John Sirois): DigestAuthenticationProvider is broken - uses platform default charset
    // (on server) and so we just have to hope here that clients are deployed in compatible jvms.
    // Consider writing and installing a version of DigestAuthenticationProvider that controls its
    // Charset explicitly.
    return new Credentials("digest", (username + ":" + password).getBytes());
  }

  private final String authScheme;
  private final byte[] authToken;

  /**
   * Creates a new set of credentials for the given ZooKeeper authentication scheme.
   *
   * @param scheme The name of the authentication scheme the {@code token} is valid in.
   * @param token The authentication token for the given {@code scheme}.
   */
  public Credentials(String scheme, byte[] token) {
    authScheme = MorePreconditions.checkNotBlank(scheme);
    authToken = requireNonNull(token);
  }

  /**
   * Returns the authentication scheme these credentials are for.
   *
   * @return the scheme these credentials are for.
   */
  public String scheme() {
    return authScheme;
  }

  /**
   * Returns the authentication token.
   *
   * @return the authentication token.
   */
  public byte[] token() {
    return Arrays.copyOf(authToken, authToken.length);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Credentials)) {
      return false;
    }

    Credentials other = (Credentials) o;
    return new EqualsBuilder()
        .append(authScheme, other.scheme())
        .append(authToken, other.token())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(authScheme, authToken);
  }
}
