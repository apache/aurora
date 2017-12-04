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
package org.apache.aurora.scheduler.http.api.security;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

public class AuthorizeHeaderTokenTest {
  private static final String ALADDIN_OPEN_SESAME = "QWxhZGRpbjpvcGVuIHNlc2FtZQ==";
  private static final byte[] ALADDIN_OPEN_SESAME_DECODED =
      "Aladdin:open sesame".getBytes(StandardCharsets.US_ASCII);

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidScheme() {
    new AuthorizeHeaderToken("Basic " + ALADDIN_OPEN_SESAME);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTooManyParts() {
    new AuthorizeHeaderToken("Negotiate " + ALADDIN_OPEN_SESAME + " more stuff");
  }

  @Test
  public void testValidScheme() {
    AuthorizeHeaderToken token = new AuthorizeHeaderToken("Negotiate " + ALADDIN_OPEN_SESAME);
    assertArrayEquals(ALADDIN_OPEN_SESAME_DECODED, token.getAuthorizeHeaderValue());
    assertNull(token.getPrincipal());
  }
}
