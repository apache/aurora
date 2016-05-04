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
package org.apache.aurora.scheduler.resources;

import org.junit.Test;

import static org.apache.aurora.scheduler.resources.AuroraResourceConverter.DOUBLE;
import static org.apache.aurora.scheduler.resources.AuroraResourceConverter.LONG;
import static org.apache.aurora.scheduler.resources.AuroraResourceConverter.STRING;
import static org.junit.Assert.assertEquals;

public class AuroraResourceConverterTest {
  @Test
  public void testRoundtrip() {
    assertEquals(234L, LONG.parseFrom(LONG.stringify(234L)).longValue());
    assertEquals(2.34, DOUBLE.parseFrom(DOUBLE.stringify(2.34)).doubleValue(), 0.0);
    assertEquals("http", STRING.parseFrom(STRING.stringify("http")));
  }
}
