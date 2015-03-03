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

import org.apache.aurora.gen.Mode;
import org.apache.aurora.gen.Volume;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VolumeParserTest {

  private VolumeParser volumeParser;

  @Before
  public void setUp() {
    volumeParser = new VolumeParser();
  }

  @Test
  public void testValidConfig() {
    assertEquals(
        new Volume("/container", "/host", Mode.RO),
        volumeParser.doParse("/host:/container:ro"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidConfig() {
    volumeParser.doParse("/host,nope");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidMode() {
    volumeParser.doParse("/host:/container:RE");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTooManySegments() {
    volumeParser.doParse("/host:/container:RO:bonusdata");
  }
}
