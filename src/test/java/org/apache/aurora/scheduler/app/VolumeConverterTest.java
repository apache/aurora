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

import org.apache.aurora.gen.Mode;
import org.apache.aurora.gen.Volume;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VolumeConverterTest {

  private VolumeConverter volumeConverter;

  @Before
  public void setUp() {
    volumeConverter = new VolumeConverter();
  }

  @Test
  public void testValidConfig() {
    assertEquals(
        new Volume("/container", "/host", Mode.RO),
        volumeConverter.convert("/host:/container:ro"));
  }

  @Test(expected = ParameterException.class)
  public void testInvalidConfig() {
    volumeConverter.convert("/host,nope");
  }

  @Test(expected = ParameterException.class)
  public void testInvalidMode() {
    volumeConverter.convert("/host:/container:RE");
  }

  @Test(expected = ParameterException.class)
  public void testTooManySegments() {
    volumeConverter.convert("/host:/container:RO:bonusdata");
  }
}
