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
package org.apache.aurora.scheduler.mesos;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Throwables;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;

import org.apache.mesos.Protos;
import org.junit.Test;

import static org.apache.mesos.Protos.FrameworkInfo.Capability.Type.REVOCABLE_RESOURCES;
import static org.junit.Assert.assertEquals;

public class CommandLineDriverSettingsModuleTest {

  @Test(expected = IllegalStateException.class)
  public void testMissingPropertiesParsing() {
    Properties testProperties = new Properties();
    testProperties.put(CommandLineDriverSettingsModule.PRINCIPAL_KEY, "aurora-scheduler");

    ByteArrayOutputStream propertiesStream = new ByteArrayOutputStream();
    try {
      testProperties.store(propertiesStream, "");
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    CommandLineDriverSettingsModule.parseCredentials(
        new ByteArrayInputStream(propertiesStream.toByteArray()));
  }

  @Test
  public void testPropertiesParsing() {
    Properties testProperties = new Properties();
    testProperties.put(CommandLineDriverSettingsModule.PRINCIPAL_KEY, "aurora-scheduler");
    testProperties.put(CommandLineDriverSettingsModule.SECRET_KEY, "secret");

    ByteArrayOutputStream propertiesStream = new ByteArrayOutputStream();
    try {
      testProperties.store(propertiesStream, "");
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    assertEquals(
        testProperties,
        CommandLineDriverSettingsModule.parseCredentials(
            new ByteArrayInputStream(propertiesStream.toByteArray())));
  }

  @Test
  public void testFrameworkInfoNoRevocable() {
    Protos.FrameworkInfo info = CommandLineDriverSettingsModule.buildFrameworkInfo(
        "user",
        Amount.of(1L, Time.MINUTES),
        false);
    assertEquals(0, info.getCapabilitiesCount());
  }

  @Test
  public void testFrameworkInfoRevocable() {
    Protos.FrameworkInfo info = CommandLineDriverSettingsModule.buildFrameworkInfo(
        "user",
        Amount.of(1L, Time.MINUTES),
        true);
    assertEquals(1, info.getCapabilitiesCount());
    assertEquals(REVOCABLE_RESOURCES, info.getCapabilities(0).getType());
  }
}
