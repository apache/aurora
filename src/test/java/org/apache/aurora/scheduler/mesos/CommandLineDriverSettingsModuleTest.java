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

import com.google.common.base.Optional;
import com.google.common.base.Throwables;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.mesos.Protos;
import org.junit.Test;

import static org.apache.mesos.Protos.FrameworkInfo.Capability.Type.REVOCABLE_RESOURCES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CommandLineDriverSettingsModuleTest {

  private static final String TEST_ROLE = "test-role";

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
        "aurora",
        "user",
        Optional.absent(),
        Amount.of(1L, Time.MINUTES),
        false,
        Optional.absent());
    assertEquals("", info.getPrincipal());
    assertEquals(0, info.getCapabilitiesCount());
    assertFalse(info.hasRole());
  }

  @Test
  public void testFrameworkInfoRevocable() {
    Protos.FrameworkInfo info = CommandLineDriverSettingsModule.buildFrameworkInfo(
        "aurora",
        "user",
        Optional.absent(),
        Amount.of(1L, Time.MINUTES),
        true,
        Optional.absent());
    assertEquals("", info.getPrincipal());
    assertEquals(1, info.getCapabilitiesCount());
    assertEquals(REVOCABLE_RESOURCES, info.getCapabilities(0).getType());
    assertFalse(info.hasRole());
  }

  @Test
  public void testFrameworkInfoNoRevocableWithAnnouncedPrincipal() {
    Protos.FrameworkInfo info = CommandLineDriverSettingsModule.buildFrameworkInfo(
        "aurora",
        "user",
        Optional.of("auroraprincipal"),
        Amount.of(1L, Time.MINUTES),
        false,
        Optional.absent());
    assertEquals("auroraprincipal", info.getPrincipal());
    assertEquals(0, info.getCapabilitiesCount());
    assertFalse(info.hasRole());
  }

  @Test
  public void testFrameworkInfoRevocableWithAnnouncedPrincipalAndRole() {
    Protos.FrameworkInfo info = CommandLineDriverSettingsModule.buildFrameworkInfo(
        "aurora",
        "user",
        Optional.of("auroraprincipal"),
        Amount.of(1L, Time.MINUTES),
        true,
        Optional.of(TEST_ROLE));
    assertEquals("auroraprincipal", info.getPrincipal());
    assertEquals(1, info.getCapabilitiesCount());
    assertEquals(REVOCABLE_RESOURCES, info.getCapabilities(0).getType());
    assertTrue(info.hasRole());
    assertEquals(TEST_ROLE, info.getRole());
  }
}
