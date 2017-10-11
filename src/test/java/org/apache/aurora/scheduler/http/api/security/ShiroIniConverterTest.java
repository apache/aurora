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

import com.beust.jcommander.ParameterException;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.scheduler.http.api.security.ShiroIniConverter.ExtraSectionsException;
import org.apache.aurora.scheduler.http.api.security.ShiroIniConverter.MissingSectionsException;
import org.apache.shiro.io.ResourceUtils;
import org.apache.shiro.realm.text.IniRealm;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ShiroIniConverterTest {
  private ShiroIniConverter parser;

  public static final String EXAMPLE_RESOURCE = "shiro-example.ini";
  private static final String EXTRA_SECTIONS_SHIRO_INI = "shiro-malformed-extra-sections.ini";
  private static final String MISSING_SECTIONS_SHIRO_INI = "shiro-missing-sections.ini";
  private static final String NONEXISTENT_RESOURCE = "shiro-nonexistent.ini";
  private static final String NO_SECTIONS_SHURO_INI = "shiro-malformed-no-sections.ini";

  @Before
  public void setUp() {
    parser = new ShiroIniConverter();
  }

  @Test
  public void testDoParseSuccess() {
    assertEquals(
        ShiroIniConverter.ALLOWED_SECTION_NAMES,
        parser.convert(
            ShiroIniConverterTest.class.getResource(EXAMPLE_RESOURCE).toString())
            .getSectionNames());
  }

  @Test
  public void testDoParseOptionalSections() {
    assertEquals(
        ImmutableSet.of(IniRealm.ROLES_SECTION_NAME),
        parser
            .convert(ShiroIniConverterTest.class.getResource(MISSING_SECTIONS_SHIRO_INI).toString())
            .getSectionNames());
  }

  @Test(expected = ParameterException.class)
  public void testDoParseNonexistent() {
    parser.convert(ResourceUtils.CLASSPATH_PREFIX + NONEXISTENT_RESOURCE);
  }

  @Test(expected = ExtraSectionsException.class)
  public void testDoParseExtraSections() {
    parser.convert(getClass().getResource(EXTRA_SECTIONS_SHIRO_INI).toString());
  }

  @Test(expected = MissingSectionsException.class)
  public void testDoParseMissingSections() {
    parser.convert(getClass().getResource(NO_SECTIONS_SHURO_INI).toString());
  }
}
