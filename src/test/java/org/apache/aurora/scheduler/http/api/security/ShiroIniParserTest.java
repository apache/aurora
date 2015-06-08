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

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.scheduler.http.api.security.ShiroIniParser.ExtraSectionsException;
import org.apache.aurora.scheduler.http.api.security.ShiroIniParser.MissingSectionsException;
import org.apache.aurora.scheduler.http.api.security.ShiroIniParser.ShiroConfigurationException;
import org.apache.shiro.io.ResourceUtils;
import org.apache.shiro.realm.text.IniRealm;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ShiroIniParserTest {
  private ShiroIniParser parser;

  private static final String EXAMPLE_RESOURCE = "shiro-example.ini";
  private static final String EXTRA_SECTIONS_SHIRO_INI = "shiro-malformed-extra-sections.ini";
  private static final String MISSING_SECTIONS_SHIRO_INI = "shiro-missing-sections.ini";
  private static final String NONEXISTENT_RESOURCE = "shiro-nonexistent.ini";
  private static final String NO_SECTIONS_SHURO_INI = "shiro-malformed-no-sections.ini";

  @Before
  public void setUp() {
    parser = new ShiroIniParser();
  }

  @Test
  public void testDoParseSuccess() {
    assertEquals(
        ShiroIniParser.ALLOWED_SECTION_NAMES,
        parser.doParse(
            ShiroIniParserTest.class.getResource(EXAMPLE_RESOURCE).toString()).getSectionNames());
  }

  @Test
  public void testDoParseOptionalSections() {
    assertEquals(
        ImmutableSet.of(IniRealm.ROLES_SECTION_NAME),
        parser
            .doParse(ShiroIniParserTest.class.getResource(MISSING_SECTIONS_SHIRO_INI).toString())
            .getSectionNames());
  }

  @Test(expected = ShiroConfigurationException.class)
  public void testDoParseNonexistent() {
    parser.doParse(ResourceUtils.CLASSPATH_PREFIX + NONEXISTENT_RESOURCE);
  }

  @Test(expected = ExtraSectionsException.class)
  public void testDoParseExtraSections() {
    parser.doParse(getClass().getResource(EXTRA_SECTIONS_SHIRO_INI).toString());
  }

  @Test(expected = MissingSectionsException.class)
  public void testDoParseMissingSections() {
    parser.doParse(getClass().getResource(NO_SECTIONS_SHURO_INI).toString());
  }
}
