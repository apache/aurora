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
package com.twitter.common.net.http.handlers;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author William Farner
 */
public class VarsJsonHandlerTest extends StatSupplierTestBase {

  private VarsJsonHandler varsJson;

  @Before
  public void setUp() {
    varsJson = new VarsJsonHandler(statSupplier);
  }

  @Test
  public void testGetEmpty() {
    expectVarScrape(ImmutableMap.<String, Object>of());

    control.replay();

    assertEquals("{}", varsJson.getBody(false));
  }

  @Test
  public void testGet() {
    expectVarScrape(ImmutableMap.<String, Object>of(
        "str", "foobar",
        "int", 5,
        "float", 4.16126
    ));

    control.replay();

    assertEquals("{\"str\":\"foobar\",\"int\":5,\"float\":4.16126}", varsJson.getBody(false));
  }

  @Test
  public void testGetPretty() {
    expectVarScrape(ImmutableMap.<String, Object>of(
        "str", "foobar",
        "int", 5,
        "float", 4.16126
    ));

    control.replay();

    assertEquals("{\n" +
        "  \"str\": \"foobar\",\n" +
        "  \"int\": 5,\n" +
        "  \"float\": 4.16126\n" +
        "}", varsJson.getBody(true));
  }
}
