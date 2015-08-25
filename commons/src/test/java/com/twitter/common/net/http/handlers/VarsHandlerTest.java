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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.stats.Stat;

import static org.junit.Assert.assertEquals;

/**
 * @author William Farner
 */
public class VarsHandlerTest extends StatSupplierTestBase {

  private VarsHandler vars;
  private HttpServletRequest request;

  @Before
  public void setUp() {
    statSupplier = createMock(new Clazz<Supplier<Iterable<Stat<?>>>>() {});
    request = createMock(HttpServletRequest.class);
    vars = new VarsHandler(statSupplier);
  }

  @Test
  public void testGetEmpty() {
    expectVarScrape(ImmutableMap.<String, Object>of());

    control.replay();

    checkOutput(Collections.<String>emptyList());
  }

  @Test
  public void testGet() {
    expectVarScrape(ImmutableMap.<String, Object>of(
        "float", 4.16126,
        "int", 5,
        "str", "foobar"
    ));

    control.replay();

    // expect the output to be in the same order
    checkOutput(Arrays.asList(
        "float 4.16126",
        "int 5",
        "str foobar"));
  }

  private void checkOutput(List<String> expectedLines) {
    assertEquals(expectedLines,
        ImmutableList.copyOf(vars.getLines(request)));
  }
}
