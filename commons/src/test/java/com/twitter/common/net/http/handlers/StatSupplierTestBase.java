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

import java.util.List;
import java.util.Map;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

import org.junit.Before;

import com.twitter.common.stats.Stat;
import com.twitter.common.testing.easymock.EasyMockTest;

import static org.easymock.EasyMock.expect;

/**
 * @author William Farner
 */
public abstract class StatSupplierTestBase extends EasyMockTest {

  protected Supplier<Iterable<Stat<?>>> statSupplier;

  @Before
  public void statSupplierSetUp() {
    statSupplier = createMock(new Clazz<Supplier<Iterable<Stat<?>>>>() {});
  }

  protected void expectVarScrape(Map<String, Object> response) {
    List<Stat<?>> vars = Lists.newArrayList();
    for (Map.Entry<String, Object> entry : response.entrySet()) {
      Stat stat = createMock(Stat.class);
      expect(stat.getName()).andReturn(entry.getKey());
      expect(stat.read()).andReturn(entry.getValue());
      vars.add(stat);
    }

    expect(statSupplier.get()).andReturn(vars);
  }
}
