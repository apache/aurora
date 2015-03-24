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

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ModulesTest {
  private static final String STRING = "string";

  @Test
  public void testLazilyInstantiated() {
    Injector injector = Guice.createInjector(Modules.lazilyInstantiated(StringInstaller.class));
    assertEquals(STRING, injector.getInstance(String.class));
  }

  static class StringInstaller extends AbstractModule {
    @Override
    protected void configure() {
      bind(String.class).toInstance(STRING);
    }
  }
}
