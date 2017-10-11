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

import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;

import org.apache.aurora.scheduler.config.CliOptions;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MoreModulesTest {
  private static final String STRING = "string";

  @Test
  public void testInstantiate() {
    CliOptions options = new CliOptions();
    options.main.clusterName = "testing";
    Injector injector = Guice.createInjector(MoreModules.instantiateAll(
        ImmutableList.of(StringInstaller.class, StringSetInstaller.class),
        options));
    assertEquals(STRING, injector.getInstance(String.class));
    assertEquals(
        ImmutableSet.of(options.main.clusterName),
        injector.getInstance(Key.get(new TypeLiteral<Set<String>>() { })));
  }

  static class StringInstaller extends AbstractModule {
    @Override
    protected void configure() {
      bind(String.class).toInstance(STRING);
    }
  }

  public static class StringSetInstaller extends AbstractModule {
    private final CliOptions options;

    public StringSetInstaller(CliOptions options) {
      this.options = options;
    }

    @Override
    protected void configure() {
      bind(new TypeLiteral<Set<String>>() { })
          .toInstance(ImmutableSet.of(options.main.clusterName));
    }
  }
}
