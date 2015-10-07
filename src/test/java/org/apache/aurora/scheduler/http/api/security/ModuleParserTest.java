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

import com.google.inject.Binder;
import com.google.inject.Module;

import org.junit.Test;

public class ModuleParserTest {
  static class NoOpModule implements Module {
    NoOpModule() {
      // No-op.
    }

    @Override
    public void configure(Binder binder) {
      // No-op.
    }
  }

  static class NoNullaryConstructorModule implements Module {
    private String name;

    NoNullaryConstructorModule(String name) {
      this.name = name;
    }

    @Override
    public void configure(Binder binder) {
      binder.bind(String.class).toInstance(name);
    }
  }

  static class ThrowingConstructorModule implements Module {
    ThrowingConstructorModule() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void configure(Binder binder) {
      // Unreachable.
    }
  }

  @Test
  public void testDoParseSuccess() {
    new ModuleParser().doParse(NoOpModule.class.getName());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDoParseClassNotFound() {
    new ModuleParser().doParse("invalid.class.name");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDoParseNotModule() {
    new ModuleParser().doParse(String.class.getCanonicalName());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDoParseNoNullaryConstructor() {
    new ModuleParser().doParse(NoNullaryConstructorModule.class.getCanonicalName());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDoParseThrowingConstructorModule() {
    new ModuleParser().doParse(ThrowingConstructorModule.class.getCanonicalName());
  }
}
