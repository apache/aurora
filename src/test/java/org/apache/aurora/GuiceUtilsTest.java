/**
 * Copyright 2013 Apache Software Foundation
 *
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
package org.apache.aurora;

import java.util.List;

import javax.inject.Singleton;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.CreationException;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.aurora.GuiceUtils.AllowUnchecked;
import org.junit.Test;

import static org.junit.Assert.fail;

public class GuiceUtilsTest {

  interface Flaky {
    void get();
    void append(List<String> strings);
    void skipped();
  }

  interface AlsoFlaky extends Flaky {
    void get(int value);
    void put(String s);
  }

  static class NotSoFlakyImpl implements AlsoFlaky {
    @Override
    public void put(String s) {
      throw new RuntimeException("Call failed");
    }

    @Override
    public void append(List<String> strings) {
      throw new RuntimeException("Call failed");
    }

    @AllowUnchecked
    @Override
    public void skipped() {
      throw new RuntimeException("Call failed");
    }

    @Override
    public void get(int value) {
      throw new RuntimeException("Call failed");
    }

    @Override public void get() {
      throw new RuntimeException("Call failed");
    }
  }

  @Test
  public void testExceptionTrapping() {
    Injector injector = Guice.createInjector(new AbstractModule() {
      @Override protected void configure() {
        GuiceUtils.bindExceptionTrap(binder(), Flaky.class);
        bind(Flaky.class).to(NotSoFlakyImpl.class);
        bind(AlsoFlaky.class).to(NotSoFlakyImpl.class);
        bind(NotSoFlakyImpl.class).in(Singleton.class);
      }
    });

    AlsoFlaky flaky = injector.getInstance(AlsoFlaky.class);
    flaky.get();
    flaky.append(ImmutableList.of("hi"));

    try {
      flaky.put("hello");
      fail("Should have thrown");
    } catch (RuntimeException e) {
      // Expected.
    }

    try {
      flaky.get(2);
      fail("Should have thrown");
    } catch (RuntimeException e) {
      // Expected.
    }

    try {
      flaky.skipped();
      fail("Should have thrown");
    } catch (RuntimeException e) {
      // Expected.
    }
  }

  interface NonVoid {
    int get();
  }

  @Test(expected = CreationException.class)
  public void testNoTrappingNonVoidMethods() {
    Guice.createInjector(new AbstractModule() {
      @Override protected void configure() {
        GuiceUtils.bindExceptionTrap(binder(), NonVoid.class);
        fail("Bind should have failed.");
      }
    });
  }

  interface NonVoidWhitelisted {
    @AllowUnchecked
    int getWhitelisted();
  }

  @Test
  public void testWhitelistNonVoidMethods() {
    Guice.createInjector(new AbstractModule() {
      @Override protected void configure() {
        GuiceUtils.bindExceptionTrap(binder(), NonVoidWhitelisted.class);
      }
    });
  }
}
