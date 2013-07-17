package com.twitter.aurora;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.CreationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;

import org.junit.Test;

import com.twitter.aurora.GuiceUtils.AllowUnchecked;

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
