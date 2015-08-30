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
package org.apache.aurora.common.inject;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.util.List;

import javax.inject.Qualifier;

import com.google.inject.AbstractModule;
import com.google.inject.BindingAnnotation;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;

import org.junit.Assert;
import org.junit.Test;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BindingsTest {

  private static final TypeLiteral<List<String>> STRING_LIST = new TypeLiteral<List<String>>() { };

  @Retention(RUNTIME)
  @BindingAnnotation
  @interface BindKey { }

  @Retention(RUNTIME)
  @Qualifier
  @interface QualifierKey { }

  @Test
  public void testCheckBindingAnnotation() {
    Bindings.checkBindingAnnotation(BindKey.class);

    try {
      Bindings.checkBindingAnnotation((Class<? extends Annotation>) null);
      fail();
    } catch (NullPointerException e) {
      // expected
    }

    try {
      Bindings.checkBindingAnnotation(BindingAnnotation.class);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  public BindingsTest() {
    super();    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Test
  public void testPlainKeyFactory() {
    Assert.assertEquals(Key.get(String.class), Bindings.KeyFactory.PLAIN.create(String.class));
    Assert.assertEquals(Key.get(STRING_LIST), Bindings.KeyFactory.PLAIN.create(STRING_LIST));
  }

  @Test
  public void testAnnotationTypeKeyFactory() {
    Bindings.KeyFactory factory = Bindings.annotatedKeyFactory(QualifierKey.class);
    assertEquals(Key.get(String.class, QualifierKey.class), factory.create(String.class));
    assertEquals(Key.get(STRING_LIST, QualifierKey.class), factory.create(STRING_LIST));
  }

  @Test
  public void testExposing() {
    Injector injector =
        Guice.createInjector(Bindings.exposing(Key.get(String.class),
            new AbstractModule() {
              @Override protected void configure() {
                bind(String.class).toInstance("jake");
                bind(Integer.class).toInstance(42);
              }
            }));

    assertTrue(injector.getBindings().containsKey(Key.get(String.class)));
    assertEquals("jake", injector.getInstance(String.class));

    assertFalse(injector.getBindings().containsKey(Key.get(Integer.class)));
  }
}
