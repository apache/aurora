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
package org.apache.aurora.scheduler.thrift.aop;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.inject.Binder;

import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.scheduler.thrift.auth.DecoratedThrift;

/**
 * An injected forwarding thrift implementation that delegates to a bound mock interface.
 * <p>
 * This is required to allow AOP to take place. For more details, see
 * https://code.google.com/p/google-guice/wiki/AOP#Limitations
 */
@DecoratedThrift
public class MockDecoratedThrift extends ForwardingThrift {

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.PARAMETER, ElementType.METHOD})
  @Qualifier
  private @interface MockThrift { }

  @Inject
  MockDecoratedThrift(@MockThrift AuroraAdmin.Iface delegate) {
    super(delegate);
  }

  public static void bindForwardedMock(Binder binder, AuroraAdmin.Iface mockThrift) {
    binder.bind(AuroraAdmin.Iface.class).annotatedWith(MockThrift.class).toInstance(mockThrift);
    binder.bind(AuroraAdmin.Iface.class).to(MockDecoratedThrift.class);
  }
}
