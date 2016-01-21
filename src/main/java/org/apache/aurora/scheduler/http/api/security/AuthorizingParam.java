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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Signals to {@link ShiroAuthorizingParamInterceptor} that this  parameter should be used to
 * determine the instance the calling subject is attempting to operate on. Methods using this
 * parameter should ensure that the RPC cannot operate on an instance outside the scope of this
 * parameter, otherwise a privilege escalation vulnerability exists.
 *
 * <p>
 * A method intercepted by this interceptor that does not contain an AuthorizingParam will result
 * in an {@link java.lang.IllegalStateException} from the interceptor.
 *
 * <p>
 * It is possible to have more than one AuthorizingParam annotations applied to a method. This may
 * be needed to ensure graceful deprecation cycle of the old annotated parameter for backwards
 * compatibility reasons. In such cases, only one of the annotated arguments must be non-null or
 * an {@link java.lang.IllegalStateException} is thrown.
 *
 * <p>
 * If the parameter type is not known to the interceptor an {@link java.lang.IllegalStateException}
 * is thrown.
 *
 * <p>
 * Inheritance semantics: while the Java Reflection API doesn't define inheritance for method
 * parameter annotations the interceptor uses the first matching superclass where the method is
 * defined and annotated. This means that
 *
 * <pre>
 * <code>
 * public class A {
 *   Response getItem(@AuthorizingParam String itemId) {
 *     // ...
 *   }
 * }
 * </code>
 * </pre>
 *
 * and
 *
 * <pre>
 * <code>
 * public interface A {
 *   Response getItem(@AuthorizingParam String itemId);
 * }
 *
 * public class B implements A {
 *   {@literal @}Override
 *   Response getItem(String itemId) {
 *     // ...
 *   }
 * }
 * </code>
 * </pre>
 *
 * are equivalent.
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface AuthorizingParam { }
