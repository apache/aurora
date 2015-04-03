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

import javax.inject.Provider;

import com.google.inject.Inject;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.shiro.authz.UnauthenticatedException;
import org.apache.shiro.subject.Subject;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkState;

/**
 * Prevents invocation of intercepted methods if the current {@link Subject} is not authenticated.
 */
class ShiroAuthenticatingThriftInterceptor implements MethodInterceptor {
  private volatile boolean initialized;

  private Provider<Subject> subjectProvider;

  ShiroAuthenticatingThriftInterceptor() {
    // Guice constructor.
  }

  @Inject
  void initialize(Provider<Subject> newSubjectProvider) {
    checkState(!initialized);

    subjectProvider = requireNonNull(newSubjectProvider);

    initialized = true;
  }

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    checkState(initialized);
    Subject subject = subjectProvider.get();
    if (subject.isAuthenticated()) {
      return invocation.proceed();
    } else {
      // This is a special exception that will signal the BasicHttpAuthenticationFilter to send
      // a 401 with a challenge. This is necessary at this layer since we only apply this
      // interceptor to methods that require authentication.
      throw new UnauthenticatedException();
    }
  }
}
