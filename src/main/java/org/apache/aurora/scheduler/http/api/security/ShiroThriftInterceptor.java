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

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.inject.Provider;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.inject.Inject;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.stats.StatsProvider;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.scheduler.thrift.Responses;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.UnauthenticatedException;
import org.apache.shiro.authz.permission.WildcardPermission;
import org.apache.shiro.subject.Subject;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkState;

/**
 * Prevents invocation of intercepted methods if the current {@link Subject} is not authenticated
 * or the current subject does not have the permission defined a prefix the name of the method
 * attempting to be invoked. The {@link org.apache.shiro.authz.Permission} checked is a
 * {@link org.apache.shiro.authz.permission.WildcardPermission} constructed from the prefix and
 * the name of the method being invoked, for example if the prefix is {@code api} and the method
 * is {@code snapshot} the current subject must have the permission {@code api:snapshot} permission.
 */
class ShiroThriftInterceptor implements MethodInterceptor {
  private static final Logger LOG = Logger.getLogger(ShiroThriftInterceptor.class.getName());
  private static final Joiner PERMISSION_JOINER = Joiner.on(":");

  @VisibleForTesting
  static final String SHIRO_AUTHORIZATION_FAILURES = "shiro_authorization_failures";

  private final String permissionPrefix;

  private volatile boolean initialized;

  private Provider<Subject> subjectProvider;
  private AtomicLong shiroAuthorizationFailures;

  @Inject
  void initialize(Provider<Subject> newSubjectProvider, StatsProvider statsProvider) {
    checkState(!initialized);

    this.subjectProvider = requireNonNull(newSubjectProvider);
    shiroAuthorizationFailures = statsProvider.makeCounter(SHIRO_AUTHORIZATION_FAILURES);

    initialized = true;
  }

  public ShiroThriftInterceptor(String permissionPrefix) {
    this.permissionPrefix = MorePreconditions.checkNotBlank(permissionPrefix);
  }

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    checkState(initialized);

    Subject subject = subjectProvider.get();
    if (!subject.isAuthenticated()) {
      // This is a special exception that will signal the BasicHttpAuthenticationFilter to send
      // a 401 with a challenge. This is necessary at this layer since we only apply this
      // interceptor to methods that require authentication.
      throw new UnauthenticatedException();
    }

    Permission checkedPermission = new WildcardPermission(
        PERMISSION_JOINER.join(permissionPrefix, invocation.getMethod().getName()));
    if (subject.isPermitted(checkedPermission)) {
      return invocation.proceed();
    } else {
      shiroAuthorizationFailures.incrementAndGet();
      String responseMessage =
          "Subject " + subject.getPrincipal() + " lacks permission " + checkedPermission;
      LOG.warning(responseMessage);
      // TODO(ksweeney): 403 FORBIDDEN would be a more accurate translation of this response code.
      return Responses.addMessage(Responses.empty(), ResponseCode.AUTH_FAILED, responseMessage);
    }
  }
}
