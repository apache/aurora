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

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import javax.inject.Inject;
import javax.inject.Provider;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.stats.StatsProvider;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.aurora.gen.Response;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.scheduler.thrift.Responses;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.WildcardPermission;
import org.apache.shiro.subject.Subject;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Does not allow the intercepted method call to proceed if the caller does not have permission to
 * call it.
 * The {@link org.apache.shiro.authz.Permission} checked is a
 * {@link org.apache.shiro.authz.permission.WildcardPermission} constructed from a prefix and
 * the name of the method being invoked. For example if the prefix is {@code api} and the method
 * is {@code snapshot} the current {@link org.apache.shiro.subject.Subject} must have the
 * {@code api:snapshot} permission.
 */
class ShiroAuthorizingInterceptor implements MethodInterceptor {
  private static final Logger LOG = Logger.getLogger(ShiroAuthorizingInterceptor.class.getName());

  @VisibleForTesting
  static final String SHIRO_AUTHORIZATION_FAILURES = "shiro_authorization_failures";

  private static final Joiner PERMISSION_JOINER = Joiner.on(":");

  private final String permissionPrefix;

  private volatile boolean initialized;

  private Provider<Subject> subjectProvider;
  private AtomicLong shiroAdminAuthorizationFailures;

  ShiroAuthorizingInterceptor(String permissionPrefix) {
    this.permissionPrefix = MorePreconditions.checkNotBlank(permissionPrefix);
  }

  @Inject
  void initialize(Provider<Subject> newSubjectProvider, StatsProvider statsProvider) {
    checkState(!initialized);

    subjectProvider = requireNonNull(newSubjectProvider);
    shiroAdminAuthorizationFailures = statsProvider.makeCounter(SHIRO_AUTHORIZATION_FAILURES);

    initialized = true;
  }

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    checkState(initialized);
    Method method = invocation.getMethod();
    checkArgument(Response.class.isAssignableFrom(method.getReturnType()));

    Subject subject = subjectProvider.get();
    Permission checkedPermission = new WildcardPermission(
        PERMISSION_JOINER.join(permissionPrefix, method.getName()));
    if (subject.isPermitted(checkedPermission)) {
      return invocation.proceed();
    } else {
      shiroAdminAuthorizationFailures.incrementAndGet();
      String responseMessage =
          "Subject " + subject.getPrincipal() + " lacks permission " + checkedPermission;
      LOG.warning(responseMessage);
      // TODO(ksweeney): 403 FORBIDDEN would be a more accurate translation of this response code.
      return Responses.addMessage(Responses.empty(), ResponseCode.AUTH_FAILED, responseMessage);
    }
  }
}
