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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.aurora.auth.CapabilityValidator;
import org.apache.aurora.auth.CapabilityValidator.AuditCheck;
import org.apache.aurora.auth.CapabilityValidator.Capability;
import org.apache.aurora.auth.SessionValidator.AuthFailedException;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.SessionKey;
import org.apache.aurora.scheduler.thrift.Responses;
import org.apache.aurora.scheduler.thrift.auth.Requires;

/**
 * A method interceptor that will authenticate users identified by a {@link SessionKey} argument
 * to invoked methods.
 * <p>
 * Intercepted methods will require {@link Capability#ROOT}, but additional capabilities
 * may be specified by annotating methods with {@link Requires} and supplying a whitelist.
 */
class UserCapabilityInterceptor implements MethodInterceptor {
  private static final Logger LOG = Logger.getLogger(UserCapabilityInterceptor.class.getName());

  @Inject private CapabilityValidator capabilityValidator;

  private static final Function<Object, SessionKey> CAST = new Function<Object, SessionKey>() {
    @Override
    public SessionKey apply(Object o) {
      return (SessionKey) o;
    }
  };

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    Objects.requireNonNull(capabilityValidator, "Session validator has not yet been set.");

    // Ensure ROOT is always permitted.
    ImmutableList.Builder<Capability> whitelistBuilder =
        ImmutableList.<Capability>builder().add(Capability.ROOT);

    Method method = invocation.getMethod();
    Requires requires = method.getAnnotation(Requires.class);
    if (requires != null) {
      whitelistBuilder.add(requires.whitelist());
    }

    List<Capability> whitelist = whitelistBuilder.build();
    LOG.fine("Operation " + method.getName() + " may be performed by: " + whitelist);
    Optional<SessionKey> sessionKey = FluentIterable.from(Arrays.asList(invocation.getArguments()))
        .firstMatch(Predicates.instanceOf(SessionKey.class)).transform(CAST);
    if (!sessionKey.isPresent()) {
      LOG.severe("Interceptor should only be applied to methods accepting a SessionKey, but "
          + method + " does not.");
      return invocation.proceed();
    }

    String key = capabilityValidator.toString(sessionKey.get());
    for (Capability user : whitelist) {
      LOG.fine("Attempting to validate " + key + " against " + user);
      try {
        capabilityValidator.checkAuthorized(sessionKey.get(), user, AuditCheck.NONE);

        LOG.info("Permitting " + key + " to act as "
            + user + " and perform action " + method.getName());
        return invocation.proceed();
      } catch (AuthFailedException e) {
        LOG.fine("Auth failed: " + e);
      }
    }

    // User is not permitted to perform this operation.
    return Responses.addMessage(
        Responses.empty(),
        ResponseCode.AUTH_FAILED,
        "Session identified by '" + key
            + "' does not have the required capability to perform this action: " + whitelist);
  }
}
