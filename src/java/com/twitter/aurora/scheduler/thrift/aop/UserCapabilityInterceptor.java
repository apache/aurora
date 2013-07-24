package com.twitter.aurora.scheduler.thrift.aop;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import com.twitter.aurora.auth.SessionValidator.AuthFailedException;
import com.twitter.aurora.gen.ResponseCode;
import com.twitter.aurora.gen.SessionKey;
import com.twitter.aurora.scheduler.thrift.auth.CapabilityValidator;
import com.twitter.aurora.scheduler.thrift.auth.CapabilityValidator.Capability;
import com.twitter.aurora.scheduler.thrift.auth.Requires;

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
    @Override public SessionKey apply(Object o) {
      return (SessionKey) o;
    }
  };

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    Preconditions.checkNotNull(capabilityValidator, "Session validator has not yet been set.");

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
    Optional<SessionKey> key = FluentIterable.from(Arrays.asList(invocation.getArguments()))
        .firstMatch(Predicates.instanceOf(SessionKey.class)).transform(CAST);
    if (!key.isPresent()) {
      LOG.severe("Interceptor should only be applied to methods accepting a SessionKey, but "
          + method + " does not.");
      return invocation.proceed();
    }

    for (Capability user : whitelist) {
      LOG.fine("Attempting to validate " + key.get().getUser() + " as " + user);
      try {
        capabilityValidator.checkAuthorized(key.get(), user);
        LOG.info("Permitting " + key.get().getUser() + " to act as "
            + user + " and perform action " + method.getName());
        return invocation.proceed();
      } catch (AuthFailedException e) {
        LOG.fine("Auth failed: " + e);
      }
    }

    // User is not permitted to perform this operation.
    return Interceptors.properlyTypedResponse(
        method,
        ResponseCode.AUTH_FAILED,
        "Your user '" + key.get().getUser()
            + "' does not have the required capability to perform this action: " + whitelist);
  }
}
