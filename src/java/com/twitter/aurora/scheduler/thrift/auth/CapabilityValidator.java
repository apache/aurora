package com.twitter.aurora.scheduler.thrift.auth;

import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import com.twitter.aurora.auth.SessionValidator;
import com.twitter.aurora.gen.SessionKey;

/**
 * A session validator that supports user capability matching.
 * <p>
 * This supports asking whether a user has been granted a specific administration capability.
 */
public interface CapabilityValidator extends SessionValidator {

  enum Capability {
    ROOT,
    PROVISIONER
  }

  /**
   * Checks whether a session key is authenticated, and has the specified capability.
   *
   * @param sessionKey Key to validate.
   * @param capability User capability to authenticate against.
   * @throws AuthFailedException If the key cannot be validated as the role.
   */
  void checkAuthorized(SessionKey sessionKey, Capability capability) throws AuthFailedException;

  /**
   * A capability validator that delegates to a provided {@link SessionValidator}, translating
   * user capabilities using a supplied concrete mapping.
   */
  class CapabilityValidatorImpl implements CapabilityValidator {
    private final SessionValidator sessionValidator;
    private final Map<Capability, String> mapping;

    @Inject
    CapabilityValidatorImpl(SessionValidator sessionValidator, Map<Capability, String> mapping) {
      this.sessionValidator = Preconditions.checkNotNull(sessionValidator);
      this.mapping = Preconditions.checkNotNull(mapping);
    }

    @Override
    public void checkAuthorized(SessionKey sessionKey, Capability capability)
        throws AuthFailedException {

      checkAuthenticated(
          sessionKey,
          Optional.of(mapping.get(capability)).or(mapping.get(Capability.ROOT)));
    }

    @Override
    public void checkAuthenticated(SessionKey sessionKey, String targetRole)
        throws AuthFailedException {

      sessionValidator.checkAuthenticated(sessionKey, targetRole);
    }
  }
}
