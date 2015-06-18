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
package org.apache.aurora.auth;

import java.util.Set;
import java.util.logging.Logger;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;

import org.apache.aurora.auth.SessionValidator.SessionContext;
import org.apache.aurora.gen.SessionKey;

import static java.util.Objects.requireNonNull;

/**
 * An authentication module that uses an {@link UnsecureSessionValidator}. This behavior
 * can be overridden by binding a secure validator, querying an internal authentication system,
 * to {@link SessionValidator}.
 */
public class UnsecureAuthModule extends AbstractModule {
  private static final Logger LOG = Logger.getLogger(UnsecureAuthModule.class.getName());

  @Override
  protected void configure() {
    LOG.info("Using default (UNSECURE!!!) authentication module.");
    bind(SessionValidator.class).to(UnsecureSessionValidator.class);
    bind(CapabilityValidator.class).to(UnsecureCapabilityValidator.class);
    // NOTE: This binding is very important, as UnsecureSessionContext has an optional injection, so its provider must
    // be created in the same injector as the one that *might* have its optional dependency. Omitting this binding will
    // cause a Just-In-Time binding to be created in the parent injector, where it will not have access to the optional
    // dependency in the child injector (so its optional dependency will never be used). This was the cause of
    // https://issues.apache.org/jira/browse/AURORA-1352. This can be mitigated slightly by
    // https://issues.apache.org/jira/browse/AURORA-1357
    bind(SessionContext.class).to(UnsecureSessionContext.class);
  }

  static class UnsecureSessionValidator implements SessionValidator {
    private final SessionContext sessionContext;

    @Inject
    UnsecureSessionValidator(SessionContext sessionContext) {
      this.sessionContext = requireNonNull(sessionContext);
    }

    @Override
    public SessionContext checkAuthenticated(SessionKey key, Set<String> targetRoles)
        throws AuthFailedException {

      return sessionContext;
    }

    @Override
    public String toString(SessionKey sessionKey) {
      return sessionKey.toString();
    }
  }

  static class UnsecureCapabilityValidator implements CapabilityValidator {
    private final SessionValidator sessionValidator;
    private final SessionContext sessionContext;

    @Inject
    UnsecureCapabilityValidator(
        SessionValidator sessionValidator,
        SessionContext sessionContext) {

      this.sessionValidator = requireNonNull(sessionValidator);
      this.sessionContext = requireNonNull(sessionContext);
    }

    @Override
    public SessionContext checkAuthorized(SessionKey key, Capability capability, AuditCheck check)
        throws AuthFailedException {

      return sessionContext;
    }

    @Override
    public SessionContext checkAuthenticated(SessionKey key, Set<String> targetRoles)
        throws AuthFailedException {

      return sessionValidator.checkAuthenticated(key, targetRoles);
    }

    @Override
    public String toString(SessionKey sessionKey) {
      return sessionValidator.toString(sessionKey);
    }
  }
}
