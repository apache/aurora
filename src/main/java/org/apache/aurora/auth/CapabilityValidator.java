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

import org.apache.aurora.gen.SessionKey;

/**
 * A session validator that supports user capability matching.
 * <p>
 * This supports asking whether a user has been granted a specific administration capability.
 */
public interface CapabilityValidator extends SessionValidator {

  enum Capability {
    ROOT,
    MACHINE_MAINTAINER,
    PROVISIONER
  }

  /**
   * Audit check to perform for a given action.
   */
  enum AuditCheck {
    /**
     * Default. No audit checks will be performed.
     */
    NONE,

    /**
     * A check will be performed to verify if a given action has
     * all necessary data to generate a valid audit trail.
     */
    REQUIRED
  }

  /**
   * Checks whether a session key is authenticated, and has the specified capability.
   *
   * @param sessionKey Key to validate.
   * @param capability User capability to authenticate against.
   * @param check Auditing data presence check required.
   * @return  A {@link SessionContext} object that provides information about the validated session.
   * @throws AuthFailedException If the key cannot be validated as the role or lacks
   * the requested capability.
   */
  SessionContext checkAuthorized(SessionKey sessionKey, Capability capability, AuditCheck check)
      throws AuthFailedException;
}
