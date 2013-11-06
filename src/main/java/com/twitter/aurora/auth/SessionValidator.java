/*
 * Copyright 2013 Twitter, Inc.
 *
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
package com.twitter.aurora.auth;

import java.util.Set;

import com.twitter.aurora.gen.SessionKey;

/**
 * Validator for RPC sessions with Aurora.
 */
public interface SessionValidator {

  /**
   * Checks whether a session key is authenticated, and has permission to act as all the roles
   * provided. Authentication is successful only if the SessionKey is successfully validated against
   * all the roles.
   *
   * @param sessionKey Key to validate.
   * @param targetRoles A set of roles to validate the key against.
   * @return A {@link SessionContext} object that provides information about the validated session.
   * @throws AuthFailedException If the key cannot be validated against a role.
   */
  SessionContext checkAuthenticated(SessionKey sessionKey, Set<String> targetRoles)
      throws AuthFailedException;

  /**
   * Translates a {@link SessionKey} to a string. Primarily provides a way for the binary data
   * within a {@link SessionKey} to be decoded and converted into a string.
   *
   * @param sessionKey The session key to translate.
   * @return A string representation of the {@link SessionKey}.
   */
  String toString(SessionKey sessionKey);

  /**
   * Provides information about a session.
   */
  interface SessionContext {

    /**
     * Provides the identity for a validated session.
     *
     * @return A string that identifies the session.
     */
    String getIdentity();
  }

  /**
   * Thrown when authentication is not successful.
   */
  public static class AuthFailedException extends Exception {
    public AuthFailedException(String msg) {
      super(msg);
    }

    public AuthFailedException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }
}
