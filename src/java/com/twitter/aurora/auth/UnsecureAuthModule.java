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
import java.util.logging.Logger;

import com.google.inject.AbstractModule;

import com.twitter.aurora.gen.SessionKey;

/**
 * An authentication module that uses an {@link UnsecureSessionValidator}. This behavior
 * can be overridden by binding a secure validator, querying an internal authentication system,
 * to {@link SessionValidator}.
 */
public class UnsecureAuthModule extends AbstractModule {
  private static final String UNSECURE = "UNSECURE";
  private static final Logger LOG = Logger.getLogger(UnsecureAuthModule.class.getName());

  @Override
  protected void configure() {
    LOG.info("Using default (UNSECURE!!!) authentication module.");
    bind(SessionValidator.class).to(UnsecureSessionValidator.class);
    bind(CapabilityValidator.class).to(UnsecureCapabilityValidator.class);
  }

  static class UnsecureSessionValidator implements SessionValidator {
    @Override
    public SessionContext checkAuthenticated(SessionKey key, Set<String> targetRoles)
        throws AuthFailedException {

      return new SessionContext() {
        @Override public String getIdentity() {
          return UNSECURE;
        }
      };
    }

    @Override
    public String toString(SessionKey sessionKey) {
      return sessionKey.toString();
    }
  }

  static class UnsecureCapabilityValidator implements CapabilityValidator {
    @Override
    public SessionContext checkAuthorized(SessionKey key, Capability capability)
        throws AuthFailedException {

      return new SessionContext() {
        @Override public String getIdentity() {
          return UNSECURE;
        }
      };
    }

    @Override
    public SessionContext checkAuthenticated(SessionKey key, Set<String> targetRoles)
        throws AuthFailedException {

      return new SessionContext() {
        @Override public String getIdentity() {
          return UNSECURE;
        }
      };
    }

    @Override
    public String toString(SessionKey sessionKey) {
      return sessionKey.toString();
    }
  }
}
