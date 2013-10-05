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
package com.twitter.aurora.scheduler.storage.entities;

import com.google.common.base.Function;

import com.twitter.aurora.gen.Identity;

/**
 * An immutable wrapper class.
 * <p>
 * This code is auto-generated, and should not be directly modified.
 * <p>
 * Yes, you're right, it shouldn't be checked in.  We'll get there, I promise.
 */
public class IIdentity {
  private final Identity wrapped;

  public IIdentity(Identity wrapped) {
    this.wrapped = wrapped.deepCopy();
  }

  public static final Function<IIdentity, Identity> TO_BUILDER =
      new Function<IIdentity, Identity>() {
        @Override
        public Identity apply(IIdentity input) {
          return input.newBuilder();
        }
      };

  public static final Function<Identity, IIdentity> FROM_BUILDER =
      new Function<Identity, IIdentity>() {
        @Override
        public IIdentity apply(Identity input) {
          return new IIdentity(input);
        }
      };

  public Identity newBuilder() {
    return wrapped.deepCopy();
  }

  public String getRole() {
    return wrapped.getRole();
  }

  public String getUser() {
    return wrapped.getUser();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof IIdentity)) {
      return false;
    }
    IIdentity other = (IIdentity) o;
    return wrapped.equals(other.wrapped);
  }

  @Override
  public int hashCode() {
    return wrapped.hashCode();
  }

  @Override
  public String toString() {
    return wrapped.toString();
  }
}
