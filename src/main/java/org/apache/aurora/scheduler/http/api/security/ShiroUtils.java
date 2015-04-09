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

import com.google.inject.Binder;
import com.google.inject.binder.LinkedBindingBuilder;
import com.google.inject.multibindings.Multibinder;

import org.apache.shiro.realm.Realm;

/**
 * Utilities for configuring Shiro.
 */
public final class ShiroUtils {
  private ShiroUtils() {
    // Utility class.
  }

  /**
   * Enable a new Shiro Realm. All realms enabled this way are passed to a
   * {@link org.apache.shiro.authc.pam.ModularRealmAuthenticator}, which is used as the primary
   * {@link org.apache.shiro.realm.Realm} for the application.
   *
   * @param binder The current module's binder.
   * @return A binding builder that can be used to add a realm to the injector.
   */
  public static LinkedBindingBuilder<Realm> addRealmBinding(Binder binder) {
    return Multibinder.newSetBinder(binder, Realm.class).addBinding();
  }
}
