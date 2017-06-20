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

import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.shiro.authc.credential.CredentialsMatcher;
import org.apache.shiro.authc.credential.SimpleCredentialsMatcher;
import org.apache.shiro.config.Ini;
import org.apache.shiro.realm.text.IniRealm;

/**
 * Provides an implementation of a Shiro {@link IniRealm} that uses a flat shiro.ini file for
 * authentication and authorization. Should be used in conjunction with the
 * {@link org.apache.shiro.web.filter.authc.BasicHttpAuthenticationFilter} or other filter that
 * produces {@link org.apache.shiro.authc.UsernamePasswordToken}s.
 *
 * <p>
 * Another filter may still be used for authentication, in which case the ini file can still be
 * used to provide authorization configuration and the passwords will be ignored.
 */
public class IniShiroRealmModule extends AbstractModule {
  @CmdLine(name = "shiro_ini_path",
      help = "Path to shiro.ini for authentication and authorization configuration.")
  private static final Arg<Ini> SHIRO_INI_PATH = Arg.create(null);

  @CmdLine(name = "shiro_credentials_matcher",
      help = "The shiro credentials matcher to use (will be constructed by Guice).")
  private static final Arg<Class<? extends CredentialsMatcher>> SHIRO_CREDENTIALS_MATCHER =
      Arg.<Class<? extends CredentialsMatcher>>create(SimpleCredentialsMatcher.class);

  private final Optional<Ini> ini;
  private final Optional<Class<? extends CredentialsMatcher>> shiroCredentialsMatcher;

  public IniShiroRealmModule() {
    this(Optional.fromNullable(SHIRO_INI_PATH.get()),
        Optional.fromNullable(SHIRO_CREDENTIALS_MATCHER.get()));
  }

  @VisibleForTesting
  IniShiroRealmModule(Ini ini, Class<? extends CredentialsMatcher> shiroCredentialsMatcher) {
    this(Optional.of(ini), Optional.of(shiroCredentialsMatcher));
  }

  private IniShiroRealmModule(Optional<Ini> ini,
      Optional<Class<? extends CredentialsMatcher>> shiroCredentialsMatcher) {
    this.ini = ini;
    this.shiroCredentialsMatcher = shiroCredentialsMatcher;
  }

  @Override
  protected void configure() {
    if (ini.isPresent()) {
      bind(Ini.class).toInstance(ini.get());
    } else {
      addError("shiro.ini is required.");
    }

    if (shiroCredentialsMatcher.isPresent()) {
      bind(CredentialsMatcher.class).to(shiroCredentialsMatcher.get()).in(Singleton.class);
    } else {
      addError("shiro_credentials_matcher is required.");
    }

    ShiroUtils.addRealmBinding(binder()).to(IniRealm.class);
  }

  @Singleton
  @Provides
  public IniRealm providesIniReal(Ini providedIni,
      CredentialsMatcher providedShiroCredentialsMatcher) {
    IniRealm result = new IniRealm(providedIni);
    result.setCredentialsMatcher(providedShiroCredentialsMatcher);
    result.init();

    return result;
  }
}
