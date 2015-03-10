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
import com.google.inject.Provides;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;
import com.google.inject.servlet.RequestScoped;
import com.google.inject.servlet.ServletModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;

import org.aopalliance.intercept.MethodInterceptor;
import org.apache.aurora.GuiceUtils;
import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.AuroraSchedulerManager;
import org.apache.aurora.scheduler.http.api.ApiModule;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.config.Ini;
import org.apache.shiro.guice.aop.ShiroAopModule;
import org.apache.shiro.guice.web.ShiroWebModule;
import org.apache.shiro.realm.text.IniRealm;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.web.filter.authc.BasicHttpAuthenticationFilter;

/**
 * Provides HTTP Basic Authentication for the API using Apache Shiro. When enabled, prevents
 * unauthenticated access to write APIs. Write API access must also be authorized, with permissions
 * configured in a shiro.ini file. For an example of this file, see the test resources included with
 * this package.
 */
public class ApiSecurityModule extends ServletModule {
  public static final String HTTP_REALM_NAME = "Apache Aurora Scheduler";

  @CmdLine(name = "enable_api_security",
      help = "Enable security for the Thrift API (beta).")
  private static final Arg<Boolean> ENABLE_API_SECURITY = Arg.create(false);

  @CmdLine(name = "shiro_ini_path",
      help = "Path to shiro.ini for authentication and authorization configuration.")
  private static final Arg<Ini> SHIRO_INI_PATH = Arg.create(null);

  private final boolean enableApiSecurity;
  private final Optional<Ini> shiroIni;

  public ApiSecurityModule() {
    this(ENABLE_API_SECURITY.get(), Optional.fromNullable(SHIRO_INI_PATH.get()));
  }

  @VisibleForTesting
  ApiSecurityModule(Ini shiroIni) {
    this(true, Optional.of(shiroIni));
  }

  private ApiSecurityModule(boolean enableApiSecurity, Optional<Ini> shiroIni) {
    this.enableApiSecurity = enableApiSecurity;
    this.shiroIni = shiroIni;
  }

  @Override
  protected void configureServlets() {
    if (enableApiSecurity) {
      doConfigureServlets();
    }
  }

  private void doConfigureServlets() {
    install(ShiroWebModule.guiceFilterModule(ApiModule.API_PATH));
    install(new ShiroWebModule(getServletContext()) {
      @Override
      @SuppressWarnings("unchecked")
      protected void configureShiroWeb() {
        try {
          bindRealm().toConstructor(IniRealm.class.getConstructor(Ini.class));
        } catch (NoSuchMethodException e) {
          addError(e);
        }

        addFilterChain("/**",
            ShiroWebModule.NO_SESSION_CREATION,
            config(ShiroWebModule.AUTHC_BASIC, BasicHttpAuthenticationFilter.PERMISSIVE));
      }
    });

    if (shiroIni.isPresent()) {
      bind(Ini.class).toInstance(shiroIni.get());
    } else {
      addError("shiro.ini is required.");
    }
    bind(IniRealm.class).in(Singleton.class);
    bindConstant().annotatedWith(Names.named("shiro.applicationName")).to(HTTP_REALM_NAME);

    // TODO(ksweeney): Disable session cookie.
    // TODO(ksweeney): Disable RememberMe cookie.

    install(new ShiroAopModule());
    MethodInterceptor apiInterceptor = new ShiroThriftInterceptor("thrift.AuroraSchedulerManager");
    requestInjection(apiInterceptor);
    bindInterceptor(
        Matchers.subclassesOf(AuroraSchedulerManager.Iface.class),
        GuiceUtils.interfaceMatcher(AuroraSchedulerManager.Iface.class, true),
        apiInterceptor);

    MethodInterceptor adminInterceptor = new ShiroThriftInterceptor("thrift.AuroraAdmin");
    requestInjection(adminInterceptor);
    bindInterceptor(
        Matchers.subclassesOf(AuroraAdmin.Iface.class),
        GuiceUtils.interfaceMatcher(AuroraAdmin.Iface.class, true),
        adminInterceptor);
  }

  @Provides
  @RequestScoped
  Subject provideSubject() {
    return SecurityUtils.getSubject();
  }
}
