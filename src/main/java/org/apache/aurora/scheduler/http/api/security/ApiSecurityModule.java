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

import java.lang.reflect.Method;
import java.util.Set;

import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.matcher.Matcher;
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
import org.apache.aurora.scheduler.app.Modules;
import org.apache.aurora.scheduler.http.api.ApiModule;
import org.apache.aurora.scheduler.thrift.aop.AnnotatedAuroraAdmin;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.guice.aop.ShiroAopModule;
import org.apache.shiro.guice.web.ShiroWebModule;
import org.apache.shiro.realm.text.IniRealm;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.web.filter.authc.BasicHttpAuthenticationFilter;

import static java.util.Objects.requireNonNull;

/**
 * Provides HTTP Basic Authentication for the API using Apache Shiro. When enabled, prevents
 * unauthenticated access to write APIs. Write API access must also be authorized, with permissions
 * configured in a shiro.ini file. For an example of this file, see the test resources included with
 * this package.
 */
public class ApiSecurityModule extends ServletModule {
  /**
   * Prefix for the permission protecting all AuroraSchedulerManager RPCs.
   */
  public static final String AURORA_SCHEDULER_MANAGER_PERMISSION = "thrift.AuroraSchedulerManager";

  /**
   * Prefix for the permission protecting all AuroraAdmin RPCs.
   */
  public static final String AURORA_ADMIN_PERMISSION = "thrift.AuroraAdmin";

  public static final String HTTP_REALM_NAME = "Apache Aurora Scheduler";

  @CmdLine(name = "enable_api_security",
      help = "Enable security for the Thrift API (beta).")
  private static final Arg<Boolean> ENABLE_API_SECURITY = Arg.create(false);

  @CmdLine(name = "shiro_realm_modules",
      help = "Guice modules for configuring Shiro Realms.")
  private static final Arg<Set<Module>> SHIRO_REALM_MODULE = Arg.<Set<Module>>create(
      ImmutableSet.of(Modules.lazilyInstantiated(IniShiroRealmModule.class)));

  @VisibleForTesting
  static final Matcher<Method> AURORA_SCHEDULER_MANAGER_SERVICE =
      GuiceUtils.interfaceMatcher(AuroraSchedulerManager.Iface.class, true);

  @VisibleForTesting
  static final Matcher<Method> AURORA_ADMIN_SERVICE =
      GuiceUtils.interfaceMatcher(AuroraAdmin.Iface.class, true);

  private final boolean enableApiSecurity;
  private final Set<Module> shiroConfigurationModules;

  public ApiSecurityModule() {
    this(ENABLE_API_SECURITY.get(), SHIRO_REALM_MODULE.get());
  }

  @VisibleForTesting
  ApiSecurityModule(Module shiroConfigurationModule) {
    this(true, ImmutableSet.of(shiroConfigurationModule));
  }

  private ApiSecurityModule(boolean enableApiSecurity, Set<Module> shiroConfigurationModules) {
    this.enableApiSecurity = enableApiSecurity;
    this.shiroConfigurationModules = requireNonNull(shiroConfigurationModules);
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
        for (Module module : shiroConfigurationModules) {
          // We can't wrap this in a PrivateModule because Guice Multibindings don't work with them
          // and we need a Set<Realm>.
          install(module);
        }

        addFilterChain("/**",
            ShiroWebModule.NO_SESSION_CREATION,
            config(ShiroWebModule.AUTHC_BASIC, BasicHttpAuthenticationFilter.PERMISSIVE));
      }
    });

    bind(IniRealm.class).in(Singleton.class);
    bindConstant().annotatedWith(Names.named("shiro.applicationName")).to(HTTP_REALM_NAME);

    // TODO(ksweeney): Disable session cookie.
    // TODO(ksweeney): Disable RememberMe cookie.

    install(new ShiroAopModule());

    // It is important that authentication happen before authorization is attempted, otherwise
    // the authorizing interceptor will always fail.
    MethodInterceptor authenticatingInterceptor = new ShiroAuthenticatingThriftInterceptor();
    requestInjection(authenticatingInterceptor);
    bindInterceptor(
        Matchers.subclassesOf(AuroraSchedulerManager.Iface.class),
        AURORA_SCHEDULER_MANAGER_SERVICE.or(AURORA_ADMIN_SERVICE),
        authenticatingInterceptor);

    MethodInterceptor apiInterceptor =
        new ShiroAuthorizingParamInterceptor(AURORA_SCHEDULER_MANAGER_PERMISSION);
    requestInjection(apiInterceptor);
    bindInterceptor(
        Matchers.subclassesOf(AuroraSchedulerManager.Iface.class),
        AURORA_SCHEDULER_MANAGER_SERVICE,
        apiInterceptor);

    MethodInterceptor adminInterceptor = new ShiroAuthorizingInterceptor(AURORA_ADMIN_PERMISSION);
    requestInjection(adminInterceptor);
    bindInterceptor(
        Matchers.subclassesOf(AnnotatedAuroraAdmin.class),
        AURORA_ADMIN_SERVICE,
        adminInterceptor);
  }

  @Provides
  @RequestScoped
  Subject provideSubject() {
    return SecurityUtils.getSubject();
  }
}
