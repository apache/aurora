/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.thrift;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;

import com.twitter.common.application.http.Registration;
import com.twitter.common.application.modules.LifecycleModule;

import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.scheduler.thrift.aop.AopModule;

/**
 * Binding module to configure a thrift server.
 */
public class ThriftModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(AuroraAdmin.Iface.class).to(SchedulerThriftInterface.class);
    bind(ThriftServer.class).in(Singleton.class);
    LifecycleModule.bindServiceRunner(binder(), ThriftServerLauncher.class);

    Registration.registerServlet(binder(), "/api", SchedulerAPIServlet.class, true);

    install(new AopModule());
  }
}
