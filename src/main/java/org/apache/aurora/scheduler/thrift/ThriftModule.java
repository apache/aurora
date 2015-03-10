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
package org.apache.aurora.scheduler.thrift;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.AbstractModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;

import org.apache.aurora.gen.AuroraAdmin;
import org.apache.aurora.gen.ReadOnlyScheduler;
import org.apache.aurora.scheduler.thrift.SchedulerThriftInterface.EnableUpdater;
import org.apache.aurora.scheduler.thrift.aop.AopModule;

/**
 * Binding module to configure a thrift server.
 */
public class ThriftModule extends AbstractModule {

  @VisibleForTesting
  @CmdLine(name = "enable_beta_updater",
      help = "Enable the async updater. Use at your own risk, this feature is not yet complete.")
  static final Arg<Boolean> ENABLE_BETA_UPDATER = Arg.create(false);

  @Override
  protected void configure() {
    bind(ReadOnlyScheduler.Iface.class).to(ReadOnlySchedulerImpl.class);
    bind(AuroraAdmin.Iface.class).to(SchedulerThriftInterface.class);

    // Promote to an explicit binding so it's created in the servlet container child injector.
    // See https://code.google.com/p/google-guice/issues/detail?id=461
    bind(SchedulerThriftInterface.class);

    bind(Boolean.class).annotatedWith(EnableUpdater.class).toInstance(ENABLE_BETA_UPDATER.get());

    install(new AopModule());
  }
}
