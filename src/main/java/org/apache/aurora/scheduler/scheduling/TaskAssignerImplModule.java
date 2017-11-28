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
package org.apache.aurora.scheduler.scheduling;

import java.util.List;
import javax.inject.Singleton;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Module;

import org.apache.aurora.scheduler.app.MoreModules;
import org.apache.aurora.scheduler.config.CliOptions;

/**
 * The default TaskAssigner implementation that allows the injection of custom offer
 * selecting modules via the '-offer_selector_modules' flag.
 */
public class TaskAssignerImplModule extends AbstractModule {

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-offer_selector_modules",
        description = "Guice module for customizing the TaskAssignerImpl's OfferSelector.")
    @SuppressWarnings("rawtypes")
    public List<Class> offerSelectorModules =
        ImmutableList.of(FirstFitOfferSelectorModule.class);
  }

  private final CliOptions cliOptions;

  public TaskAssignerImplModule(CliOptions cliOptions) {
    this.cliOptions = cliOptions;
  }

  @Override
  protected void configure() {
    Options options = cliOptions.taskAssigner;
    for (Module module : MoreModules.instantiateAll(options.offerSelectorModules, cliOptions)) {
      install(module);
    }

    bind(TaskAssigner.class).to(TaskAssignerImpl.class);
    bind(TaskAssignerImpl.class).in(Singleton.class);
  }
}
