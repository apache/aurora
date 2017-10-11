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

package org.apache.aurora.scheduler.config;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.name.Names;

public class CustomModule extends AbstractModule {
  static final Key<String> BINDING_KEY = Key.get(String.class, Names.named("custom_test"));

  private final Options options;

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-custom_flag")
    String customFlag = "default";
  }

  static {
    // Statically register custom options for CLI parsing.
    CommandLine.registerCustomOptions(new Options());
  }

  public CustomModule(CliOptions options) {
    // Consume parsed and populated options.
    this.options = options.getCustom(Options.class);
  }

  @Override
  protected void configure() {
    bind(BINDING_KEY).toInstance(options.customFlag);
  }
}
