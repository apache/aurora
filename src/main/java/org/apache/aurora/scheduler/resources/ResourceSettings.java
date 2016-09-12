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
package org.apache.aurora.scheduler.resources;

import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;

/**
 * Control knobs for how Aurora treats different resource types.
 *
 * The command line handling seen here is non-standard. Normally we declare them in modules
 * and then inject them via 'settings' classes. Unfortunately, this does not work here as we
 * would need to perform the injection into the ResourceType enum. Enums are picky in that regard.
 */
final class ResourceSettings {

  @CmdLine(name = "enable_revocable_cpus", help = "Treat CPUs as a revocable resource.")
  static final Arg<Boolean> ENABLE_REVOCABLE_CPUS = Arg.create(true);

  @CmdLine(name = "enable_revocable_ram", help = "Treat RAM as a revocable resource.")
  static final Arg<Boolean> ENABLE_REVOCABLE_RAM = Arg.create(false);

  private ResourceSettings() {

  }
}
