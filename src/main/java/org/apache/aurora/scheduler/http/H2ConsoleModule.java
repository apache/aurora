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
package org.apache.aurora.scheduler.http;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.servlet.ServletModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;

import org.h2.server.web.WebServlet;

/**
 * Binding module for the H2 management console.
 * <p>
 * See: http://www.h2database.com/html/tutorial.html#tutorial_starting_h2_console
 */
public class H2ConsoleModule extends ServletModule {
  public static final String H2_PATH = "/h2console";
  public static final String H2_PERM = "h2_management_console";

  @CmdLine(name = "enable_h2_console", help = "Enable H2 DB management console.")
  private static final Arg<Boolean> ENABLE_H2_CONSOLE = Arg.create(false);

  private final boolean enabled;

  public H2ConsoleModule() {
    this(ENABLE_H2_CONSOLE.get());
  }

  @VisibleForTesting
  public H2ConsoleModule(boolean enabled) {
    this.enabled = enabled;
  }

  @Override
  protected void configureServlets() {
    if (enabled) {
      filter(H2_PATH, H2_PATH + "/*").through(LeaderRedirectFilter.class);
      serve(H2_PATH, H2_PATH + "/*").with(new WebServlet(), ImmutableMap.of(
          "webAllowOthers", "true",
          "ifExists", "true"
      ));
    }
  }
}
