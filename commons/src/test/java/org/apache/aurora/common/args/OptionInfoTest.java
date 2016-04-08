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
package org.apache.aurora.common.args;

import java.io.File;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static junit.framework.Assert.assertEquals;

public class OptionInfoTest {
  private static class App {
    @CmdLine(name = "files", help = "help.", argFile = true)
    private final Arg<List<File>> files = Arg.<List<File>>create(ImmutableList.<File>of());

    @CmdLine(name = "flag", help = "help.")
    private final Arg<Boolean> flag = Arg.create();
  }

  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder();
  private App app;

  @Before
  public void setUp() throws Exception {
    app = new App();
  }

  @Test
  public void testArgumentFilesRegularFormat() throws Exception {
    new ArgScanner().parse(Args.from(ArgFilters.selectClass(App.class), app),
        ImmutableList.of("-files=1.txt,2.txt"));
    assertEquals(
        ImmutableList.of(new File("1.txt"), new File("2.txt")),
        app.files.get());
  }

  @Test
  public void testArgumentFlagCreateFromField() throws Exception {
    OptionInfo optionInfo = OptionInfo.createFromField(App.class.getDeclaredField("flag"), app);
    assertEquals("flag", optionInfo.getName());
    assertEquals("help.", optionInfo.getHelp());
    assertEquals("no_flag", optionInfo.getNegatedName());
  }
}
