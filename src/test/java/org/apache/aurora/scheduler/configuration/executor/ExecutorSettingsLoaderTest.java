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
package org.apache.aurora.scheduler.configuration.executor;

import java.io.InputStreamReader;
import java.io.StringReader;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;

import org.apache.aurora.scheduler.configuration.executor.ExecutorSettingsLoader.ExecutorConfigException;
import org.apache.aurora.scheduler.mesos.TestExecutorSettings;
import org.apache.mesos.Protos.Volume;
import org.apache.mesos.Protos.Volume.Mode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExecutorSettingsLoaderTest {
  private static final ExecutorConfig THERMOS_CONFIG = new ExecutorConfig(
      TestExecutorSettings.THERMOS_CONFIG.getExecutor(),
      ImmutableList.of(
          Volume.newBuilder()
              .setHostPath("/path/to/observer_root")
              .setContainerPath("/path/to/observer_root")
              .setMode(Mode.RO)
              .build(),
          Volume.newBuilder()
              .setHostPath("/host")
              .setContainerPath("/container")
              .setMode(Mode.RW)
              .build()));

  private ExecutorConfig loadResource(String name) throws ExecutorConfigException {
    return ExecutorSettingsLoader.read(
        new InputStreamReader(getClass().getResourceAsStream(name), Charsets.UTF_8));
  }

  private void assertParsedResult(ExecutorConfig expected, String file)
      throws ExecutorConfigException {

    assertEquals(expected, loadResource(file));
  }

  @Test
  public void testParse() throws Exception {
    assertParsedResult(THERMOS_CONFIG, "test-thermos-executor.json");
  }

  @Test(expected = ExecutorConfigException.class)
  public void testInvalidJson() throws Exception {
    ExecutorSettingsLoader.read(new StringReader("this is not json"));
  }

  @Test(expected = ExecutorConfigException.class)
  public void testMissingField() throws Exception {
    loadResource("test-missing-field.json");
  }
}
