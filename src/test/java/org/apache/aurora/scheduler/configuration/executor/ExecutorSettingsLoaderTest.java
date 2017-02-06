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
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.aurora.gen.apiConstants;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettingsLoader.ExecutorConfigException;
import org.apache.aurora.scheduler.mesos.TestExecutorSettings;
import org.apache.mesos.v1.Protos.Volume;
import org.apache.mesos.v1.Protos.Volume.Mode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExecutorSettingsLoaderTest {
  private static final String EXECUTOR_NAME = apiConstants.AURORA_EXECUTOR_NAME;
  private static final Map<String, ExecutorConfig> THERMOS_CONFIG_SINGLE =
      ImmutableMap.<String, ExecutorConfig>builder().put(
          EXECUTOR_NAME,
          new ExecutorConfig(
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
                      .build()),
              TestExecutorSettings.THERMOS_TASK_PREFIX)).build();

  private static final Map<String, ExecutorConfig> THERMOS_CONFIG_MULTI =
      ImmutableMap.<String, ExecutorConfig>builder()
          .putAll(THERMOS_CONFIG_SINGLE)
          .put(EXECUTOR_NAME + "_2",
              new ExecutorConfig(
                  TestExecutorSettings.THERMOS_CONFIG
                      .getExecutor()
                      .toBuilder()
                      .setName(EXECUTOR_NAME + "_2").build(),
                  ImmutableList.of(
                      Volume.newBuilder()
                          .setHostPath("/path/to/observer_root2")
                          .setContainerPath("/path/to/observer_root2")
                          .setMode(Mode.RO)
                          .build(),
                      Volume.newBuilder()
                          .setHostPath("/host2")
                          .setContainerPath("/container2")
                          .setMode(Mode.RW)
                          .build()),
                   TestExecutorSettings.THERMOS_TASK_PREFIX + "2-")).build();

  private Map<String, ExecutorConfig> loadResource(String name) throws ExecutorConfigException {
    return ExecutorSettingsLoader.read(
        new InputStreamReader(getClass().getResourceAsStream(name), Charsets.UTF_8));
  }

  private void assertParsedResult(Map<String, ExecutorConfig> expected, String file)
      throws ExecutorConfigException {

    assertEquals(expected, loadResource(file));
  }

  @Test
  public void testParseSingle() throws Exception {
    assertParsedResult(THERMOS_CONFIG_SINGLE, "test-single-executor.json");
  }

  @Test
  public void testParseMultiple() throws Exception {
    assertParsedResult(THERMOS_CONFIG_MULTI, "test-multiple-executor.json");
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
