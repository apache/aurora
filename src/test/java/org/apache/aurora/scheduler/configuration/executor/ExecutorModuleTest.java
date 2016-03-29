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

import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.aurora.GuavaUtils;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExecutorModuleTest {

  @Test
  public void testMakeExecutorCommand() {
    testSingleCommand(
        "/path/executor.pex",
        ImmutableList.of(),
        false,
        null,
        "${MESOS_SANDBOX=.}/executor.pex",
        ImmutableList.of("/path/executor.pex"));

    testSingleCommand(
        "/path/executor.pex",
        ImmutableList.of(),
        true,
        null,
        "HOME=${MESOS_SANDBOX=.} ${MESOS_SANDBOX=.}/executor.pex",
        ImmutableList.of("/path/executor.pex"));

    testSingleCommand(
        "/path/executor.pex",
        ImmutableList.of("/other/thing.pex"),
        false,
        null,
        "${MESOS_SANDBOX=.}/executor.pex",
        ImmutableList.of("/path/executor.pex", "/other/thing.pex"));

    testSingleCommand(
        "/path/executor.pex",
        ImmutableList.of(),
        false,
        "--extra=args",
        "${MESOS_SANDBOX=.}/executor.pex --extra=args",
        ImmutableList.of("/path/executor.pex"));
  }

  private void testSingleCommand(
      String path,
      List<String> resources,
      boolean homeInSandbox,
      String flags,
      String expectedCommand,
      List<String> expectedUris) {

    CommandInfo info = ExecutorModule.makeExecutorCommand(path, resources, homeInSandbox, flags);
    assertEquals(expectedCommand, info.getValue());
    assertEquals(expectedUris, extractUris(info.getUrisList()));
  }

  private List<String> extractUris(List<URI> uris) {
    return uris.stream().map(URI::getValue).collect(GuavaUtils.toImmutableList());
  }
}
