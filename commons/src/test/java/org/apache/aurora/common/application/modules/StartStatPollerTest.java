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
package org.apache.aurora.common.application.modules;

import java.util.Properties;

import org.apache.aurora.common.application.ShutdownRegistry;
import org.junit.Test;

import org.apache.aurora.common.stats.Stat;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.common.stats.TimeSeriesRepository;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.BuildInfo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StartStatPollerTest extends EasyMockTest {
  @Test
  public void testStartStatPollerExecute() {
    ShutdownRegistry shutdownRegistry = createMock(ShutdownRegistry.class);
    TimeSeriesRepository repository = createMock(TimeSeriesRepository.class);

    Properties properties = new Properties();
    final Long gitRevisionNumber = 1404461016779713L;
    properties.setProperty(BuildInfo.Key.GIT_REVISION_NUMBER.value, gitRevisionNumber.toString());
    String gitRevision = "foo_branch";
    properties.setProperty(BuildInfo.Key.GIT_REVISION.value, gitRevision);
    BuildInfo buildInfo = new BuildInfo(properties);

    StatsModule.StartStatPoller poller =
        new StatsModule.StartStatPoller(shutdownRegistry, buildInfo, repository);

    repository.start(shutdownRegistry);
    control.replay();

    poller.execute();

    Stat<Long> gitRevisionNumberStat =
        Stats.getVariable(Stats.normalizeName(BuildInfo.Key.GIT_REVISION_NUMBER.value));
    assertEquals(gitRevisionNumber, gitRevisionNumberStat.read());

    Stat<String> gitRevisionStat =
        Stats.getVariable(Stats.normalizeName(BuildInfo.Key.GIT_REVISION.value));
    assertEquals(gitRevision, gitRevisionStat.read());

    Stat<String> gitBranchNameStat =
        Stats.getVariable(Stats.normalizeName(BuildInfo.Key.GIT_BRANCHNAME.value));
    assertNull(gitBranchNameStat);
  }
}
