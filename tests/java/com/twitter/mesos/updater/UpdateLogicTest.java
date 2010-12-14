package com.twitter.mesos.updater;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.gen.UpdateConfig;
import com.twitter.mesos.updater.ConfigParser.UpdateConfigException;
import com.twitter.mesos.updater.UpdateLogic.UpdateCommand;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static com.twitter.mesos.updater.UpdateLogic.UpdateCommand.Type.ROLLBACK_TASK;
import static com.twitter.mesos.updater.UpdateLogic.UpdateCommand.Type.UPDATE_TASK;
import static org.easymock.EasyMock.expect;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author wfarner
 */
public class UpdateLogicTest extends EasyMockTest {

  private static final Set<Integer> TASKS = ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

  private static final UpdateConfig DEFAULT_UPDATE_CONFIG = new UpdateConfig();

  private Function<UpdateCommand, Map<Integer, Boolean>> commandRunner;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    commandRunner = createMock(Function.class);
  }

  @Test
  public void testDefaultUpdate() throws Exception {
    expectCommand(UPDATE_TASK, 30, 0).andReturn(ImmutableMap.of(0, true));
    expectCommand(UPDATE_TASK, 30, 1).andReturn(ImmutableMap.of(1, true));
    expectCommand(UPDATE_TASK, 30, 2).andReturn(ImmutableMap.of(2, true));
    expectCommand(UPDATE_TASK, 30, 3).andReturn(ImmutableMap.of(3, true));
    expectCommand(UPDATE_TASK, 30, 4).andReturn(ImmutableMap.of(4, true));
    expectCommand(UPDATE_TASK, 30, 5).andReturn(ImmutableMap.of(5, true));
    expectCommand(UPDATE_TASK, 30, 6).andReturn(ImmutableMap.of(6, true));
    expectCommand(UPDATE_TASK, 30, 7).andReturn(ImmutableMap.of(7, true));
    expectCommand(UPDATE_TASK, 30, 8).andReturn(ImmutableMap.of(8, true));
    expectCommand(UPDATE_TASK, 30, 9).andReturn(ImmutableMap.of(9, true));

    control.replay();

    UpdateLogic updater = new UpdateLogic(TASKS, DEFAULT_UPDATE_CONFIG, commandRunner);
    assertThat(updater.run(), is(true));
  }

  @Test
  public void testCustomUpdate() throws Exception {
    expectCommand(UPDATE_TASK, 5, 0, 1).andReturn(ImmutableMap.of(0, true, 1, true));
    expectCommand(UPDATE_TASK, 60, 2, 3, 4).andReturn(ImmutableMap.of(2, true, 3, true, 4, true));
    expectCommand(UPDATE_TASK, 60, 5, 6, 7).andReturn(ImmutableMap.of(5, true, 6, true, 7, true));
    expectCommand(UPDATE_TASK, 60, 8, 9).andReturn(ImmutableMap.of(8, true, 9, true));

    control.replay();

    UpdateLogic updater = new UpdateLogic(TASKS,
        new UpdateConfig().setConfig(ImmutableMap.of(
            "canary_size", "2",
            "canary_watch_secs", "5",
            "update_batch_size", "3",
            "update_watch_secs", "60"
        )),
        commandRunner);
    assertThat(updater.run(), is(true));
  }

  @Test
  public void testCanaryFails() throws Exception {
    expectCommand(UPDATE_TASK, 30, 0, 1).andReturn(ImmutableMap.of(0, false, 1, false));
    expectCommand(ROLLBACK_TASK, 60, 0).andReturn(ImmutableMap.of(0, true));
    expectCommand(ROLLBACK_TASK, 60, 1).andReturn(ImmutableMap.of(1, true));

    control.replay();

    UpdateLogic updater = new UpdateLogic(TASKS,
        new UpdateConfig().setConfig(ImmutableMap.of(
            "canary_size", "2",
            "tolerated_canary_failures", "1",
            "update_batch_size", "1",
            "update_watch_secs", "60",
            "tolerated_total_failures", "100"  // Ensures canary still breaks the update.
        )),
        commandRunner);
    assertThat(updater.run(), is(false));
  }

  @Test
  public void testUpdateWithFailures() throws Exception {
    expectCommand(UPDATE_TASK, 30, 0, 1).andReturn(ImmutableMap.of(0, true, 1, true));
    expectCommand(UPDATE_TASK, 30, 2, 3).andReturn(ImmutableMap.of(2, false, 3, true));
    expectCommand(UPDATE_TASK, 30, 4, 5).andReturn(ImmutableMap.of(4, false, 5, true));
    expectCommand(UPDATE_TASK, 30, 6, 7).andReturn(ImmutableMap.of(6, false, 7, true));
    expectCommand(UPDATE_TASK, 30, 8, 9).andReturn(ImmutableMap.of(8, false, 9, true));

    control.replay();

    UpdateLogic updater = new UpdateLogic(TASKS,
        new UpdateConfig().setConfig(ImmutableMap.of(
            "canary_size", "2",
            "update_batch_size", "2",
            "tolerated_total_failures", "4"
        )),
        commandRunner);
    assertThat(updater.run(), is(true));
  }

  @Test
  public void testUpdateRollsBack() throws Exception {
    expectCommand(UPDATE_TASK, 30, 0).andReturn(ImmutableMap.of(0, true));
    expectCommand(UPDATE_TASK, 30, 1, 2, 3).andReturn(ImmutableMap.of(1, false, 2, true, 3, true));
    expectCommand(UPDATE_TASK, 30, 4, 5, 6).andReturn(ImmutableMap.of(4, false, 5, true, 6, true));
    expectCommand(UPDATE_TASK, 30, 7, 8, 9).andReturn(ImmutableMap.of(7, false, 8, false, 9, true));
    expectCommand(ROLLBACK_TASK, 30, 0, 1, 2).andReturn(ImmutableMap.of(0, true, 1, true, 2, true));
    expectCommand(ROLLBACK_TASK, 30, 3, 4, 5).andReturn(ImmutableMap.of(3, true, 4, true, 5, true));
    expectCommand(ROLLBACK_TASK, 30, 6, 7, 8).andReturn(ImmutableMap.of(6, true, 7, true, 8, true));
    expectCommand(ROLLBACK_TASK, 30, 9).andReturn(ImmutableMap.of(9, true));

    control.replay();

    UpdateLogic updater = new UpdateLogic(TASKS,
        new UpdateConfig().setConfig(ImmutableMap.of(
            "canary_size", "1",
            "update_batch_size", "3",
            "tolerated_total_failures", "3"
        )),
        commandRunner);
    assertThat(updater.run(), is(false));
  }

  @Test
  public void testBadConfig() throws Exception {
    control.replay();

    expectBadConfig(ImmutableMap.of("canary_size", "asdf"));
    expectBadConfig(ImmutableMap.of("canary_size", "-1"));
    expectBadConfig(ImmutableMap.of("canary_size", "0"));
    expectBadConfig(ImmutableMap.of("tolerated_canary_failures", "-1"));
    expectBadConfig(ImmutableMap.of("canary_watch_secs", "0"));
    expectBadConfig(ImmutableMap.of("update_batch_size", "0"));
    expectBadConfig(ImmutableMap.of("tolerated_total_failures", "-1"));
    expectBadConfig(ImmutableMap.of("update_watch_secs", "0"));
  }

  private void expectBadConfig(Map<String, String> config) {
    try {
      new UpdateLogic(TASKS, new UpdateConfig().setConfig(config), commandRunner);
      fail("Bad config allowed.");
    } catch (UpdateConfigException e) {
      // Expected.
    }
  }

  private IExpectationSetters<Map<Integer, Boolean>> expectCommand(UpdateCommand.Type type,
      int watchSecs, int... restartIds) {
    Set<Integer> ids = Sets.newHashSet();
    for (int id : restartIds) ids.add(id);
    return expect(commandRunner.apply(new UpdateCommand(type, ids, watchSecs)));
  }
}
