package com.twitter.mesos.updater;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.gen.UpdateConfig;
import com.twitter.mesos.updater.ConfigParser.UpdateConfigException;
import com.twitter.mesos.updater.UpdateLogic.UpdateException;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static com.twitter.mesos.updater.UpdateCommand.Type.ROLLBACK_TASK;
import static com.twitter.mesos.updater.UpdateCommand.Type.UPDATE_TASK;
import static org.easymock.EasyMock.expect;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author wfarner
 */
public class UpdateLogicTest extends EasyMockTest {

  private static final Set<Integer> TASKS = ImmutableSet.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

  private UpdateConfig defaultConfig;

  private ExceptionalFunction<UpdateCommand, Integer, UpdateException> commandRunner;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws UpdateConfigException {
    commandRunner = createMock(ExceptionalFunction.class);
    defaultConfig = new UpdateConfig();
    ConfigParser.parseAndVerify(defaultConfig);
  }

  @Test
  public void testDefaultUpdate() throws Exception {
    expectCommand(UPDATE_TASK, 30, 0, 0).andReturn(0);
    expectCommand(UPDATE_TASK, 30, 0, 1).andReturn(0);
    expectCommand(UPDATE_TASK, 30, 0, 2).andReturn(0);
    expectCommand(UPDATE_TASK, 30, 0, 3).andReturn(0);
    expectCommand(UPDATE_TASK, 30, 0, 4).andReturn(0);
    expectCommand(UPDATE_TASK, 30, 0, 5).andReturn(0);
    expectCommand(UPDATE_TASK, 30, 0, 6).andReturn(0);
    expectCommand(UPDATE_TASK, 30, 0, 7).andReturn(0);
    expectCommand(UPDATE_TASK, 30, 0, 8).andReturn(0);
    expectCommand(UPDATE_TASK, 30, 0, 9).andReturn(0);

    control.replay();

    UpdateLogic updater = new UpdateLogic(TASKS, TASKS, defaultConfig, commandRunner);
    assertThat(updater.run(), is(true));
  }

  @Test
  public void testCustomUpdate() throws Exception {
    expectCommand(UPDATE_TASK, 5, 0, 0, 1).andReturn(0);
    expectCommand(UPDATE_TASK, 60, 0, 2, 3, 4).andReturn(0);
    expectCommand(UPDATE_TASK, 60, 0, 5, 6, 7).andReturn(0);
    expectCommand(UPDATE_TASK, 60, 0, 8, 9).andReturn(0);

    control.replay();

    UpdateConfig config = new UpdateConfig(defaultConfig)
        .setCanaryTaskCount(2)
        .setCanaryWatchDurationSecs(5)
        .setUpdateBatchSize(3)
        .setUpdateWatchDurationSecs(60);
    UpdateLogic updater = new UpdateLogic(TASKS, TASKS, config, commandRunner);
    assertThat(updater.run(), is(true));
  }

  @Test
  public void testCanaryFails() throws Exception {
    expectCommand(UPDATE_TASK, 30, 1, 0, 1).andReturn(2);
    expectCommand(ROLLBACK_TASK, 60, Integer.MAX_VALUE, 0).andReturn(0);
    expectCommand(ROLLBACK_TASK, 60, Integer.MAX_VALUE, 1).andReturn(0);

    control.replay();

    UpdateConfig config = new UpdateConfig(defaultConfig)
        .setCanaryTaskCount(2)
        .setToleratedCanaryFailures(1)
        .setUpdateBatchSize(1)
        .setUpdateWatchDurationSecs(60)
        .setToleratedUpdateFailures(100);
    UpdateLogic updater = new UpdateLogic(TASKS, TASKS, config, commandRunner);
    assertThat(updater.run(), is(false));
  }

  @Test
  public void testUpdateWithFailures() throws Exception {
    expectCommand(UPDATE_TASK, 30, 0, 0, 1).andReturn(0);
    expectCommand(UPDATE_TASK, 30, 4, 2, 3).andReturn(1);
    expectCommand(UPDATE_TASK, 30, 3, 4, 5).andReturn(1);
    expectCommand(UPDATE_TASK, 30, 2, 6, 7).andReturn(1);
    expectCommand(UPDATE_TASK, 30, 1, 8, 9).andReturn(1);

    control.replay();

    UpdateConfig config = new UpdateConfig(defaultConfig)
        .setCanaryTaskCount(2)
        .setUpdateBatchSize(2)
        .setToleratedUpdateFailures(4);
    UpdateLogic updater = new UpdateLogic(TASKS, TASKS, config, commandRunner);
    assertThat(updater.run(), is(true));
  }

  @Test
  public void testUpdateRollsBack() throws Exception {
    expectCommand(UPDATE_TASK, 30, 0, 0).andReturn(0);
    expectCommand(UPDATE_TASK, 30, 3, 1, 2, 3).andReturn(1);
    expectCommand(UPDATE_TASK, 30, 2, 4, 5, 6).andReturn(1);
    expectCommand(UPDATE_TASK, 30, 1, 7, 8, 9).andReturn(2);
    expectCommand(ROLLBACK_TASK, 30, Integer.MAX_VALUE, 0, 1, 2).andReturn(0);
    expectCommand(ROLLBACK_TASK, 30, Integer.MAX_VALUE, 3, 4, 5).andReturn(0);
    expectCommand(ROLLBACK_TASK, 30, Integer.MAX_VALUE, 6, 7, 8).andReturn(0);
    expectCommand(ROLLBACK_TASK, 30, Integer.MAX_VALUE, 9).andReturn(0);

    control.replay();

    UpdateConfig config = new UpdateConfig(defaultConfig)
        .setCanaryTaskCount(1)
        .setUpdateBatchSize(3)
        .setToleratedUpdateFailures(3);
    UpdateLogic updater = new UpdateLogic(TASKS, TASKS, config, commandRunner);
    assertThat(updater.run(), is(false));
  }

  private IExpectationSetters<Integer> expectCommand(UpdateCommand.Type type,
      int watchSecs, int allowedFailures, int firstRestartId, int... restartIds) throws Exception {
    Set<Integer> ids = Sets.newHashSet(firstRestartId);
    for (int id : restartIds) ids.add(id);
    return expect(commandRunner.apply(new UpdateCommand(type, ids, 30, watchSecs,
        allowedFailures)));
  }
}
