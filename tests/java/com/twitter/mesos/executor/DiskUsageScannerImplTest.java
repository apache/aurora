package com.twitter.mesos.executor;

import java.io.File;
import java.io.IOException;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.testing.TearDown;

import org.easymock.Capture;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.io.FileUtils;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.executor.DiskUsageScanner.DiskUsageScannerImpl;
import com.twitter.mesos.executor.Task.AuditedStatus;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TwitterTaskInfo;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class DiskUsageScannerImplTest extends EasyMockTest {

  private static final AssignedTask TASK_CONFIG = new AssignedTask()
      .setTask(new TwitterTaskInfo().setDiskMb(1));

  private TaskManager taskManager;
  private Task task;
  private File sandbox;

  private DiskUsageScannerImpl scanner;

  @Before
  public void setUp() {
    taskManager = createMock(TaskManager.class);
    task = createMock(Task.class);
    sandbox = FileUtils.createTempDir();
    addTearDown(new TearDown() {
      @Override public void tearDown() throws IOException {
        org.apache.commons.io.FileUtils.deleteDirectory(sandbox);
      }
    });

    scanner = new DiskUsageScannerImpl(taskManager, new FakeClock());
  }

  private void taskExpectations() {
    expect(taskManager.getLiveTasks()).andReturn(ImmutableList.<Task>of(task));
    expect(task.getSandboxDir()).andReturn(sandbox);
    expect(task.getAssignedTask()).andReturn(TASK_CONFIG);
  }

  @Test
  public void testEmptySandbox() {
    taskExpectations();

    control.replay();

    scanner.run();
  }

  @Test
  public void testUsageUnderRequest() throws Exception {
    writeTestData("testdata", 1020);
    taskExpectations();

    control.replay();

    scanner.run();
  }

  @Test
  public void testUsageExceedsRequest() throws Exception {
    writeTestData("testdata", 1025);
    taskExpectations();
    expect(task.getId()).andReturn("task_id").anyTimes();
    Capture<AuditedStatus> statusCapture = createCapture();
    task.terminate(capture(statusCapture));

    control.replay();

    scanner.run();

    AuditedStatus status = statusCapture.getValue();
    assertEquals(ScheduleStatus.FAILED, status.getStatus());
    String expectedMessage =
        DiskUsageScannerImpl.auditMessage(Amount.of(1L, Data.MB), Amount.of(1025L, Data.KB));
    assertEquals(Optional.of(expectedMessage), status.getMessage());
  }

  private static final String CHUNK_1KB = Strings.repeat("Thirty-two characters of text!!!", 32);

  private void writeTestData(String fileName, int kilobytes) throws IOException {
    Files.write(Strings.repeat(CHUNK_1KB, kilobytes).getBytes(), new File(sandbox, fileName));
  }
}
