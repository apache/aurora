package com.twitter.nexus;

import nexus.Executor;
import nexus.ExecutorDriver;
import nexus.NexusExecutorDriver;
import nexus.TaskDescription;
import nexus.TaskState;
import nexus.TaskStatus;

public class TwitterExecutor extends Executor {
  static {
    System.loadLibrary("nexus");
  }

  @Override
  public void launchTask(ExecutorDriver driver, TaskDescription task) {
    System.out.println("Running task " + task.getName());
    try {
      Thread.sleep(100000);
    } catch (InterruptedException ie) { ie.printStackTrace(); }
    TaskStatus status = new TaskStatus(task.getTaskId(),
				       TaskState.TASK_FAILED,
				       new byte[0]);
    driver.sendStatusUpdate(status);
  }

  public static void main(String[] args) throws Exception {
    Executor exec = new TwitterExecutor();
    new NexusExecutorDriver(exec).run();
  }
}
