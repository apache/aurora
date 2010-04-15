package com.twitter.nexus;

import com.twitter.common.process.Process;

/**
 * Utility to send requests to the Twitter nexus scheduler.
 *
 * @author wfarner
 */
public class TwitterSchedulerUtil extends Process {
  @Override
  protected boolean checkOptions() {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  protected Options getOptions() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  protected void runProcess() {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  public static void main(String[] args) {
    new TwitterSchedulerUtil().run(args);
  }
}
