package com.twitter.aurora.scheduler.cron;

import java.util.Date;

/**
 * A utility function that predicts a cron run given a schedule.
 */
public interface CronPredictor {
  /**
   * Predicts the next date at which a cron schedule will trigger.
   *
   * @param schedule Cron schedule to predict the next time for.
   * @return A prediction for the next time a cron will run.
   */
  Date predictNextRun(String schedule);
}
