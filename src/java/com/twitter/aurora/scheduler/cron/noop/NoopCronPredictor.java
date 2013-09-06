package com.twitter.aurora.scheduler.cron.noop;

import java.util.Date;

import com.twitter.aurora.scheduler.cron.CronPredictor;

/**
 * A cron predictor that always suggests that the next run is Unix epoch time.
 *
 * This class exists as a short term hack to get around a license compatibility issue - Real
 * Implementation (TM) coming soon.
 */
class NoopCronPredictor implements CronPredictor {
  @Override
  public Date predictNextRun(String schedule) {
    return new Date(0);
  }
}
