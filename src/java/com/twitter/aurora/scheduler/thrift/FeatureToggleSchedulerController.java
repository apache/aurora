package com.twitter.aurora.scheduler.thrift;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.ResponseCode;
import com.twitter.aurora.gen.SessionKey;
import com.twitter.aurora.gen.StartUpdateResponse;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * A scheduler controller that allows toggling of support for specific features.
 */
class FeatureToggleSchedulerController extends ForwardingSchedulerController {

  @BindingAnnotation
  @Target({FIELD, PARAMETER, METHOD}) @Retention(RUNTIME)
  public @interface Delegate { }

  @BindingAnnotation
  @Target({FIELD, PARAMETER, METHOD}) @Retention(RUNTIME)
  public @interface EnableUpdates { }

  private final boolean enableJobUpdates;

  @Inject
  FeatureToggleSchedulerController(
      @Delegate SchedulerController delegate,
      @EnableUpdates boolean enableJobUpdates) {

    super(delegate);
    this.enableJobUpdates = enableJobUpdates;
  }

  @Override
  public StartUpdateResponse startUpdate(JobConfiguration updatedConfig, SessionKey session) {
    if (!enableJobUpdates) {
      return new StartUpdateResponse()
          .setResponseCode(ResponseCode.ERROR)
          .setMessage("Job updates are currently disabled on the scheduler.");
    }

    return super.startUpdate(updatedConfig, session);
  }
}
