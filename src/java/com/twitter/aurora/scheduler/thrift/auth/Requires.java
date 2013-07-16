package com.twitter.aurora.scheduler.thrift.auth;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import com.twitter.aurora.scheduler.thrift.auth.CapabilityValidator.Capability;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotation applied to a method that may allow users with non-ROOT capabilities to perform
 * an action.
 */
@Target(METHOD) @Retention(RUNTIME)
public @interface Requires {
  /**
   * The list of capabilities required to perform an action.
   */
  Capability[] whitelist() default { Capability.ROOT };
}
