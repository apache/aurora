package com.twitter.aurora.scheduler.thrift.auth;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Type annotation to apply to a thrift interface implementation that should be decorated with
 * additional functionality.
 */
@Target({PARAMETER, TYPE}) @Retention(RUNTIME)
public @interface DecoratedThrift {
}
