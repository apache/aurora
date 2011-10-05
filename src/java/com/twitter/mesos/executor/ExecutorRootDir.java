package com.twitter.mesos.executor;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import com.google.inject.BindingAnnotation;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Binding annotation for the executor root directory.
 *
 * @author William Farner
 */
@BindingAnnotation @Target({FIELD, PARAMETER, METHOD}) @Retention(RUNTIME)
public @interface ExecutorRootDir {}
