package com.twitter.mesos.updater;

import com.google.inject.BindingAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Binding annotation for the update token.
 *
 * @author William Farner
 */
@BindingAnnotation
@Target({FIELD, PARAMETER, METHOD}) @Retention(RUNTIME)
public @interface UpdateToken {}
