package com.twitter.mesos.scheduler.storage;

import com.google.inject.BindingAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * A {@literal @BindingAnnotation} for the scheduler {@link Storage} service.
 *
 * @author John Sirois
 */
@BindingAnnotation
@Target({FIELD, PARAMETER, METHOD})
@Retention(RUNTIME)
public @interface StorageRole {

  /**
   * Used to indicate the role of the bound {@link Storage} service in the scheduler runtime.
   */
  enum Role {
    /**
     * Indicates the bound storage is the one used for primary storage of scheduler data.
     */
    Primary,

    /**
     * Indicates the bound storage is a legacy storage system that should be upgraded from.
     */
    Legacy
  }

  /**
   * Indicates the storage role of the bound {@link Storage} service.
   *
   * @return The storage role.
   */
  Role value();
}
