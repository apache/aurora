package com.twitter.mesos.scheduler.storage;

import com.twitter.mesos.gen.StorageSystemId;

/**
 * Manages scheduler storage operations providing an interface to perform atomic changes.
 *
 * @author John Sirois
 */
public interface Storage {

  /**
   * Encapsulates a storage transaction unit of work.
   *
   * @param <T> The type of result this unit of work produces.
   * @param <E> The type of exception this unit of work can throw.
   */
  interface Work<T, E extends Exception> {

    /**
     * Abstracts a unit of work that should either commit a set of changes to storage as a side
     * effect of successful completion or else commit no changes at all when an exception is thrown.
     *
     * @param schedulerStore The storage for scheduler data.
     * @param jobStore The storage for job configuration data.
     * @param taskStore The storage for task data.
     * @return the result of the successfully completed unit of work
     * @throws E if the unit of work could not be completed
     */
    T apply(SchedulerStore schedulerStore, JobStore jobStore, TaskStore taskStore) throws E;

    /**
     * A convenient typedef for Work that throws no checked exceptions - it runs quitely.
     *
     * @param <T> The type of result this unit of work produces.
     */
    interface Quiet<T> extends Work<T, RuntimeException> {
      // typedef
    }

    /**
     * Encapsulates work that returns no result.
     *
     * @param <E> The type of exception this unit of work can throw.
     */
    abstract class NoResult<E extends Exception> implements Work<Void, E> {

      @Override
      public final Void apply(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) throws E {

        execute(schedulerStore, jobStore, taskStore);
        return null;
      }

      /**
       * Similar to {@link #apply(SchedulerStore, JobStore, TaskStore)} except that no result is
       * returned.
       *
       * @param schedulerStore The storage for scheduler data.
       * @param jobStore The storage for job configuration data.
       * @param taskStore The storage for task data.
       * @throws E if the unit of work could not be completed
       */
      protected abstract void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) throws E;

      /**
       * A convenient typedef for Work with no result that throws no checked exceptions - it runs
       * quitely.
       */
      public static abstract class Quiet extends Work.NoResult<RuntimeException> {
        // typedef
      }
    }
  }

  /**
   * Returns an identifier for the storage implementation.  The version should be changed to
   * indicate that either storage semantics or format have changed in such a way that migration
   * between storage formats is required.
   *
   * @return the id number of this {@code Storage} implementation
   */
  StorageSystemId id();

  /**
   * Prepares the underlying storage for serving traffic.
   *
   * @param initilizationLogic work to perform after this storage system is ready but before
   *     allowing general use of
   *     {@link #doInTransaction(com.twitter.mesos.scheduler.storage.Storage.Work)}.
   */
  void start(Work.NoResult.Quiet initilizationLogic);

  /**
   * Executes the unit of {@code work} in a transaction such that any storage write operations
   * requested are either all committed if the unit of work does not throw or else none of the
   * requested storage operations commit when it does throw.
   *
   * @param work The unit of work to execute in a transaction.
   * @param <T> The type of result this unit of work produces.
   * @param <E> The type of exception this unit of work can throw.
   * @return the result when the unit of work completes successfully
   * @throws E bubbled transparently when the unit of work throws
   */
  <T, E extends Exception> T doInTransaction(Work<T, E> work) throws E;

  /**
   * Prepares the underlying storage system for clean shutdown.
   */
  void stop();
}
