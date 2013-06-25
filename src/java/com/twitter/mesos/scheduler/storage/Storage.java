package com.twitter.mesos.scheduler.storage;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.inject.BindingAnnotation;

import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.SchedulerException;

/**
 * Manages scheduler storage operations providing an interface to perform atomic changes.
 */
public interface Storage {

  interface StoreProvider {
    SchedulerStore getSchedulerStore();
    JobStore getJobStore();
    TaskStore getTaskStore();
    UpdateStore getUpdateStore();
    QuotaStore getQuotaStore();
    AttributeStore getAttributeStore();
  }

  interface MutableStoreProvider extends StoreProvider {
    SchedulerStore.Mutable getSchedulerStore();
    JobStore.Mutable getJobStore();

    /**
     * Gets access to the mutable task store.
     * <p>
     * This is labeled as unsafe, since it's rare that a caller should be using this.  In most
     * cases, mutations to the task store should be done through
     * {@link com.twitter.mesos.scheduler.StateManager}.
     * <p>
     * TODO(William Farner): Come up with a way to restrict access to this interface.  As it stands,
     * it's trivial for an unsuspecting caller to modify the task store directly and subvert the
     * state machine and side effect systems.
     *
     * @return The mutable task store.
     */
    TaskStore.Mutable getUnsafeTaskStore();

    UpdateStore.Mutable getUpdateStore();
    QuotaStore.Mutable getQuotaStore();
    AttributeStore.Mutable getAttributeStore();
  }

  /**
   * Encapsulates a read-only storage operation.
   *
   * @param <T> The type of result this unit of work produces.
   * @param <E> The type of exception this unit of work can throw.
   */
  interface Work<T, E extends Exception> {

    /**
     * Abstracts a unit of work that has a result, but may also throw a specific exception.
     *
     * @param storeProvider A provider to give access to different available stores.
     * @return the result of the successfully completed unit of work
     * @throws E if the unit of work could not be completed
     */
    T apply(StoreProvider storeProvider) throws E;

    /**
     * A convenient typedef for Work that throws no checked exceptions - it runs quietly.
     *
     * @param <T> The type of result this unit of work produces.
     */
    interface Quiet<T> extends Work<T, RuntimeException> {
      // typedef
    }
  }

  /**
   * Encapsulates a storage operation, which has mutable storage access.
   *
   * @param <T> The type of result this unit of work produces.
   * @param <E> The type of exception this unit of work can throw.
   */
  interface MutateWork<T, E extends Exception> {

    NoResult.Quiet NOOP = new NoResult.Quiet() {
      @Override protected void execute(Storage.MutableStoreProvider storeProvider) {
        // No-op.
      }
    };

    /**
     * Abstracts a unit of work that should either commit a set of changes to storage as a side
     * effect of successful completion or else commit no changes at all when an exception is thrown.
     *
     * @param storeProvider A provider to give access to different available stores.
     * @return the result of the successfully completed unit of work
     * @throws E if the unit of work could not be completed
     */
    T apply(MutableStoreProvider storeProvider) throws E;

    /**
     * A convenient typedef for Work that throws no checked exceptions - it runs quietly.
     *
     * @param <T> The type of result this unit of work produces.
     */
    interface Quiet<T> extends MutateWork<T, RuntimeException> {
      // typedef
    }

    /**
     * Encapsulates work that returns no result.
     *
     * @param <E> The type of exception this unit of work can throw.
     */
    abstract class NoResult<E extends Exception> implements MutateWork<Void, E> {

      @Override public final Void apply(MutableStoreProvider storeProvider) throws E {
        execute(storeProvider);
        return null;
      }

      /**
       * Similar to {@link #apply(MutableStoreProvider)} except that no result is
       * returned.
       *
       * @param storeProvider A provider to give access to different available stores.
       * @throws E if the unit of work could not be completed
       */
      protected abstract void execute(MutableStoreProvider storeProvider) throws E;

      /**
       * A convenient typedef for Work with no result that throws no checked exceptions - it runs
       * quitely.
       */
      public abstract static class Quiet extends NoResult<RuntimeException> {
        // typedef
      }
    }
  }

  /**
   * Indicates a problem reading from or writing to stable storage.
   */
  class StorageException extends SchedulerException {
    public StorageException(String message, Throwable cause) {
      super(message, cause);
    }

    public StorageException(String message) {
      super(message);
    }
  }

  /**
   * Executes the unit of read-only {@code work}.  All data in the stores may be expected to be
   * consistent, as the invocation is mutually exclusive of any writes.
   *
   * <p>TODO(John Sirois): Audit usages and handle StorageException appropriately.
   *
   * @param work The unit of work to execute.
   * @param <T> The type of result this unit of work produces.
   * @param <E> The type of exception this unit of work can throw.
   * @return the result when the unit of work completes successfully
   * @throws StorageException if there was a problem reading from stable storage.
   * @throws E bubbled transparently when the unit of work throws
   */
  <T, E extends Exception> T consistentRead(Work<T, E> work) throws StorageException, E;

  /**
   * Executes a unit of read-only {@code work}.  This is functionally identical to
   * {@link #consistentRead(Work)} with the exception that data in the stores may not be fully
   * consistent.
   *
   * @param work The unit of work to execute.
   * @param <T> The type of result this unit of work produces.
   * @param <E> The type of exception this unit of work can throw.
   * @return the result when the unit of work completes successfully
   * @throws StorageException if there was a problem reading from stable storage.
   * @throws E bubbled transparently when the unit of work throws
   */
  <T, E extends Exception> T weaklyConsistentRead(Work<T, E> work) throws StorageException, E;

  /**
   * Executes the unit of mutating {@code work}.
   *
   * @param work The unit of work to execute.
   * @param <T> The type of result this unit of work produces.
   * @param <E> The type of exception this unit of work can throw.
   * @return the result when the unit of work completes successfully
   * @throws StorageException if there was a problem reading from or writing to stable storage.
   * @throws E bubbled transparently when the unit of work throws
   */
  <T, E extends Exception> T write(MutateWork<T, E> work) throws StorageException, E;

  /**
   * Clean up the underlying storage by optimizing internal data structures. Does not change
   * externally-visible state but might not run concurrently with write operations.
   */
  void snapshot() throws StorageException;

  /**
   * A non-volatile storage that has additional methods to control its lifecycle.
   */
  interface NonVolatileStorage extends Storage {
    /**
     * Requests the underlying storage prepare its data set; ie: initialize schemas, begin syncing
     * out of date data, etc.  This method should not block.
     *
     * @throws StorageException if there was a problem preparing storage.
     */
    void prepare() throws StorageException;

    /**
     * Prepares the underlying storage for serving traffic.
     *
     * @param initializationLogic work to perform after this storage system is ready but before
     *     allowing general use of
     *     {@link #consistentRead}.
     * @throws StorageException if there was a starting storage.
     */
    void start(MutateWork.NoResult.Quiet initializationLogic) throws StorageException;

    /**
     * Prepares the underlying storage system for clean shutdown.
     */
    void stop();
  }

  /**
   * Identifies a storage layer that is in-memory only.
   * This generally should only be used when the storage is first starting up, to perform queries
   * related to initially load the storage.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ ElementType.PARAMETER, ElementType.METHOD })
  @BindingAnnotation
  public @interface Volatile { }

  /**
   * Utility functions for interacting with a Storage instance.
   */
  public final class Util {

    private Util() {
      // Utility class.
    }

    /**
     * Fetches tasks from the {@link TaskStore} for {@code storage} in a read operation.
     * This is intended for convenience, and should only be used in instances where no write work
     * is performed with the result.
     *
     * @param storage Storage instance to query from.
     * @param query Query to perform.
     * @return Tasks returned from the query.
     * @deprecated Use {@link #consistentFetchTasks(Storage, Supplier)} instead.
     */
    @Deprecated
    public static ImmutableSet<ScheduledTask> consistentFetchTasks(
        Storage storage,
        final TaskQuery query) {

      return storage.consistentRead(new Work.Quiet<ImmutableSet<ScheduledTask>>() {
        @Override public ImmutableSet<ScheduledTask> apply(StoreProvider storeProvider) {
          return storeProvider.getTaskStore().fetchTasks(query);
        }
      });
    }

    /**
     * Fetch tasks matching the query returned by {@code querySupplier} from {@code storage} in a
     * read operation. Intended for use with {@link com.twitter.mesos.scheduler.base.Query.Builder}
     * instances.
     *
     * @see #consistentFetchTasks
     * @param storage Storage instance to query from.
     * @param querySupplier Supplier of the query to perform.
     * @return Tasks returned from the query.
     */
    public static ImmutableSet<ScheduledTask> consistentFetchTasks(
        Storage storage,
        final Supplier<TaskQuery> querySupplier) {

      return storage.consistentRead(new Work.Quiet<ImmutableSet<ScheduledTask>>() {
        @Override public ImmutableSet<ScheduledTask> apply(StoreProvider storeProvider) {
          return storeProvider.getTaskStore().fetchTasks(querySupplier);
        }
      });
    }

    /**
     * Identical to {@link #weaklyConsistentFetchTasks(Storage, Supplier)}, but using the deprecated
     * fask fetch API.
     *
     * @param storage Storage instance to query from.
     * @param query Query to perform.
     * @return Tasks returned from the query.
     * @deprecated Use {@link #weaklyConsistentFetchTasks(Storage, Supplier)} instead.
     */
    @Deprecated
    public static ImmutableSet<ScheduledTask> weaklyConsistentFetchTasks(
        Storage storage,
        final TaskQuery query) {

      return storage.weaklyConsistentRead(new Work.Quiet<ImmutableSet<ScheduledTask>>() {
        @Override public ImmutableSet<ScheduledTask> apply(StoreProvider storeProvider) {
          return storeProvider.getTaskStore().fetchTasks(query);
        }
      });
    }

    /**
     * Identical to {@link #consistentFetchTasks(Storage, Supplier)}, but fetches tasks using a
     * weakly-consistent read operation.
     *
     * @see #consistentFetchTasks
     * @param storage Storage instance to query from.
     * @param querySupplier Supplier of the query to perform.
     * @return Tasks returned from the query.
     */
    public static ImmutableSet<ScheduledTask> weaklyConsistentFetchTasks(
        Storage storage,
        final Supplier<TaskQuery> querySupplier) {

      return storage.weaklyConsistentRead(new Work.Quiet<ImmutableSet<ScheduledTask>>() {
        @Override public ImmutableSet<ScheduledTask> apply(StoreProvider storeProvider) {
          return storeProvider.getTaskStore().fetchTasks(querySupplier);
        }
      });
    }
  }
}
