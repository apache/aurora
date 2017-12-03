/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.storage;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.inject.Qualifier;

import com.google.common.base.Optional;

import org.apache.aurora.scheduler.base.Query.Builder;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

/**
 * Manages scheduler storage operations providing an interface to perform atomic changes.
 */
public interface Storage {

  /**
   * Provider for read-only stores.  Store implementations must be thread-safe, and should support
   * concurrent reads where appropriate.
   */
  interface StoreProvider {
    SchedulerStore getSchedulerStore();
    CronJobStore getCronJobStore();
    TaskStore getTaskStore();
    QuotaStore getQuotaStore();
    AttributeStore getAttributeStore();
    JobUpdateStore getJobUpdateStore();
  }

  /**
   * Provider for stores that permit mutations.  Store implementations need not support concurrent
   * writes, as a global reentrant write lock is used to serialize write operations.
   */
  interface MutableStoreProvider extends StoreProvider {
    SchedulerStore.Mutable getSchedulerStore();
    CronJobStore.Mutable getCronJobStore();

    /**
     * Gets access to the mutable task store.
     * <p>
     * This is labeled as unsafe, since it's rare that a caller should be using this.  In most
     * cases, mutations to the task store should be done through
     * {@link org.apache.aurora.scheduler.state.StateManager}.
     * <p>
     * TODO(William Farner): Come up with a way to restrict access to this interface.  As it stands,
     * it's trivial for an unsuspecting caller to modify the task store directly and subvert the
     * state machine and side effect systems.
     *
     * @return The mutable task store.
     */
    TaskStore.Mutable getUnsafeTaskStore();

    QuotaStore.Mutable getQuotaStore();
    AttributeStore.Mutable getAttributeStore();
    JobUpdateStore.Mutable getJobUpdateStore();
  }

  /**
   * A unit of work to perform against the storage system.
   *
   * @param <S> Type of stores the work needs access to.
   * @param <T> Return type of the operation.
   * @param <E> Exception type thrown by the operation.
   */
  @FunctionalInterface
  interface StorageOperation<S extends StoreProvider, T, E extends Exception> {
    /**
     * Abstracts a unit of work that has a result, but may also throw a specific exception.
     *
     * @param storeProvider A provider to give access to different available stores.
     * @return the result of the successfully completed unit of work
     * @throws E if the unit of work could not be completed
     */
    T apply(S storeProvider) throws E;
  }

  /**
   * Encapsulates a read-only storage operation.
   *
   * @param <T> The type of result this unit of work produces.
   * @param <E> The type of exception this unit of work can throw.
   */
  @FunctionalInterface
  interface Work<T, E extends Exception> extends StorageOperation<StoreProvider, T, E> {

    /**
     * A convenient typedef for Work that throws no checked exceptions - it runs quietly.
     *
     * @param <T> The type of result this unit of work produces.
     */
    @FunctionalInterface
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
  @FunctionalInterface
  interface MutateWork<T, E extends Exception>
      extends StorageOperation<MutableStoreProvider, T, E> {

    /**
     * A convenient typedef for Work that throws no checked exceptions - it runs quietly.
     *
     * @param <T> The type of result this unit of work produces.
     */
    @FunctionalInterface
    interface Quiet<T> extends MutateWork<T, RuntimeException> {
      // typedef
    }

    /**
     * Encapsulates work that returns no result.
     *
     * @param <E> The type of exception this unit of work can throw.
     */
    @FunctionalInterface
    interface NoResult<E extends Exception> extends MutateWork<Void, E> {

      @Override
      default Void apply(MutableStoreProvider storeProvider) throws E {
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
      void execute(MutableStoreProvider storeProvider) throws E;

      /**
       * A convenient typedef for Work with no result that throws no checked exceptions - it runs
       * quitely.
       */
      @FunctionalInterface
      interface Quiet extends NoResult<RuntimeException> {
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
   * Indicates that stable storage is temporarily unavailable.
   */
  class TransientStorageException extends StorageException {
    public TransientStorageException(String message, Throwable cause) {
      super(message, cause);
    }

    public TransientStorageException(String message) {
      super(message);
    }
  }

  /**
   * Executes the unit of read-only {@code work}.  The consistency model creates the possibility
   * for a reader to read uncommitted state from a concurrent writer.
   * <p>
   * TODO(wfarner): This method no longer needs to exist now that there is no global locking for
   * reads.  We could instead directly inject the individual stores where they are used, as long
   * as the stores have a layer to replicate what is currently done by
   * {@link CallOrderEnforcingStorage}.
   *
   * @param work The unit of work to execute.
   * @param <T> The type of result this unit of work produces.
   * @param <E> The type of exception this unit of work can throw.
   * @return the result when the unit of work completes successfully
   * @throws StorageException if there was a problem reading from stable storage.
   * @throws E bubbled transparently when the unit of work throws
   */
  <T, E extends Exception> T read(Work<T, E> work) throws StorageException, E;

  /**
   * Executes the unit of mutating {@code work}.
   * TODO(wfarner): Add a mechanism by which mutating work can add side-effect operations to be
   * performed after completion of the outer-most transaction.  As it stands, it's somewhat
   * futile to try to achieve this within a transaction, since the local code does not know
   * if the current transaction is nested.
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
   * Requests the underlying storage prepare its data set; ie: initialize schemas, begin syncing
   * out of date data, etc.  This method should not block.
   *
   * @throws StorageException if there was a problem preparing storage.
   */
  void prepare() throws StorageException;

  /**
   * A non-volatile storage that has additional methods to control its lifecycle.
   */
  interface NonVolatileStorage extends Storage {
    /**
     * Prepares the underlying storage for serving traffic.
     *
     * @param initializationLogic work to perform after this storage system is ready but before
     *     allowing general use of
     *     {@link #read}.
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
  @Qualifier
  @interface Volatile { }

  /**
   * Utility functions for interacting with a Storage instance.
   */
  final class Util {

    private Util() {
      // Utility class.
    }

    /**
     * Fetch tasks matching the query returned by {@code query} from {@code storage} in a
     * read operation.
     *
     * @see #fetchTasks
     * @param storage Storage instance to query from.
     * @param query Builder of the query to perform.
     * @return Tasks returned from the query.
     */
    public static Iterable<IScheduledTask> fetchTasks(Storage storage, Builder query) {
      return storage.read(storeProvider -> storeProvider.getTaskStore().fetchTasks(query));
    }

    public static Optional<IScheduledTask> fetchTask(Storage storage, String taskId) {
      return storage.read(storeProvider -> storeProvider.getTaskStore().fetchTask(taskId));
    }

    public static Iterable<IJobConfiguration> fetchCronJobs(Storage storage) {
      return storage.read(storeProvider -> storeProvider.getCronJobStore().fetchJobs());
    }
  }
}
